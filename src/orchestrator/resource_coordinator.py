# src/orchestrator/resource_coordinator.py
"""
The Resource Coordinator is the stateful, intelligent, and distributed
decision-making component for the Orchestrator. It uses a state management
strategy pattern to support both in-memory (single-process) and Redis (EKS)
deployment models.

NOTE: This component requires the DatabaseInterface to have a method:
- get_datasource_with_schedule(datasource_id: str) -> Optional[DataSource]
  This method should return a SQLAlchemy ORM object for the DataSource,
  eagerly loading its related Calendar and CalendarRules.
"""

import redis
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone, time, timedelta
from typing import Dict, Any, Optional, List, NamedTuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.db.database_interface import DatabaseInterface
from core.db_models.job_schema import Job
from core.errors import ConfigurationError

# Define Redis keys in a central place
FAIR_SHARING_KEY = "rc:fair_sharing"
DATASOURCE_CONN_KEY = "rc:datasource_connections"

class ResourceDecision(NamedTuple):
    is_approved: bool
    reason: str
    snooze_duration_sec: int = 0

# =================================================================
# State Management Strategy (Abstract Base Class)
# =================================================================

class AbstractStateManager(ABC):
    """Abstract base class for state management strategies."""
    @abstractmethod
    def get_worker_counts_for_jobs(self, job_ids: List[str]) -> Dict[str, int]: pass
    
    @abstractmethod
    def reserve_datasource_connection(self, datasource_id: str, limit: int) -> bool: pass

    @abstractmethod
    def release_datasource_connection(self, datasource_id: str): pass

    @abstractmethod
    def confirm_task_assignment(self, job_id: int): pass
    
    @abstractmethod
    def release_job_worker_slot(self, job_id: int): pass

    @abstractmethod
    def check_system_resource_limits(self, memory_limit: int, cpu_limit: int) -> ResourceDecision: pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]: pass

# =================================================================
# In-Memory State Manager (for Single-Process Mode)
# =================================================================

class InMemoryStateManager(AbstractStateManager):
    """Manages state using a thread-safe in-memory dictionary for single-process mode."""
    def __init__(self):
        self._lock = threading.Lock()
        self._fair_sharing_tracker = defaultdict(int)
        self._datasource_connection_tracker = defaultdict(int)
        # In a real implementation, a background thread would update these values using a library like psutil
        self._current_memory_usage_mb = 512 # Placeholder for real-time monitoring
        self._current_cpu_percent = 25     # Placeholder

    def check_system_resource_limits(self, memory_limit: int, cpu_limit: int) -> ResourceDecision:
        with self._lock:
            if self._current_cpu_percent >= cpu_limit:
                reason = f"System CPU usage ({self._current_cpu_percent}%) exceeds limit ({cpu_limit}%)."
                return ResourceDecision(False, reason, snooze_duration_sec=60)
            if self._current_memory_usage_mb >= memory_limit:
                reason = f"System memory usage ({self._current_memory_usage_mb}MB) exceeds limit ({memory_limit}MB)."
                return ResourceDecision(False, reason, snooze_duration_sec=60)
            return ResourceDecision(True, "OK")

    def get_worker_counts_for_jobs(self, job_ids: List[str]) -> Dict[str, int]:
        with self._lock:
            return {jid: self._fair_sharing_tracker.get(jid, 0) for jid in job_ids}

    def reserve_datasource_connection(self, datasource_id: str, limit: int) -> bool:
        with self._lock:
            if self._datasource_connection_tracker[datasource_id] < limit:
                self._datasource_connection_tracker[datasource_id] += 1
                return True
            return False

    def release_datasource_connection(self, datasource_id: str):
        with self._lock:
            if datasource_id in self._datasource_connection_tracker and self._datasource_connection_tracker[datasource_id] > 0:
                self._datasource_connection_tracker[datasource_id] -= 1
                if self._datasource_connection_tracker[datasource_id] == 0:
                    del self._datasource_connection_tracker[datasource_id]

    def confirm_task_assignment(self, job_id: int):
        with self._lock:
            self._fair_sharing_tracker[str(job_id)] += 1

    def release_job_worker_slot(self, job_id: int):
        with self._lock:
            job_id_str = str(job_id)
            if job_id_str in self._fair_sharing_tracker and self._fair_sharing_tracker[job_id_str] > 0:
                self._fair_sharing_tracker[job_id_str] -= 1
                if self._fair_sharing_tracker[job_id_str] == 0:
                    del self._fair_sharing_tracker[job_id_str]
    
    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "fair_sharing": dict(self._fair_sharing_tracker),
                "datasource_connections": dict(self._datasource_connection_tracker)
            }

# =================================================================
# Redis State Manager (for EKS Mode)
# =================================================================

class RedisStateManager(AbstractStateManager):
    """Manages state using Redis for distributed (EKS) mode."""
    def __init__(self, redis_client: redis.Redis, logger: SystemLogger):
        self.redis = redis_client
        self.logger = logger
        self._reserve_script = self.redis.script_load("""
            local count = tonumber(redis.call('HGET', KEYS[1], ARGV[1])) or 0
            if count < tonumber(ARGV[2]) then
                return redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
            else
                return -1
            end
        """)
        self._release_script = self.redis.script_load("""
            local count = tonumber(redis.call('HGET', KEYS[1], ARGV[1])) or 0
            if count and count > 0 then
                return redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
            else
                return 0
            end
        """)

    def check_system_resource_limits(self, memory_limit: int, cpu_limit: int) -> ResourceDecision:
        # Host-level resources are managed by Kubernetes pods, not the RC in EKS mode.
        return ResourceDecision(True, "OK")

    def get_worker_counts_for_jobs(self, job_ids: List[str]) -> Dict[str, int]:
        if not job_ids: return {}
        try:
            counts = self.redis.hmget(FAIR_SHARING_KEY, job_ids)
            return {jid: int(count or 0) for jid, count in zip(job_ids, counts)}
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in get_worker_counts_for_jobs.", error=str(e))
            return {} # Return empty dict to prevent crash, system will degrade gracefully

    def reserve_datasource_connection(self, datasource_id: str, limit: int) -> bool:
        try:
            result = self.redis.evalsha(self._reserve_script, 1, DATASOURCE_CONN_KEY, datasource_id, limit)
            return result != -1
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in reserve_datasource_connection.", error=str(e))
            return False

    def release_datasource_connection(self, datasource_id: str):
        try:
            self.redis.evalsha(self._release_script, 1, DATASOURCE_CONN_KEY, datasource_id)
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in release_datasource_connection.", error=str(e))

    def confirm_task_assignment(self, job_id: int):
        try:
            self.redis.hincrby(FAIR_SHARING_KEY, str(job_id), 1)
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in confirm_task_assignment.", error=str(e))

    def release_job_worker_slot(self, job_id: int):
        try:
            self.redis.hincrby(FAIR_SHARING_KEY, str(job_id), -1)
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in release_job_worker_slot.", error=str(e))

    def get_metrics(self) -> Dict[str, Any]:
        try:
            # HGETALL can be slow on large hashes; for production, consider HSCAN.
            return {
                "fair_sharing": self.redis.hgetall(FAIR_SHARING_KEY),
                "datasource_connections": self.redis.hgetall(DATASOURCE_CONN_KEY)
            }
        except redis.exceptions.ConnectionError as e:
            self.logger.error("Redis connection failed in get_metrics.", error=str(e))
            return {}

# =================================================================
# Resource Coordinator
# =================================================================

class ResourceCoordinator:
    """Manages resource allocation using a configurable state management backend."""

    def __init__(self, settings: SystemConfig, logger: SystemLogger, db: DatabaseInterface):
        self.settings = settings
        self.logger = logger
        self.db = db
        self.state_manager: AbstractStateManager
        
        model = settings.orchestrator.deployment_model.upper()
        if model == "EKS":
            try:
                redis_config = settings.redis
                redis_client = redis.Redis(host=redis_config.host, port=redis_config.port, password=redis_config.password, db=redis_config.db, decode_responses=True)
                redis_client.ping()
                self.state_manager = RedisStateManager(redis_client, logger)
                logger.info("ResourceCoordinator initialized with RedisStateManager.")
            except (redis.exceptions.ConnectionError, AttributeError) as e:
                raise ConfigurationError("EKS deployment requires a valid Redis client configuration.", cause=e)
        else: # Defaults to SINGLE_PROCESS
            self.state_manager = InMemoryStateManager()
            logger.info("ResourceCoordinator initialized with InMemoryStateManager.")
        
        try:
            self.system_timezone = ZoneInfo(settings.system.local_timezone)
        except (ZoneInfoNotFoundError, AttributeError):
            self.system_timezone = ZoneInfo("UTC")
            logger.warning("System timezone not configured, defaulting to UTC.", config_key="system.local_timezone")
            
        self.logger.log_component_lifecycle("ResourceCoordinator", "INITIALIZED")

    
    def release_task_reservation(self, datasource_id: str):
        self.state_manager.release_datasource_connection(datasource_id)

    def get_next_job_for_assignment(self, active_jobs: List[Job]) -> Optional[Job]:
        if not active_jobs: return None
        highest_priority = min(j.Priority for j in active_jobs)
        top_priority_jobs = sorted([j for j in active_jobs if j.Priority == highest_priority], key=lambda j: j.name)
        job_ids_str = [str(j.id) for j in top_priority_jobs]
        counts_map = self.state_manager.get_worker_counts_for_jobs(job_ids_str)
        chosen_job = min(top_priority_jobs, key=lambda j: counts_map.get(str(j.id), 0))
        return chosen_job

    def reserve_resources_for_task(self, datasource_id: str) -> ResourceDecision:
        datasource = self.db.get_datasource_with_schedule(datasource_id)
        if not datasource:
            return ResourceDecision(False, f"Datasource {datasource_id} not found.")
        
        host_decision = self.state_manager.check_system_resource_limits(
            self.settings.system.total_process_memory_limit_mb,
            self.settings.worker.global_cpu_threshold_percent
        )
        if not host_decision.is_approved:
            self.logger.warning(host_decision.reason, datasource_id=datasource_id, audit=True)
            return host_decision

        is_allowed, next_window_start_utc = self._is_within_schedule(datasource.calendar)
        if not is_allowed:
            reason = f"Datasource {datasource_id} is outside its allowed schedule."
            snooze_sec = (next_window_start_utc - datetime.now(timezone.utc)).total_seconds() if next_window_start_utc else 300
            self.logger.info(reason, datasource_id=datasource_id, audit=True)
            return ResourceDecision(False, reason, snooze_duration_sec=max(60, int(snooze_sec)))

        limit = datasource.configuration.get("max_connections", 10)
        if self.state_manager.reserve_datasource_connection(datasource_id, limit):
            return ResourceDecision(True, "OK")
        else:
            reason = f"Datasource {datasource_id} connection limit ({limit}) reached."
            self.logger.info(reason, datasource_id=datasource_id, audit=True)
            return ResourceDecision(False, reason, snooze_duration_sec=60)

    def _is_within_schedule(self, calendar) -> (bool, Optional[datetime]):
        if not calendar or not calendar.rules:
            return True, None
        
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(self.system_timezone)
        current_day_str = now_local.strftime('%A').upper()
        
        for rule in calendar.rules:
            if rule.day_of_week.name == current_day_str:
                if (rule.start_time <= rule.end_time and rule.start_time <= now_local.time() < rule.end_time) or \
                   (rule.start_time > rule.end_time and (now_local.time() >= rule.start_time or now_local.time() < rule.end_time)):
                    return True, None
        
        return False, self._calculate_next_window_start(calendar.rules, now_local)

    def _calculate_next_window_start(self, rules, now_local: datetime) -> Optional[datetime]:
        day_map = {0: "MONDAY", 1: "TUESDAY", 2: "WEDNESDAY", 3: "THURSDAY", 4: "FRIDAY", 5: "SATURDAY", 6: "SUNDAY"}
        today_weekday_idx = now_local.weekday()

        todays_rules = sorted([r for r in rules if r.day_of_week.name == day_map[today_weekday_idx] and r.start_time > now_local.time()], key=lambda r: r.start_time)
        if todays_rules:
            next_start_time = todays_rules[0].start_time
            next_window_dt_local = now_local.replace(hour=next_start_time.hour, minute=next_start_time.minute, second=0, microsecond=0)
            return next_window_dt_local.astimezone(timezone.utc)

        for i in range(1, 8):
            next_day_idx = (today_weekday_idx + i) % 7
            next_day_rules = sorted([r for r in rules if r.day_of_week.name == day_map[next_day_idx]], key=lambda r: r.start_time)
            if next_day_rules:
                next_start_time = next_day_rules[0].start_time
                next_day_date = now_local + timedelta(days=i)
                next_window_dt_local = next_day_date.replace(hour=next_start_time.hour, minute=next_start_time.minute, second=0, microsecond=0)
                return next_window_dt_local.astimezone(timezone.utc)
        
        return None