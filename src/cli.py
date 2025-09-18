# Modified cli.py - Option 1: Use Main Event Loop

import asyncio
import time
import logging
import threading
from collections import defaultdict

# Core infrastructure imports needed by the CLI
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger
from core.db_models.job_schema import JobType

class DatabaseInterfaceSyncWrapper:
    """
    Thread-safe wrapper that submits database operations to the main event loop
    instead of creating a separate loop.
    """
    def __init__(self, async_db_interface, main_loop):
        self.db = async_db_interface
        self.main_loop = main_loop  # Reference to main app's event loop

    def _run_coro_in_main_loop(self, coro):
        """Submits a coroutine to the main event loop and waits for result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.main_loop)
        return future.result()  # This blocks until the coroutine completes

    def get_scan_template_by_id(self, template_id: str):
        return self._run_coro_in_main_loop(self.db.get_scan_template_by_id(template_id))

    def get_datasources_by_ids(self, datasource_ids: list):
        return self._run_coro_in_main_loop(self.db.get_datasources_by_ids(datasource_ids))

    def create_job_execution(self, **kwargs):
        return self._run_coro_in_main_loop(self.db.create_job_execution(**kwargs))
    
    def issue_job_command(self, job_id, command):
        return self._run_coro_in_main_loop(self.db.update_job_command(job_id, command))

    def get_all_child_jobs_for_master(self, master_job_id):
        return self._run_coro_in_main_loop(self.db.get_child_jobs_by_master_id(master_job_id))

    def shutdown(self):
        """No separate loop to shutdown - database will be closed by main app."""
        pass

def run_cli(db_sync: DatabaseInterfaceSyncWrapper, settings):
    print("CLI Ready. Type 'help' for commands.")
    while True:
        try:
            command_line = input("> ")
            parts = command_line.split()
            if not parts: continue
            command = parts[0].lower()

            if command == "start_job":
                template_id = parts[1]
                scan_template = db_sync.get_scan_template_by_id(template_id)
                if not scan_template:
                    print(f"Error: ScanTemplate '{template_id}' not found.")
                    continue

                datasource_targets = scan_template.configuration.get("datasource_targets", [])
                datasource_ids = [t.get("datasource_id") for t in targets]
                
                # Fetch the full datasource objects
                datasources = db_sync.get_datasources_by_ids(datasource_ids)
                if not datasources:
                    print(f"Error: No valid datasources found for the IDs in template '{template_id}'.")
                    continue

                master_job_id = f"master-{template_id}-{int(time.time())}"
                
                # --- NEW LOGIC: Loop per datasource ---
                for ds in datasources:
                    # Create a self-contained configuration for this single datasource job
                    child_config = scan_template.configuration.copy()
                    child_config["datasource_targets"] = [{"datasource_id": ds.datasource_id}]
                    
                    # Inject the system-level failure threshold for this job
                    child_config["failure_threshold_percent"] = settings.job.failure_threshold_percent

                    # Determine the nodegroup for this specific datasource
                    nodegroup_name = ds.node_group.name if ds.node_group else 'default'

                    db_sync.create_job_execution(
                        master_job_id=master_job_id,
                        template_table_id=scan_template.id,
                        template_type=scan_template.job_type,
                        execution_id=f"run-{master_job_id}-{ds.datasource_id}", # Unique ID per datasource
                        trigger_type="cli",
                        nodegroup=nodegroup_name,
                        configuration=child_config,
                        priority=scan_template.priority,
                        master_pending_commands="START"
                    )
                
                print(f"Successfully created {len(datasources)} jobs under master_job_id: {master_job_id}")
            elif command == "list_jobs":
                print("Fetching all active jobs (Queued, Running, Pausing, Cancelling)...")
                active_jobs = db_sync.get_active_jobs()
                if not active_jobs:
                    print("No active jobs found.")
                    continue
                
                print(f"{'Job ID':<10} {'Status':<12} {'Template Name':<25} {'Start Time (UTC)':<22}")
                print(f"{'-'*10} {'-'*12} {'-'*25} {'-'*22}")
                
                for job in active_jobs:
                    template = db_sync.get_template_for_job(job)
                    template_name = template.name if template else "N/A"
                    start_time = job.created_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"{job.id:<10} {job.status.value:<12} {template_name:<25} {start_time:<22}")

# Replace the entire 'pause, resume, cancel, status' block with this new version
            elif command in ["pause", "resume", "cancel"]:
                master_id = parts[1]
                child_jobs = db_sync.get_all_child_jobs_for_master(master_id)
                if not child_jobs:
                    print(f"No jobs found for master_job_id '{master_id}'.")
                    continue

                command_to_issue = command.upper()
                for job in child_jobs:
                    db_sync.issue_job_command(job.id, command_to_issue)
                print(f"Sent '{command_to_issue}' command to {len(child_jobs)} child jobs.")

            elif command == "status":
                if len(parts) < 2:
                    print("Usage: status <master_job_id or job_id_1> [job_id_2] ...")
                    continue
                
                job_ids_to_check = []
                # First, try treating the ID as a master_job_id
                master_id = parts[1]
                child_jobs = db_sync.get_all_child_jobs_for_master(master_id)
                
                if child_jobs:
                    print(f"--- Status for Master Job '{master_id}' ---")
                    job_ids_to_check = [job.id for job in child_jobs]
                else:
                    # If not a master_id, treat all parts as individual integer job IDs
                    print(f"--- Status for Individual Job(s) ---")
                    try:
                        job_ids_to_check = [int(job_id_str) for job_id_str in parts[1:]]
                    except ValueError:
                        print(f"Error: '{parts[1]}' is not a valid master_job_id or integer job_id.")
                        continue
                
                jobs = db_sync.get_jobs_by_ids(job_ids_to_check)
                if not jobs:
                    print("No valid jobs found for the specified IDs.")
                    continue

                for job in jobs:
                    summary = db_sync.get_job_progress_summary(job.id)
                    total = sum(summary.values())
                    summary_str = ", ".join([f"{status}: {count}" for status, count in summary.items()])
                    print(f"\n> Job ID: {job.id} | Status: {job.status.value} | NodeGroup: {job.node_group}")
                    print(f"  Tasks ({total} total): {summary_str if summary else 'No tasks created yet.'}")


            elif command == "help":
                print("Available Commands: start_job, list_jobs, status, pause, resume, cancel, exit")













            
            elif command == "exit":
                break
            else:
                print("Unknown command. Type 'help'.")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("CLI cannot run standalone with Option 1 - must be started from main.py")
    print("Run: python main.py --mode single_process")