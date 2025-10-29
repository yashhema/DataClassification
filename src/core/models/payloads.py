from datetime import datetime
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field
from .querymodel import SelectionCriteria
from .models import ActionDefinition

class PolicyQueryExecutePayload(BaseModel):
    """Payload for POLICY_QUERY_EXECUTE task (PolicyWorker)"""
    task_type: Literal["POLICY_QUERY_EXECUTE"] = "POLICY_QUERY_EXECUTE"
    policy_template_id: int
    policy_name: str
    query_source: Literal["athena", "yugabyte"]
    data_source: Literal["DISCOVEREDOBJECT", "OBJECTMETADATA", "SCANFINDINGS"]
    policy_storage_scope: Literal["S3", "DB", "BOTH"]
    selection_criteria: SelectionCriteria
    action_definition: ActionDefinition
    incremental_mode: bool = False
    incremental_checkpoints: Optional[Dict[str, datetime]] = None

class PrepareClassificationTasksPayload(BaseModel):
    """Payload for PREPARE_CLASSIFICATION_TASKS task (PolicyWorker)"""
    task_type: Literal["PREPARE_CLASSIFICATION_TASKS"] = "PREPARE_CLASSIFICATION_TASKS"
    scan_type: Literal["structured", "unstructured"]
    full_or_delta: Literal["full", "delta"]
    datasource_ids: List[str]
    classifier_template_id: str
    
class JobClosurePayload(BaseModel):
    """Payload for JOB_CLOSURE task (PolicyWorker)"""
    task_type: Literal["JOB_CLOSURE"] = "JOB_CLOSURE"
    completed_job_id: int
    job_type: str  # From JobType enum
    datasource_ids: List[str]
    template_id: int
    closure_actions: List[Literal[
        "update_timestamps",
        "execute_reporting",
        "promote_to_latest",
        "trigger_merge",
        "update_remediation",
        "update_policy_checkpoint"
    ]]
    write_to_latest: bool = False
    scan_mode: Optional[Literal["full", "delta"]] = None
    reporting_policy_template_id: Optional[int] = None

class S3MergeJobTriggerPayload(BaseModel):
    """Internal payload for triggering S3 merge batch job"""
    scan_job_id: int
    datasource_id: str
    scan_type: Literal["full", "delta"]
    main_path: str
    delta_paths: List[str]
    output_path: str
