# src/orchestrator/threads/pipeliner.py
"""
The Pipeliner thread is a background process that enables asynchronous,
multi-stage workflows. It queries for `TaskOutputRecord`s created by workers
and generates the next set of tasks in the job's processing pipeline.

FIXES APPLIED:
- Added TaskStatus enum import for consistent status comparison
- Updated parent task status check to use TaskStatus.COMPLETED enum
"""

import threading
import time

from core.db_models.job_schema import TaskStatus

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class Pipeliner(threading.Thread):
    """Thread responsible for creating new tasks based on worker progress."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        super().__init__(name="PipelinerThread", daemon=True)
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.interval = orchestrator.settings.orchestrator.pipeliner_interval_sec

    def run(self):
        """The main loop for the Pipeliner thread."""
        self.logger.log_component_lifecycle("Pipeliner", "STARTED")
        
        while not self.orchestrator._shutdown_event.wait(self.interval):
            try:
                # Fetch a batch of unprocessed output records from the database
                records = self.db.get_pending_output_records(limit=100)
                if not records:
                    continue
                
                self.logger.info(f"Pipeliner found {len(records)} output records to process.", count=len(records))
                
                for record in records:
                    try:
                        self._process_output_record(record)
                    except Exception as record_error:
                        self.logger.error(
                            f"Error processing output record {record.ID}",
                            record_id=record.ID,
                            exc_info=True
                        )
                        # Mark record as failed but continue processing others
                        try:
                            self.db.update_output_record_status(record.ID, "FAILED")
                        except Exception:
                            self.logger.error(f"Failed to update status for record {record.ID}")
                        continue

                # Update the liveness timestamp to show the thread is healthy
                self.orchestrator.update_thread_liveness("pipeliner")
            
            except Exception as e:
                # Catch exceptions to prevent the thread from crashing
                self.logger.error("An unexpected error occurred in the Pipeliner loop.", exc_info=True)
                # Avoid a tight loop on repeated database failures
                time.sleep(self.interval)

        self.logger.log_component_lifecycle("Pipeliner", "STOPPED")

    def _process_output_record(self, record):
        """Process a single output record and create next stage tasks."""
        parent_task = self.db.get_task_by_id(record.TaskID)
        
        # FIXED: Use TaskStatus enum instead of string comparison
        if not parent_task or parent_task.Status != TaskStatus.COMPLETED:
            self.logger.warning(
                "Orphaning output record from a non-completed or missing parent task.",
                record_id=record.ID,
                parent_task_id=record.TaskID,
                parent_task_status=parent_task.Status.value if parent_task else "MISSING"
            )
            self.db.update_output_record_status(record.ID, "ORPHANED")
            return

        # Core pipeline logic: create next task based on the output type
        self._create_next_stage_task(parent_task, record)
        
        # Mark the record as processed to prevent duplicate task creation
        self.db.update_output_record_status(record.ID, "PROCESSED")

    def _create_next_stage_task(self, parent_task, record):
        """Creates the correct downstream task based on the parent's output."""
        
        # Validate that we have a valid payload
        if not record.OutputPayload:
            self.logger.warning(
                f"Output record {record.ID} has empty payload, skipping task creation.",
                record_id=record.ID,
                output_type=record.OutputType
            )
            return
        
        # This is the core Discovery -> Get Details -> Classification pipeline logic
        if record.OutputType == "DISCOVERED_OBJECTS":
            self.db.create_task(
                job_id=parent_task.JobID,
                task_type="DISCOVERY_GET_DETAILS",
                work_packet={"payload": record.OutputPayload}, # Payload contains object_ids
                datasource_id=parent_task.DatasourceID,
                parent_task_id=parent_task.ID
            )
            self.logger.info("Pipeliner created DISCOVERY_GET_DETAILS task.", parent_task_id=parent_task.ID)
            
        elif record.OutputType == "OBJECT_DETAILS_FETCHED":
            # This completes the pipeline by creating a classification task
            self.db.create_task(
                job_id=parent_task.JobID,
                task_type="CLASSIFICATION",
                work_packet={"payload": record.OutputPayload}, # Payload also contains object_ids
                datasource_id=parent_task.DatasourceID,
                parent_task_id=parent_task.ID
            )
            self.logger.info("Pipeliner created CLASSIFICATION task.", parent_task_id=parent_task.ID)
            
        # Other pipeline stages (e.g., DELTA_CALCULATE) could be added here
        else:
            self.logger.warning(f"Unknown OutputType '{record.OutputType}' in Pipeliner.", output_type=record.OutputType)