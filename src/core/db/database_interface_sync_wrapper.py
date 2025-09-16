# src/core/db/database_interface_sync_wrapper.py
import asyncio

class DatabaseInterfaceSyncWrapper:
    """Provides synchronous wrappers for async db methods needed by the CLI."""
    def __init__(self, async_db_interface):
        self.db = async_db_interface

    def _run_sync(self, coro):
        return asyncio.run(coro)

    def get_scan_template_by_id(self, template_id: str):
        return self._run_sync(self.db.get_scan_template_by_id(template_id))

    def get_datasources_by_ids(self, datasource_ids: list):
        return self._run_sync(self.db.get_datasources_by_ids(datasource_ids))

    def create_job_execution(self, **kwargs):
        return self._run_sync(self.db.create_job_execution(**kwargs))
    
    # --- NEW METHODS ---

    def issue_job_command(self, job_id: int, command: str):
        """
        Issues a command to a job by updating its 'master_pending_commands' field.
        """
        return self._run_sync(self.db.update_job_command(job_id, command))

    def get_all_child_jobs_for_master(self, master_job_id: str):
        """
        Finds all child jobs in the database associated with a single master_job_id.
        """
        return self._run_sync(self.db.get_child_jobs_by_master_id(master_job_id))