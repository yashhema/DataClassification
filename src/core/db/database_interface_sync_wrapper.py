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
        return future.result()

    def get_scan_template_by_id(self, template_id: str):
        return self._run_coro_in_main_loop(self.db.get_scan_template_by_id(template_id))

    def get_datasources_by_ids(self, datasource_ids: list):
        return self._run_coro_in_main_loop(self.db.get_datasources_by_ids(datasource_ids))

    def create_job_execution(self, **kwargs):
        return self._run_coro_in_main_loop(self.db.create_job_execution(**kwargs))
    
    def issue_job_command(self, job_id: int, command: str):
        return self._run_coro_in_main_loop(self.db.update_job_command(job_id, command))

    def get_all_child_jobs_for_master(self, master_job_id: str):
        return self._run_coro_in_main_loop(self.db.get_child_jobs_by_master_id(master_job_id))
        

    def get_active_jobs(self):
        # Fetches jobs that are in a non-terminal state
        return self._run_coro_in_main_loop(self.db.get_active_jobs())

    def get_template_for_job(self, job):
        # Gets the template (Scan or Policy) associated with a job
        return self._run_coro_in_main_loop(self.db.get_template_for_job(job))

    def get_job_progress_summary(self, job_id):
        # Gets the task status counts for a given job ID
        return self._run_coro_in_main_loop(self.db.get_job_progress_summary(job_id))

    def get_jobs_by_ids(self, job_ids: list):
        # Fetches job objects by their integer IDs
        return self._run_coro_in_main_loop(self.db.get_jobs_by_ids(job_ids))