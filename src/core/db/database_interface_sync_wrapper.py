import asyncio
import traceback
import threading

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
        try:
            # Fix: Use repr() instead of __name__ since coroutines don't have __name__
            coro_name = repr(coro)[:100]  # Truncate for readability
            print(f"CLI THREAD: Submitting coroutine {coro_name} to main event loop...")
            
            # Debug info
            print(f"CLI THREAD: Current thread: {threading.current_thread().name}")
            print(f"CLI THREAD: Main loop running: {self.main_loop.is_running()}")
            print(f"CLI THREAD: Main loop closed: {self.main_loop.is_closed()}")
            
            # Submit the coroutine
            future = asyncio.run_coroutine_threadsafe(coro, self.main_loop)
            print(f"CLI THREAD: Future created: {future}")
            
            print(f"CLI THREAD: Waiting for result...")
            # Add timeout to prevent infinite hanging
            try:
                result = future.result(timeout=30.0)  # 30 second timeout
                print(f"CLI THREAD: Received result.")
                return result
            except asyncio.TimeoutError:
                print(f"CLI THREAD: TIMEOUT waiting for coroutine result!")
                future.cancel()
                raise RuntimeError("Database operation timed out after 30 seconds")
            
        except Exception as e:
            print(f"CLI THREAD: Exception in _run_coro_in_main_loop: {e}")
            print(f"CLI THREAD: Traceback: {traceback.format_exc()}")
            raise

    def get_scan_template_by_id(self, template_id: str):
        print(f"CLI THREAD: Getting scan template for ID: {template_id}")
        return self._run_coro_in_main_loop(self.db.get_scan_template_by_id(template_id))

    def get_datasources_by_ids(self, datasource_ids: list):
        print(f"CLI THREAD: Getting datasources for IDs: {datasource_ids}")
        return self._run_coro_in_main_loop(self.db.get_datasources_by_ids(datasource_ids))

    def create_job_execution(self, **kwargs):
        print(f"CLI THREAD: Creating job execution")
        return self._run_coro_in_main_loop(self.db.create_job_execution(**kwargs))
    
    def issue_job_command(self, job_id: int, command: str):
        print(f"CLI THREAD: Issuing command {command} to job {job_id}")
        return self._run_coro_in_main_loop(self.db.update_job_command(job_id, command))

    def get_all_child_jobs_for_master(self, master_job_id: str):
        print(f"CLI THREAD: Getting child jobs for master: {master_job_id}")
        return self._run_coro_in_main_loop(self.db.get_child_jobs_by_master_id(master_job_id))

    def get_active_jobs(self):
        print(f"CLI THREAD: Getting active jobs")
        return self._run_coro_in_main_loop(self.db.get_active_jobs())

    def get_template_for_job(self, job):
        print(f"CLI THREAD: Getting template for job {job.id}")
        return self._run_coro_in_main_loop(self.db.get_template_for_job(job))

    def get_job_progress_summary(self, job_id):
        print(f"CLI THREAD: Getting progress summary for job {job_id}")
        return self._run_coro_in_main_loop(self.db.get_job_progress_summary(job_id))

    def get_jobs_by_ids(self, job_ids: list):
        print(f"CLI THREAD: Getting jobs by IDs: {job_ids}")
        return self._run_coro_in_main_loop(self.db.get_jobs_by_ids(job_ids))
        
    def create_master_job(self, **kwargs):
        """Synchronous wrapper to create the MasterJob record."""
        print(f"CLI THREAD: Creating master job")
        return self._run_coro_in_main_loop(self.db.create_master_job(**kwargs))

    def create_master_job_summary(self, **kwargs):
        """Synchronous wrapper to create the MasterJobStateSummary record."""
        print(f"CLI THREAD: Creating master job summary")
        return self._run_coro_in_main_loop(self.db.create_master_job_summary(**kwargs))
        
    def start_job_transactional(self, job_details: dict):
        """
        Synchronous wrapper for a new, single async method that creates the master job,
        summary, and all child jobs in one atomic transaction.
        NOTE: This requires a corresponding `start_job_transactional` method to be added
        to the async `DatabaseInterface` class.
        """
        print(f"CLI THREAD: Starting transactional job creation...")
        return self._run_coro_in_main_loop(self.db.start_job_transactional(job_details))
