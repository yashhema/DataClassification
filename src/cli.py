# cli.py
import asyncio
import time
from collections import defaultdict

# Core infrastructure imports needed by the CLI
from src.core.config.configuration_manager import ConfigurationManager
from src.core.db.database_interface import DatabaseInterface
from src.core.errors import ErrorHandler
from src.core.logging.system_logger import SystemLogger # Needed for db interface
from src.core.db_models.job_schema import JobType

class DatabaseInterfaceSyncWrapper:
    """Provides synchronous wrappers for async db methods needed by the CLI."""
    def __init__(self, async_db_interface):
        self.db = async_db_interface

    def _run_sync(self, coro):
        # A simple way to run a coroutine from a sync function
        return asyncio.run(coro)

    def get_scan_template_by_id(self, template_id: str):
        return self._run_sync(self.db.get_scan_template_by_id(template_id))

    def get_datasources_by_ids(self, datasource_ids: list):
        return self._run_sync(self.db.get_datasources_by_ids(datasource_ids))

    def create_job_execution(self, **kwargs):
        return self._run_sync(self.db.create_job_execution(**kwargs))
    
    def issue_job_command(self, job_id, command):
        return self._run_sync(self.db.update_job_command(job_id, command))

    def get_all_child_jobs_for_master(self, master_job_id):
        return self._run_sync(self.db.get_child_jobs_by_master_id(master_job_id))


def run_cli(db_interface_sync: DatabaseInterfaceSyncWrapper):
    print("CLI Ready. Type 'help' for commands.")
    while True:
        try:
            command_line = input("> ")
            parts = command_line.split()
            if not parts:
                continue
            command = parts[0].lower()

            if command == "start_job":
                template_id = parts[1]
                
                # 1. Fetch the ScanTemplate
                scan_template = db_interface_sync.get_scan_template_by_id(template_id)
                if not scan_template:
                    print(f"Error: ScanTemplate '{template_id}' not found.")
                    continue

                # 2. Group target datasource_ids by their NodeGroup
                targets = scan_template.configuration.get("datasource_targets", [])
                datasource_ids = [t.get("datasource_id") for t in targets]
                
                datasources = db_interface_sync.get_datasources_by_ids(datasource_ids)
                nodegroup_map = defaultdict(list)
                for ds in datasources:
                    nodegroup_map[ds.NodeGroup].append(ds.datasource_id)

                # 3. Create a separate Child Job for each NodeGroup
                created_job_ids = []
                master_job_id = f"master-{template_id}-{int(time.time())}"
                
                for nodegroup, ds_ids in nodegroup_map.items():
                    child_job_config = scan_template.configuration.copy()
                    child_job_config["datasource_targets"] = [{"datasource_id": ds_id} for ds_id in ds_ids]
                    
                    execution_id = f"run-{master_job_id}-{nodegroup}"
                    
                    job = db_interface_sync.create_job_execution(
                        master_job_id=master_job_id,
                        template_table_id=scan_template.id,
                        template_type=scan_template.job_type,
                        execution_id=execution_id,
                        trigger_type="cli",
                        nodegroup=nodegroup,
                        configuration=child_job_config,
                        priority=scan_template.priority,
                        # Set initial command
                        master_pending_commands="START"
                    )
                    created_job_ids.append(job.id)
                
                print(f"Successfully created {len(created_job_ids)} child jobs under master_job_id: {master_job_id}")

            elif command in ["pause", "resume", "cancel", "status"]:
                master_id = parts[1]
                if command == "status":
                    child_jobs = db_interface_sync.get_all_child_jobs_for_master(master_id)
                    print(f"Status for master job '{master_id}': Found {len(child_jobs)} child jobs.")
                    for job in child_jobs:
                        print(f"  - Child Job ID: {job.id}, NodeGroup: {job.NodeGroup}, Status: {job.status.value}")
                else:
                    # For other commands, issue them to all child jobs
                    child_jobs = db_interface_sync.get_all_child_jobs_for_master(master_id)
                    command_to_issue = command.upper()
                    for job in child_jobs:
                        db_interface_sync.issue_job_command(job.id, command_to_issue)
                    print(f"Sent '{command_to_issue}' command to {len(child_jobs)} child jobs.")

            elif command == "help":
                print("Available Commands:")
                print("  start_job <template_id>")
                print("  status <master_job_id>")
                print("  pause <master_job_id>")
                print("  resume <master_job_id>")
                print("  cancel <master_job_id>")
                print("  exit")
            
            elif command == "exit":
                break
            else:
                print("Unknown command. Type 'help'.")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("Initializing CLI...")
    # The CLI needs its own connection to the database
    config = ConfigurationManager("config/system_default.yaml")
    # Provide dummy logger for db interface initialization
    dummy_logger = SystemLogger(logging.getLogger("cli"), "TEXT", {})
    db_interface = DatabaseInterface(config.get_db_connection_string(), dummy_logger, ErrorHandler())
    db_interface_sync = DatabaseInterfaceSyncWrapper(db_interface)
    
    run_cli(db_interface_sync)