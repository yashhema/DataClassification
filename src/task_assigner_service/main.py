"""
Standalone TaskAssigner service using FastAPI.
Provides task assignment with FOR UPDATE SKIP LOCKED for scalability.
"""

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
from datetime import datetime, timezone, timedelta
import logging

from core.db_interface import DatabaseInterface
from core.logging.system_logger import SystemLogger

app = FastAPI(title="TaskAssigner Service", version="1.0.0")

# Initialize logger
logger = SystemLogger(
    logger=logging.getLogger(__name__),
    log_format="JSON",
    ambient_context={
        "component_name": "TaskAssigner",
        "deployment_model": "standalone",
        "machine_name": "task-assigner-pod",
        "node_group": "central"
    }
)

class TaskRequest(BaseModel):
    """Request for next available task"""
    worker_id: str
    worker_type: Literal["DATACENTER", "POLICY"]
    nodegroup: str

class TaskResponse(BaseModel):
    """Response with assigned task"""
    task_id: str
    work_packet: dict
    lease_duration: int  # seconds

@app.post("/api/v1/tasks/get-next")
async def get_next_task(
    request: TaskRequest,
    db: DatabaseInterface = Depends(get_db)
) -> Optional[TaskResponse]:
    """
    Assign next available task using atomic FOR UPDATE SKIP LOCKED.
    
    Returns None if no tasks available.
    """
    
    logger.info(
        "Task assignment request",
        worker_id=request.worker_id,
        worker_type=request.worker_type,
        nodegroup=request.nodegroup
    )
    
    try:
        # Execute FOR UPDATE SKIP LOCKED query
        # This provides atomic task assignment across multiple workers
        
        lease_duration = 300  # 5 minutes
        lease_expiry = datetime.now(timezone.utc) + timedelta(seconds=lease_duration)
        
        query = f"""
            WITH NextTask AS (
                SELECT TOP 1 id
                FROM tasks WITH (ROWLOCK, READPAST)
                WHERE status = 'PENDING'
                  AND node_group = '{request.nodegroup}'
                  AND (eligible_worker_type = '{request.worker_type}' 
                       OR eligible_worker_type IS NULL)
                  AND (lease_expiry IS NULL OR lease_expiry < SYSDATETIMEOFFSET())
                ORDER BY priority DESC, created_at ASC
            )
            UPDATE t
            SET status = 'ASSIGNED',
                worker_id = '{request.worker_id}',
                lease_expiry = '{lease_expiry.isoformat()}',
                assigned_at = SYSDATETIMEOFFSET()
            OUTPUT INSERTED.id, INSERTED.work_packet
            FROM tasks t
            INNER JOIN NextTask nt ON t.id = nt.id
        """
        
        result = await db.execute_raw_sql(query)
        
        if not result:
            logger.debug(
                "No tasks available",
                worker_id=request.worker_id,
                worker_type=request.worker_type
            )
            return None
        
        task_id = result[0]["id"]
        work_packet = result[0]["work_packet"]
        
        logger.info(
            "Task assigned successfully",
            task_id=task_id.hex(),
            worker_id=request.worker_id,
            worker_type=request.worker_type
        )
        
        return TaskResponse(
            task_id=task_id.hex(),
            work_packet=work_packet,
            lease_duration=lease_duration
        )
    
    except Exception as e:
        logger.error(
            "Task assignment failed",
            worker_id=request.worker_id,
            error=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/tasks/{task_id}/heartbeat")
async def extend_lease(
    task_id: str,
    worker_id: str,
    db: DatabaseInterface = Depends(get_db)
):
    """Extend task lease (heartbeat)"""
    
    try:
        task_id_bytes = bytes.fromhex(task_id)
        new_expiry = datetime.now(timezone.utc) + timedelta(seconds=300)
        
        await db.execute_raw_sql(f"""
            UPDATE tasks
            SET lease_expiry = '{new_expiry.isoformat()}'
            WHERE id = 0x{task_id}
              AND worker_id = '{worker_id}'
              AND status = 'ASSIGNED'
        """)
        
        return {"status": "ok", "new_expiry": new_expiry.isoformat()}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

# Dependency injection for DatabaseInterface
async def get_db() -> DatabaseInterface:
    # Initialize from environment/config
    # Return shared database interface instance
    pass
