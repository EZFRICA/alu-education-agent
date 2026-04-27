import asyncio
from typing import Dict, Any, List
import sys
import os

# Local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from logger import get_logger

logger = get_logger(__name__)

class LocalScheduler:
    """
    Manages background asynchronous tasks for the local app (GC, Optimization).
    """
    def __init__(self):
        self.queue = asyncio.Queue()
        self._is_running = False

    async def push(self, task_type: str, payload: Dict[str, Any], priority: int = 1):
        """Adds a task to the queue."""
        await self.queue.put((priority, task_type, payload))
        logger.debug(f"Scheduler: Task added {task_type}")

    async def start(self):
        """Starts the scheduler loop."""
        if self._is_running: return
        self._is_running = True
        logger.info("Local scheduler started.")
        
        while self._is_running:
            priority, task_type, payload = await self.queue.get()
            try:
                await self._handle_task(task_type, payload)
            except Exception as e:
                logger.error(f"Scheduler error on {task_type}: {e}")
            finally:
                self.queue.task_done()

    async def _handle_task(self, task_type: str, payload: Dict[str, Any]):
        """Executes the task logic."""
        if task_type == "GC_OPTIMIZE":
            # Local LanceDB cleanup/optimization could be implemented here if needed
            logger.debug(f"GC: Optimization for block {payload.get('block_id')}")
        else:
            logger.warning(f"Unknown task: {task_type}")

# Global Instance
scheduler = LocalScheduler()
