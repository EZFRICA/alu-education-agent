"""
APU Scheduler — The Semantic Control Unit.
Manages asynchronous task priorities and background execution for the Agent Processor Unit.

Enables "Letta as a Disk" by deferring Cloud writes while keeping L1/L3 writes synchronous.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional
from logger import get_logger

logger = get_logger(__name__)

@dataclass(order=True)
class APUTask:
    priority: int  # 0: Real-time (User UI), 1: Normal (Tool), 2: Background (Sync/GC)
    task_type: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    timestamp: float = field(default_factory=time.time)
    attempts: int = field(default=0, compare=False) # Tracking attempts for retry
    callback: Optional[Callable] = field(default=None, compare=False)

class APUScheduler:
    _instance: Optional['APUScheduler'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(APUScheduler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.queue: Optional[asyncio.PriorityQueue[APUTask]] = None
        self.worker_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._initialized = True
        logger.info("APU Scheduler initialized.")

    async def start(self):
        """Start the background worker loop in its dedicated thread."""
        self._loop = asyncio.get_running_loop()
        self.queue = asyncio.PriorityQueue()
        if self.worker_task and not self.worker_task.done():
            return
        self.worker_task = asyncio.create_task(self._worker_loop())
        logger.info("APU Scheduler background worker started.")

    async def stop(self):
        """Stop the background worker loop."""
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            logger.info("APU Scheduler background worker stopped.")

    async def push(self, task_type: str, payload: Dict[str, Any], priority: int = 2):
        """Add a task to the queue (Thread-safe from any loop)."""
        task = APUTask(priority=priority, task_type=task_type, payload=payload)
        
        if self._loop is None or self.queue is None:
            logger.warning("Scheduler not running. Task DROPPED: %s", task_type)
            return

        # Safely schedule the queue.put coroutine on the worker's thread loop
        asyncio.run_coroutine_threadsafe(self.queue.put(task), self._loop)
        logger.debug("Task PUSHED (Thread-safe): %s (Pri: %d)", task_type, priority)

    async def _worker_loop(self):
        """Continuous loop processing tasks from the priority queue."""
        while True:
            task = await self.queue.get()
            start_time = time.time()
            logger.debug("Task START: %s", task.task_type)

            try:
                await self._dispatch(task)
                duration = (time.time() - start_time) * 1000
                logger.debug("Task DONE: %s (took %.2fms)", task.task_type, duration)
            except Exception as e:
                task.attempts += 1
                if task.attempts < 3:
                    logger.warning("Task RETRY (%d/3): %s Error: %s", task.attempts, task.task_type, e)
                    # Push back with lower priority and slight delay simulation
                    task.priority += 1
                    await asyncio.sleep(2) # Backoff
                    await self.queue.put(task)
                else:
                    logger.error("Task ABORTED after 3 attempts: %s Error: %s", task.task_type, e)
            finally:
                self.queue.task_done()

    async def _dispatch(self, task: APUTask):
        """Route tasks to their specific handlers."""
        if task.task_type == "SYNC_LETTA":
            await self._handle_sync_letta(task.payload)
        elif task.task_type == "GC_OPTIMIZE":
            await self._handle_gc_optimize(task.payload)
        else:
            logger.warning("Unknown task type: %s", task.task_type)

    # ── Task Handlers ─────────────────────────────────────────────────────────

    async def _handle_sync_letta(self, payload: Dict[str, Any]):
        """
        Deferred write to Letta Cloud. 
        Ensures the 'slow' cloud storage is updated without blocking the user.
        """
        from apu.storage.letta_driver import update_block, append_block
        agent_id = payload.get("agent_id")
        block_id = payload.get("block_id")
        content = payload.get("content")

        if not all([agent_id, block_id, content]):
            logger.error("SYNC_LETTA: Missing payload data.")
            return

        logger.debug("SYNC_LETTA: Writing '%s' to Letta Cloud for agent %s...", block_id, agent_id)
        try:
            await update_block(agent_id, block_id, content)
            logger.info("L4 SYNC DONE: '%s' updated in Letta Cloud.", block_id)
        except Exception as e:
            logger.debug("L4 SYNC: Update failed '%s' (%s), attempting append...", block_id, str(e))
            await append_block(agent_id, block_id, content)
            logger.info("L4 SYNC DONE: '%s' created in Letta Cloud.", block_id)

    async def _handle_gc_optimize(self, payload: Dict[str, Any]):
        """
        Background optimization — Move-To-Front (MTF).
        Reorders the DLL nodes in background to optimize search hits.
        """
        from apu.mmu.controller import load_dll, move_to_front, save_dll
        block_id = payload.get("block_id")
        
        if not block_id:
            return

        logger.debug("GC Worker: Optimizing DLL for block '%s'...", block_id)
        # 1. Load current state
        dll = await load_dll()
        # 2. Reorder (MTF)
        dll = move_to_front(block_id, dll)
        # 3. Persist
        save_dll(dll)
        logger.info("GC Worker: DLL reordered (MTF for '%s').", block_id)

# Global singleton
scheduler = APUScheduler()
