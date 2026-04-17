"""
TEU — Tool Execution Unit.
The IO Controller of the APU. Handles tool lifecycle, sandboxing, and result caching.
"""

import time
import hashlib
import json
from typing import Any, Dict, Optional
from logger import get_logger

logger = get_logger(__name__)

class TEUController:
    _instance: Optional['TEUController'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TEUController, cls).__new__(cls)
            cls._instance._io_cache = {} # Key: Hash of (tool_name + params), Value: (result, timestamp)
            cls._ttl = 900 # 15 minutes cache for IO results
        return cls._instance

    def _generate_cache_key(self, tool_name: str, params: Dict[str, Any]) -> str:
        """Generate a unique hash for a tool call."""
        param_str = json.dumps(params, sort_keys=True)
        return hashlib.md5(f"{tool_name}:{param_str}".encode()).hexdigest()

    async def execute_tool(self, tool_name: str, tool_func: Any, **kwargs) -> Any:
        """
        Execute a tool through the TEU Controller.
        Implements L1 IO Caching and Performance Monitoring.
        """
        cache_key = self._generate_cache_key(tool_name, kwargs)
        
        # ── 1. Check IO Cache ─────────────────────────────────────────────────
        if cache_key in self._io_cache:
            result, timestamp = self._io_cache[cache_key]
            if time.time() - timestamp < self._ttl:
                logger.info("TEU L1 CACHE HIT: Tool '%s' returned from cache.", tool_name)
                return result
            else:
                del self._io_cache[cache_key] # Cache expired

        # ── 2. Actual Execution ───────────────────────────────────────────────
        start_time = time.time()
        logger.info("TEU EXECUTING: Tool '%s'...", tool_name)
        
        try:
            # Handle both async and sync tools
            if callable(tool_func):
                 # Check if it's a coroutine function
                 import inspect
                 if inspect.iscoroutinefunction(tool_func):
                     result = await tool_func(**kwargs)
                 else:
                     result = tool_func(**kwargs)
            else:
                raise ValueError(f"Tool function for '{tool_name}' is not callable.")

            duration = (time.time() - start_time) * 1000
            logger.info("TEU DONE: Tool '%s' finished in %.2fms.", tool_name, duration)

            # ── 3. Update IO Cache ────────────────────────────────────────────
            self._io_cache[cache_key] = (result, time.time())
            
            return result

        except Exception as e:
            logger.error("TEU ERROR: Tool '%s' failed: %s", tool_name, e)
            raise e

    def clear_cache(self):
        """Clear all cached IO results."""
        self._io_cache = {}
        logger.info("TEU IO Cache cleared.")

# Global TEU Singleton
teu = TEUController()
