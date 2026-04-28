import time
import threading
import logging

log = logging.getLogger(__name__)

class TimeBudget:
    """
    Track elapsed wall-clock time for operations with a fixed duration budget.
    
    This class solves the per-chunk vs per-request timeout problem by sharing
     a single deadline across multiple serial or parallel operations.
    
    Usage:
        budget = TimeBudget(max_seconds=2.0)
        while not budget.exhausted:
            result = do_something(timeout=budget.remaining)
            if not result:
                break
    
    Thread-safety: Uses time.monotonic() which is thread-safe and immune
    to system clock adjustments.
    """
    
    # Minimum wait granularity - how often we check if budget is exhausted
    # during a wait. Smaller = more responsive but slightly more CPU.
    WAIT_GRANULARITY_SEC = 0.05
    
    def __init__(self, max_seconds: float):
        """
        Initialize a time budget.
        
        Args:
            max_seconds: Maximum wall-clock time allowed for this budget.
        """
        self.max_seconds = max_seconds
        self.start_time = time.monotonic()
        self._exhausted = False
    
    @property
    def timeout(self) -> float:
        """Alias for max_seconds for compatibility."""
        return self.max_seconds

    @property
    def remaining(self) -> float:
        """Return remaining time in seconds (never negative)."""
        return max(0.0, self.max_seconds - self.elapsed)
    
    @property 
    def elapsed(self) -> float:
        """Return elapsed time since budget creation in seconds."""
        return time.monotonic() - self.start_time
    
    @property
    def exhausted(self) -> bool:
        """
        Check if the time budget is exhausted.
        
        Once exhausted, always returns True (sticky flag for efficiency).
        """
        if self._exhausted:
            return True
        if self.elapsed >= self.max_seconds:
            self._exhausted = True
            return True
        return False
    
    def wait_with_budget(self, event: threading.Event, max_single_wait: float = None) -> bool:
        """
        Wait on an event while respecting both the time budget AND an optional per-operation timeout.
        
        Args:
            event: A threading.Event to wait on.
            max_single_wait: Optional per-operation timeout in seconds.
        
        Returns:
            True if the event was set (success)
            False if budget exhausted or max_single_wait exceeded before event was set
        """
        # Fast path: already set
        if event.is_set():
            return True
        
        # Fast path: budget already gone
        if self.exhausted:
            return event.is_set()
        
        # Track start time for max_single_wait
        single_wait_start = time.monotonic() if max_single_wait else None
        
        # Poll with granularity until event set or budget/maxwait exhausted
        while not self.exhausted:
            # Check max_single_wait limit
            if max_single_wait:
                single_elapsed = time.monotonic() - single_wait_start
                if single_elapsed >= max_single_wait:
                    return event.is_set()
            
            # Determine wait time for this step
            remaining_budget = self.remaining
            wait_time = min(remaining_budget, self.WAIT_GRANULARITY_SEC)
            if max_single_wait:
                remaining_single = max(0.0, max_single_wait - (time.monotonic() - single_wait_start))
                wait_time = min(wait_time, remaining_single)
            
            if wait_time <= 0:
                break
                
            # Perform granular wait
            if event.wait(timeout=wait_time):
                return True
                
        return event.is_set()

    def record_chunk_processed(self):
        """Record that a chunk was successfully processed."""
        if not hasattr(self, '_chunks_processed'):
            self._chunks_processed = 0
        self._chunks_processed += 1
    
    def record_chunk_skipped(self):
        """Record that a chunk was skipped due to budget exhaustion."""
        if not hasattr(self, '_chunks_skipped'):
            self._chunks_skipped = 0
        self._chunks_skipped += 1
    
    @property
    def chunks_processed(self) -> int:
        """Number of chunks successfully processed within budget."""
        return getattr(self, '_chunks_processed', 0)
    
    @property
    def chunks_skipped(self) -> int:
        """Number of chunks skipped due to budget exhaustion."""
        return getattr(self, '_chunks_skipped', 0)

    def __repr__(self):
        return (f"TimeBudget(max={self.max_seconds:.2f}s, "
                f"elapsed={self.elapsed:.2f}s, "
                f"remaining={self.remaining:.2f}s, "
                f"exhausted={self.exhausted})")
