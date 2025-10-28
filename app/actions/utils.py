"""
Utility classes and functions for action handlers.
"""
import logging
import redis.asyncio as redis
import stamina
from app import settings

logger = logging.getLogger(__name__)


class FileProcessingLockManager:
    """
    Manages file processing locks to prevent concurrent processing of the same file.
    Uses Redis with expiration to ensure locks are automatically released.
    """
    
    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_STATE_DB)
        self.db_client = redis.Redis(host=host, port=port, db=db)
        self.lock_timeout = 3600  # 1 hour timeout for locks
    
    async def acquire_lock(self, integration_id: str, file_name: str) -> bool:
        """
        Try to acquire a lock for processing a file.
        
        Args:
            integration_id: Integration ID
            file_name: Name of the file to lock
            
        Returns:
            True if lock was acquired, False if file is already being processed
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        
        try:
            for attempt in stamina.retry_context(on=redis.RedisError, attempts=3, wait_initial=0.5, wait_max=5, wait_jitter=1.0):
                with attempt:
                    # Try to set the lock with expiration
                    result = await self.db_client.set(lock_key, "locked", ex=self.lock_timeout, nx=True)
                    return result is not None
        except Exception as e:
            logger.error(f"Error acquiring lock for file {file_name}: {str(e)}")
            return False
    
    async def release_lock(self, integration_id: str, file_name: str) -> bool:
        """
        Release a lock for a file.
        
        Args:
            integration_id: Integration ID
            file_name: Name of the file to unlock
            
        Returns:
            True if lock was released successfully
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        
        try:
            for attempt in stamina.retry_context(on=redis.RedisError, attempts=3, wait_initial=0.5, wait_max=5, wait_jitter=1.0):
                with attempt:
                    result = await self.db_client.delete(lock_key)
                    return result > 0
        except Exception as e:
            logger.error(f"Error releasing lock for file {file_name}: {str(e)}")
            return False
    
    async def is_locked(self, integration_id: str, file_name: str) -> bool:
        """
        Check if a file is currently locked for processing.
        
        Args:
            integration_id: Integration ID
            file_name: Name of the file to check
            
        Returns:
            True if file is locked, False otherwise
        """
        lock_key = f"file_processing_lock.{integration_id}.{file_name}"
        
        try:
            for attempt in stamina.retry_context(on=redis.RedisError, attempts=3, wait_initial=0.5, wait_max=5, wait_jitter=1.0):
                with attempt:
                    result = await self.db_client.exists(lock_key)
                    return result > 0
        except Exception as e:
            logger.error(f"Error checking lock for file {file_name}: {str(e)}")
            return False
