import asyncio
from enum import Enum
from functools import lru_cache


class LockType(Enum):
    """锁类型枚举"""
    MEMORY_DATA = "memory_data"
    GROUP_CONFIG = "group_config"


class DatabaseLock:
    """改进的数据库锁管理器，解决死锁问题"""

    def __init__(self):
        self._locks: dict[str, asyncio.Lock] = {}
        self._lock_order: dict[str, int] = {}
        self._lock_hierarchy = {
            LockType.MEMORY_DATA.value: 1,
            LockType.GROUP_CONFIG.value: 2,
        }

    def _get_lock_key(self, *args, **kwargs) -> str:
        """生成锁键值"""
        # 基于参数生成唯一的锁键
        if "lock_type" in kwargs:
            lock_type = kwargs["lock_type"]
            ins_id = args[0] if args else 0
            return f"{lock_type.value}:{ins_id}"
        return str(args) + str(kwargs)

    def _check_lock_hierarchy(self, new_lock_key: str, existing_lock_keys: set) -> bool:
        """检查锁层次结构，防止死锁"""
        if new_lock_key not in self._lock_hierarchy:
            return True

        new_level = self._lock_hierarchy[new_lock_key]
        for existing_key in existing_lock_keys:
            if existing_key in self._lock_hierarchy:
                existing_level = self._lock_hierarchy[existing_key]
                # 防止低级别锁等待高级别锁
                if new_level <= existing_level:
                    return False
        return True

    @lru_cache(maxsize=2048)
    def get_lock(self, *args, **kwargs) -> asyncio.Lock:
        """获取锁实例"""
        return asyncio.Lock()


# 全局锁管理器实例
database_lock_manager = DatabaseLock()


@lru_cache(maxsize=1024)
def get_group_lock(_: int) -> asyncio.Lock:
    """获取群组锁"""
    return asyncio.Lock()


@lru_cache(maxsize=1024)
def get_private_lock(_: int) -> asyncio.Lock:
    """获取私聊锁"""
    return asyncio.Lock()


def database_lock_with_type(lock_type: LockType, ins_id: int) -> asyncio.Lock:
    """带类型的数据库锁"""
    key = f"{lock_type.value}:{ins_id}"
    if key not in database_lock_manager._locks:
        database_lock_manager._locks[key] = asyncio.Lock()
    return database_lock_manager._locks[key]


def database_lock(*args, **kwargs) -> asyncio.Lock:
    """改进的数据库锁函数"""
    # 默认行为保持向后兼容
    return database_lock_manager.get_lock(*args, **kwargs)


class DeadlockRetryManager:
    """死锁重试管理器"""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 0.1):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    async def execute_with_retry(self, operation, *args, **kwargs):
        """执行操作并处理死锁重试"""
        def is_deadlock_error(error):
            error_msg = str(error).lower()
            return any(
                keyword in error_msg
                for keyword in ["deadlock", "lock wait timeout", "dead lock"]
            )

        attempt = 0
        while attempt <= self.max_retries:
            try:
                result = await operation(*args, **kwargs)
                return result
            except Exception as e:  # noqa: PERF203
                if not is_deadlock_error(e):
                    raise
                if attempt >= self.max_retries:
                    raise
                wait_time = self.backoff_factor * (2 ** attempt)
                await asyncio.sleep(wait_time)
                attempt += 1


# 全局重试管理器
deadlock_retry_manager = DeadlockRetryManager()
