# 文件路径: MoonSQL/src/storage/buffer.py

"""
BufferPool - 页面缓冲池

【功能说明】
- 在FileManager之上提供页面缓存层
- 支持LRU(最近最少使用)和FIFO(先进先出)替换策略
- 跟踪缓存命中率和页面淘汰日志
- 管理脏页(修改过的页面)的刷盘

【设计原理】
- 缓存热点页面在内存中，减少磁盘I/O
- 使用OrderedDict实现LRU，普通dict+队列实现FIFO
- 脏页延迟写入，提高性能
- 详细统计信息便于性能分析

【缓存键】
缓存键格式: (table_name, page_id)
例如: ("users", 1) 表示users表的第1页
"""

import time
from collections import OrderedDict, deque
from typing import Dict, Tuple, Set, List, Optional, Any
from file_manager import FileManager
from page import SlottedPage


class EvictionEvent:
    """页面淘汰事件记录"""

    def __init__(self, table_name: str, page_id: int, reason: str, was_dirty: bool):
        self.timestamp = time.time()
        self.table_name = table_name
        self.page_id = page_id
        self.reason = reason  # "capacity_full", "manual_flush", "shutdown"
        self.was_dirty = was_dirty
        self.formatted_time = time.strftime("%H:%M:%S", time.localtime(self.timestamp))

    def __repr__(self):
        dirty_mark = "*" if self.was_dirty else ""
        return f"[{self.formatted_time}] 淘汰页面 {self.table_name}.{self.page_id}{dirty_mark} (原因: {self.reason})"


class BufferPool:
    """页面缓冲池"""

    def __init__(self, file_manager: FileManager, capacity: int = 64, policy: str = "LRU"):
        """
        初始化缓冲池

        Args:
            file_manager: 文件管理器
            capacity: 缓存页面数量
            policy: 替换策略 "LRU" 或 "FIFO"
        """
        if capacity <= 0:
            raise ValueError("缓存容量必须大于0")

        if policy not in ["LRU", "FIFO"]:
            raise ValueError("替换策略必须是 'LRU' 或 'FIFO'")

        self.file_manager = file_manager
        self.capacity = capacity
        self.policy = policy

        # 缓存存储
        if policy == "LRU":
            self.cache = OrderedDict()  # (table, page_id) -> SlottedPage
        else:  # FIFO
            self.cache = {}  # (table, page_id) -> SlottedPage
            self.fifo_queue = deque()  # 记录插入顺序

        # 脏页跟踪
        self.dirty_pages: Set[Tuple[str, int]] = set()

        # 统计信息
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.eviction_log: List[EvictionEvent] = []

        print(f"BufferPool初始化: 容量={capacity}页, 策略={policy}")

    def _make_cache_key(self, table_name: str, page_id: int) -> Tuple[str, int]:
        """生成缓存键"""
        return (table_name, page_id)

    def get_page(self, table_name: str, page_id: int) -> SlottedPage:
        """
        获取页面(优先从缓存)

        Args:
            table_name: 表名
            page_id: 页面ID

        Returns:
            SlottedPage对象
        """
        cache_key = self._make_cache_key(table_name, page_id)

        # 检查缓存
        if cache_key in self.cache:
            self.hits += 1

            # LRU: 移动到末尾表示最近使用
            if self.policy == "LRU":
                self.cache.move_to_end(cache_key)

            return self.cache[cache_key]

        # 缓存未命中，从磁盘加载
        self.misses += 1
        page = self.file_manager.read_page(table_name, page_id)

        # 加入缓存
        self._add_to_cache(cache_key, page)

        return page

    def put_page(self, table_name: str, page: SlottedPage, mark_dirty: bool = True) -> None:
        """
        更新页面到缓存

        Args:
            table_name: 表名
            page: 页面对象
            mark_dirty: 是否标记为脏页
        """
        cache_key = self._make_cache_key(table_name, page.page_id)

        # 更新缓存
        self._add_to_cache(cache_key, page)

        # 标记脏页
        if mark_dirty:
            self.dirty_pages.add(cache_key)

    def _add_to_cache(self, cache_key: Tuple[str, int], page: SlottedPage) -> None:
        """添加页面到缓存，必要时进行淘汰"""
        # 检查是否需要淘汰
        if len(self.cache) >= self.capacity and cache_key not in self.cache:
            self._evict_page()

        # 添加到缓存
        if self.policy == "LRU":
            self.cache[cache_key] = page
            self.cache.move_to_end(cache_key)  # 标记为最近使用
        else:  # FIFO
            if cache_key not in self.cache:
                self.fifo_queue.append(cache_key)
            self.cache[cache_key] = page

    def _evict_page(self) -> None:
        """根据策略淘汰一个页面"""
        if not self.cache:
            return

        # 选择淘汰的页面
        if self.policy == "LRU":
            # OrderedDict: 第一个是最久未使用的
            evict_key, evict_page = self.cache.popitem(last=False)
        else:  # FIFO
            # 从队列头部取出最早进入的页面
            evict_key = self.fifo_queue.popleft()
            evict_page = self.cache.pop(evict_key)

        table_name, page_id = evict_key
        was_dirty = evict_key in self.dirty_pages

        # 如果是脏页，写回磁盘
        if was_dirty:
            self.file_manager.write_page(table_name, evict_page)
            self.dirty_pages.remove(evict_key)

        # 记录淘汰事件
        eviction_event = EvictionEvent(table_name, page_id, "capacity_full", was_dirty)
        self.eviction_log.append(eviction_event)
        self.evictions += 1

        print(f"淘汰页面: {table_name}.{page_id} ({'脏页' if was_dirty else '干净页'})")

    def flush_dirty_pages(self, table_name: str = None) -> int:
        """
        刷新脏页到磁盘

        Args:
            table_name: 指定表名，None表示刷新所有表

        Returns:
            刷新的页面数量
        """
        flushed_count = 0
        dirty_to_flush = []

        # 找出需要刷新的脏页
        for cache_key in list(self.dirty_pages):
            key_table, key_page_id = cache_key
            if table_name is None or key_table == table_name:
                dirty_to_flush.append(cache_key)

        # 执行刷新
        for cache_key in dirty_to_flush:
            key_table, key_page_id = cache_key
            if cache_key in self.cache:
                page = self.cache[cache_key]
                self.file_manager.write_page(key_table, page)
                self.dirty_pages.remove(cache_key)
                flushed_count += 1

                # 记录刷新事件
                event = EvictionEvent(key_table, key_page_id, "manual_flush", True)
                self.eviction_log.append(event)

        if flushed_count > 0:
            print(f"刷新脏页: {flushed_count}页 {'(表: ' + table_name + ')' if table_name else '(所有表)'}")

        return flushed_count

    def evict_table_pages(self, table_name: str) -> int:
        """
        淘汰指定表的所有页面

        Args:
            table_name: 表名

        Returns:
            淘汰的页面数量
        """
        evicted_count = 0
        pages_to_evict = []

        # 找出该表的所有页面
        for cache_key in list(self.cache.keys()):
            key_table, key_page_id = cache_key
            if key_table == table_name:
                pages_to_evict.append(cache_key)

        # 执行淘汰
        for cache_key in pages_to_evict:
            key_table, key_page_id = cache_key
            page = self.cache.pop(cache_key)
            was_dirty = cache_key in self.dirty_pages

            # 如果是脏页，写回磁盘
            if was_dirty:
                self.file_manager.write_page(key_table, page)
                self.dirty_pages.remove(cache_key)

            # 从FIFO队列中移除
            if self.policy == "FIFO" and cache_key in self.fifo_queue:
                temp_queue = deque()
                while self.fifo_queue:
                    item = self.fifo_queue.popleft()
                    if item != cache_key:
                        temp_queue.append(item)
                self.fifo_queue = temp_queue

            # 记录淘汰事件
            event = EvictionEvent(key_table, key_page_id, "table_eviction", was_dirty)
            self.eviction_log.append(event)
            evicted_count += 1

        self.evictions += evicted_count

        if evicted_count > 0:
            print(f"淘汰表{table_name}的页面: {evicted_count}页")

        return evicted_count

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self.hits + self.misses
        hit_ratio = (self.hits / total_requests * 100) if total_requests > 0 else 0

        return {
            'policy': self.policy,
            'capacity': self.capacity,
            'cached_pages': len(self.cache),
            'dirty_pages': len(self.dirty_pages),
            'hits': self.hits,
            'misses': self.misses,
            'total_requests': total_requests,
            'hit_ratio_pct': round(hit_ratio, 2),
            'evictions': self.evictions,
            'eviction_events': len(self.eviction_log)
        }

    def get_eviction_log(self, limit: int = 20) -> List[EvictionEvent]:
        """获取最近的淘汰日志"""
        return self.eviction_log[-limit:] if limit else self.eviction_log

    def clear_cache(self) -> None:
        """清空缓存(先刷新脏页)"""
        dirty_count = self.flush_dirty_pages()

        evicted_count = len(self.cache)
        self.cache.clear()
        if self.policy == "FIFO":
            self.fifo_queue.clear()
        self.dirty_pages.clear()

        self.evictions += evicted_count

        print(f"清空缓存: 刷新{dirty_count}脏页, 淘汰{evicted_count}页")

    def close(self) -> None:
        """关闭缓冲池(刷新所有脏页)"""
        dirty_count = len(self.dirty_pages)
        if dirty_count > 0:
            print(f"关闭缓冲池: 刷新{dirty_count}脏页")
            self.flush_dirty_pages()
        self.clear_cache()


# ==================== 测试代码 ====================

def test_buffer_pool_basic():
    """测试缓冲池基本功能"""
    print("=== BufferPool 基本功能测试 ===")

    try:
        from .file_manager import FileManager
    except ImportError:
        from file_manager import FileManager

    # 创建小容量缓冲池便于测试
    fm = FileManager("test_buffer_data")
    bp = BufferPool(fm, capacity=3, policy="LRU")

    # 创建测试表
    table_name = "test_buffer"
    if fm.table_exists(table_name):
        fm.delete_table_file(table_name)

    fm.create_table_file(table_name)

    print("\n1. 基本读取测试(产生缓存未命中):")

    # 分配页面并访问
    page_ids = []
    for i in range(4):  # 超过缓存容量
        page_id = fm.allocate_new_page(table_name)
        page_ids.append(page_id)

        # 通过缓冲池读取页面
        page = bp.get_page(table_name, page_id)
        page.insert(f"Data for page {page_id}".encode())

        # 写回缓存(标记脏页)
        bp.put_page(table_name, page, mark_dirty=True)

        stats = bp.get_stats()
        print(f"   访问页面{page_id}: 命中率={stats['hit_ratio_pct']}%, "
              f"缓存页数={stats['cached_pages']}, 脏页数={stats['dirty_pages']}")

    print(f"\n最终统计: {bp.get_stats()}")

    print("\n2. 缓存命中测试:")
    # 重复访问已缓存的页面
    for page_id in page_ids[-2:]:  # 访问最后2个页面(应该在缓存中)
        page = bp.get_page(table_name, page_id)
        stats = bp.get_stats()
        print(f"   重新访问页面{page_id}: 命中率={stats['hit_ratio_pct']}%")

    print("\n3. 淘汰日志:")
    for event in bp.get_eviction_log():
        print(f"   {event}")

    # 清理
    bp.close()
    fm.delete_table_file(table_name)

    return bp.get_stats()


def test_replacement_policies():
    """测试不同替换策略"""
    print("\n=== LRU vs FIFO 替换策略对比 ===")

    try:
        from .file_manager import FileManager
    except ImportError:
        from file_manager import FileManager

    def test_policy(policy_name):
        print(f"\n--- 测试 {policy_name} 策略 ---")

        fm = FileManager("test_policy_data")
        bp = BufferPool(fm, capacity=3, policy=policy_name)

        table_name = f"test_{policy_name.lower()}"
        if fm.table_exists(table_name):
            fm.delete_table_file(table_name)

        fm.create_table_file(table_name)

        # 分配5个页面(超过缓存容量)
        page_ids = []
        for i in range(5):
            page_id = fm.allocate_new_page(table_name)
            page_ids.append(page_id)

        # 按顺序访问页面1,2,3
        print("访问序列: 1,2,3")
        for page_id in page_ids[:3]:
            bp.get_page(table_name, page_id)

        # 再次访问页面2(LRU中会刷新其位置)
        print("再次访问页面2")
        bp.get_page(table_name, page_ids[1])

        # 访问页面4(会触发淘汰)
        print("访问页面4(触发淘汰)")
        bp.get_page(table_name, page_ids[3])

        # 访问页面5(再次触发淘汰)
        print("访问页面5(再次触发淘汰)")
        bp.get_page(table_name, page_ids[4])

        print("淘汰历史:")
        for event in bp.get_eviction_log():
            print(f"   {event}")

        stats = bp.get_stats()
        bp.close()
        fm.delete_table_file(table_name)

        return stats

    # 对比两种策略
    lru_stats = test_policy("LRU")
    fifo_stats = test_policy("FIFO")

    print(f"\n--- 策略对比结果 ---")
    print(f"LRU  - 命中率: {lru_stats['hit_ratio_pct']}%, 淘汰次数: {lru_stats['evictions']}")
    print(f"FIFO - 命中率: {fifo_stats['hit_ratio_pct']}%, 淘汰次数: {fifo_stats['evictions']}")


def test_dirty_page_management():
    """测试脏页管理"""
    print("\n=== 脏页管理测试 ===")

    try:
        from .file_manager import FileManager
    except ImportError:
        from file_manager import FileManager

    fm = FileManager("test_dirty_data")
    bp = BufferPool(fm, capacity=4, policy="LRU")

    table_name = "test_dirty"
    if fm.table_exists(table_name):
        fm.delete_table_file(table_name)

    fm.create_table_file(table_name)

    print("1. 创建并修改页面:")
    page_ids = []
    for i in range(3):
        page_id = fm.allocate_new_page(table_name)
        page_ids.append(page_id)

        # 读取页面
        page = bp.get_page(table_name, page_id)

        # 修改页面内容
        page.insert(f"Modified data {i}".encode())

        # 写回缓存(标记脏页)
        bp.put_page(table_name, page, mark_dirty=True)

        stats = bp.get_stats()
        print(f"   修改页面{page_id}: 脏页数={stats['dirty_pages']}")

    print(f"\n2. 手动刷新脏页:")
    flushed = bp.flush_dirty_pages(table_name)
    print(f"   刷新了{flushed}个脏页")

    stats = bp.get_stats()
    print(f"   刷新后脏页数: {stats['dirty_pages']}")

    print("\n3. 淘汰事件日志:")
    for event in bp.get_eviction_log():
        print(f"   {event}")

    # 清理
    bp.close()
    fm.delete_table_file(table_name)


def run_all_buffer_tests():
    """运行所有缓冲池测试"""
    print("BufferPool 全功能测试")
    print("=" * 60)

    test_buffer_pool_basic()
    test_replacement_policies()
    test_dirty_page_management()

    print("\n" + "=" * 60)
    print("BufferPool 测试完成!")


if __name__ == "__main__":
    run_all_buffer_tests()