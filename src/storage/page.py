# 文件路径: MoonSQL/src/storage/page.py

"""
SlottedPage - 4KB槽式页实现

【设计说明】
- 固定4KB页面大小
- 页头(12B) + 槽目录(5B*N) + 空闲空间 + 数据区(变长记录)
- 数据从页尾向前增长，槽目录从页头向后增长
- 逻辑删除：标记tomb=1，不移动数据

【页面布局】
 0    12        12+5*N              4096-data_size    4096
[页头][槽目录...][   空闲空间   ][...数据记录从后往前...]
     ^                          ^
  slot_start               data_start
"""

import struct
from typing import List, Tuple, Optional

# 常量定义
PAGE_SIZE = 4096  # 4KB标准页面大小
HEADER_SIZE = 14  # 页头固定14字节 (struct '<HIHHI' 需要14字节)
SLOT_SIZE = 5  # 每个槽5字节
MAGIC_NUMBER = 0x4D53  # 'MS'的ASCII码


class SlottedPage:
    """4KB槽式页实现"""

    def __init__(self, page_id: int, raw_data: bytes = None):
        """
        初始化页面

        Args:
            page_id: 页面唯一ID
            raw_data: 原始字节数据(None=新建空页)
        """
        self.page_id = page_id
        self.data = bytearray(PAGE_SIZE)

        if raw_data is None:
            self._init_empty_page()
        else:
            self._load_from_bytes(raw_data)

    def _init_empty_page(self):
        """初始化空页面"""
        # 页头格式: magic(2) + page_id(4) + data_start(2) + slot_count(2) + flags(2)
        header = struct.pack('<HIHHI',
                             MAGIC_NUMBER,  # magic
                             self.page_id,  # page_id
                             PAGE_SIZE,  # data_start: 数据区起始位置(空页时在最后)
                             0,  # slot_count: 槽数量
                             0)  # flags: 预留标志位
        self.data[:HEADER_SIZE] = header

    def _load_from_bytes(self, raw_data: bytes):
        """从字节数据加载页面"""
        if len(raw_data) != PAGE_SIZE:
            raise ValueError(f"页面数据必须是{PAGE_SIZE}字节，实际{len(raw_data)}字节")

        self.data[:] = raw_data

        # 验证魔数
        magic = struct.unpack('<H', self.data[:2])[0]
        if magic != MAGIC_NUMBER:
            raise ValueError(f"无效的页面魔数: 0x{magic:04X}")

        # 验证页面ID
        stored_page_id = struct.unpack('<I', self.data[2:6])[0]
        if stored_page_id != self.page_id:
            raise ValueError(f"页面ID不匹配: 存储{stored_page_id} vs 期望{self.page_id}")

    def _get_header_info(self) -> Tuple[int, int, int]:
        """获取页头信息: (data_start, slot_count, flags)"""
        _, _, data_start, slot_count, flags = struct.unpack('<HIHHI', self.data[:HEADER_SIZE])
        return data_start, slot_count, flags

    def _set_header_info(self, data_start: int, slot_count: int, flags: int = 0):
        """设置页头信息"""
        header = struct.pack('<HIHHI', MAGIC_NUMBER, self.page_id, data_start, slot_count, flags)
        self.data[:HEADER_SIZE] = header

    def _get_slot_info(self, slot_id: int) -> Tuple[int, int, bool]:
        """获取槽信息: (offset, length, is_deleted)"""
        data_start, slot_count, _ = self._get_header_info()

        if slot_id < 0 or slot_id >= slot_count:
            raise IndexError(f"槽ID {slot_id} 超出范围 [0, {slot_count})")

        slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE
        offset, length, tomb = struct.unpack('<HHB', self.data[slot_offset:slot_offset + SLOT_SIZE])
        return offset, length, bool(tomb)

    def _set_slot_info(self, slot_id: int, offset: int, length: int, is_deleted: bool = False):
        """设置槽信息"""
        slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE
        slot_data = struct.pack('<HHB', offset, length, int(is_deleted))
        self.data[slot_offset:slot_offset + SLOT_SIZE] = slot_data

    def get_free_space(self) -> int:
        """计算剩余可用空间"""
        data_start, slot_count, _ = self._get_header_info()
        slots_end = HEADER_SIZE + slot_count * SLOT_SIZE
        return data_start - slots_end

    def insert(self, record: bytes) -> int:
        """
        插入记录

        Args:
            record: 要插入的记录数据

        Returns:
            slot_id: 成功返回槽ID，失败返回-1
        """
        if len(record) == 0:
            raise ValueError("记录不能为空")

        record_len = len(record)
        data_start, slot_count, flags = self._get_header_info()

        # 检查空间：需要记录空间 + 新槽空间
        if self.get_free_space() < record_len + SLOT_SIZE:
            return -1  # 页面空间不足

        # 在数据区写入记录(从后往前)
        new_data_start = data_start - record_len
        self.data[new_data_start:new_data_start + record_len] = record

        # 添加新槽
        new_slot_id = slot_count
        self._set_slot_info(new_slot_id, new_data_start, record_len, False)

        # 更新页头
        self._set_header_info(new_data_start, slot_count + 1, flags)

        return new_slot_id

    def read(self, slot_id: int) -> bytes:
        """
        读取记录

        Args:
            slot_id: 槽ID

        Returns:
            记录数据

        Raises:
            IndexError: 槽ID无效
            ValueError: 记录已删除
        """
        offset, length, is_deleted = self._get_slot_info(slot_id)

        if is_deleted:
            raise ValueError(f"记录已删除，槽ID: {slot_id}")

        return bytes(self.data[offset:offset + length])

    def delete(self, slot_id: int):
        """
        逻辑删除记录

        Args:
            slot_id: 要删除的槽ID
        """
        offset, length, is_deleted = self._get_slot_info(slot_id)

        if not is_deleted:
            self._set_slot_info(slot_id, offset, length, True)

    def is_deleted(self, slot_id: int) -> bool:
        """检查记录是否已删除"""
        try:
            _, _, is_deleted = self._get_slot_info(slot_id)
            return is_deleted
        except IndexError:
            return True

    def get_slot_count(self) -> int:
        """获取总槽数(包括已删除的)"""
        _, slot_count, _ = self._get_header_info()
        return slot_count

    def get_active_slots(self) -> List[int]:
        """获取所有活跃(未删除)的槽ID"""
        active_slots = []
        for slot_id in range(self.get_slot_count()):
            if not self.is_deleted(slot_id):
                active_slots.append(slot_id)
        return active_slots

    def get_all_records(self) -> List[Tuple[int, bytes]]:
        """获取所有活跃记录: [(slot_id, data), ...]"""
        records = []
        for slot_id in self.get_active_slots():
            try:
                data = self.read(slot_id)
                records.append((slot_id, data))
            except ValueError:
                continue  # 跳过已删除记录
        return records

    def to_bytes(self) -> bytes:
        """序列化为字节数组"""
        return bytes(self.data)

    @classmethod
    def from_bytes(cls, page_id: int, data: bytes) -> 'SlottedPage':
        """从字节数组反序列化"""
        return cls(page_id, data)

    def get_stats(self) -> dict:
        """获取页面统计信息"""
        data_start, slot_count, _ = self._get_header_info()
        active_slots = len(self.get_active_slots())
        deleted_slots = slot_count - active_slots

        # 计算数据占用空间
        data_size = PAGE_SIZE - data_start
        free_space = self.get_free_space()

        return {
            'page_id': self.page_id,
            'total_slots': slot_count,
            'active_slots': active_slots,
            'deleted_slots': deleted_slots,
            'data_size': data_size,
            'free_space': free_space,
            'utilization_pct': round((data_size / PAGE_SIZE) * 100, 2)
        }

    def __repr__(self):
        stats = self.get_stats()
        return (f"SlottedPage(id={self.page_id}, "
                f"slots={stats['active_slots']}/{stats['total_slots']}, "
                f"free={stats['free_space']}B, "
                f"util={stats['utilization_pct']}%)")


# ==================== 测试代码 ====================

def test_basic_operations():
    """测试基本操作"""
    print("=== 测试1: 基本插入/读取/删除 ===")

    # 创建新页面
    page = SlottedPage(page_id=100)
    print(f"新建页面: {page}")

    # 插入测试记录
    test_records = [
        b"Alice",
        b"Bob Smith",
        b"Charlie Brown has a long name",
        b"Diana"
    ]

    slot_ids = []
    for i, record in enumerate(test_records):
        slot_id = page.insert(record)
        slot_ids.append(slot_id)
        print(f"插入记录{i + 1}: '{record.decode()}' -> 槽{slot_id}")

    print(f"插入完成: {page}")

    # 读取验证
    print("\n读取验证:")
    for slot_id in slot_ids:
        data = page.read(slot_id)
        print(f"槽{slot_id}: '{data.decode()}'")

    # 删除测试
    delete_slot = slot_ids[1]
    print(f"\n删除槽{delete_slot}")
    page.delete(delete_slot)

    try:
        page.read(delete_slot)
        print("错误: 应该无法读取已删除记录")
    except ValueError as e:
        print(f"正确: {e}")

    print(f"删除后: {page}")
    return page


def test_serialization():
    """测试序列化"""
    print("\n=== 测试2: 序列化/反序列化 ===")

    # 创建页面并添加数据
    page1 = SlottedPage(200)
    records = [b"Test1", b"Test2", b"Long test record with more data"]

    for record in records:
        slot_id = page1.insert(record)
        print(f"原页面插入: '{record.decode()}' -> 槽{slot_id}")

    # 序列化
    page_bytes = page1.to_bytes()
    print(f"序列化完成: {len(page_bytes)} 字节")

    # 反序列化
    page2 = SlottedPage.from_bytes(200, page_bytes)
    print(f"反序列化完成: {page2}")

    # 验证数据一致性
    records1 = page1.get_all_records()
    records2 = page2.get_all_records()

    print(f"原页面记录: {len(records1)} 条")
    print(f"恢复页面记录: {len(records2)} 条")

    data_match = all(r1[1] == r2[1] for r1, r2 in zip(records1, records2))
    print(f"数据一致性: {'通过' if data_match else '失败'}")


def test_capacity_limits():
    """测试容量限制"""
    print("\n=== 测试3: 容量限制 ===")

    page = SlottedPage(300)
    record = b"x" * 50  # 50字节记录

    inserted_count = 0
    while True:
        slot_id = page.insert(record)
        if slot_id == -1:
            break
        inserted_count += 1
        if inserted_count % 20 == 0:
            print(f"已插入{inserted_count}条记录，剩余空间: {page.get_free_space()}字节")

    print(f"容量测试完成: 共插入{inserted_count}条记录")
    print(f"最终状态: {page}")
    print(f"详细统计: {page.get_stats()}")


def test_edge_cases():
    """测试边界情况"""
    print("\n=== 测试4: 边界情况 ===")

    page = SlottedPage(400)

    # 测试空记录
    try:
        page.insert(b"")
        print("错误: 应该拒绝空记录")
    except ValueError as e:
        print(f"正确: 拒绝空记录 - {e}")

    # 测试无效槽ID
    try:
        page.read(999)
        print("错误: 应该拒绝无效槽ID")
    except IndexError as e:
        print(f"正确: 拒绝无效槽ID - {e}")

    # 插入一条记录用于测试
    slot_id = page.insert(b"test record")

    # 测试重复删除
    page.delete(slot_id)
    page.delete(slot_id)  # 重复删除应该安全
    print("重复删除测试通过")

    # 测试页面ID验证
    try:
        page_bytes = page.to_bytes()
        SlottedPage.from_bytes(999, page_bytes)  # 错误的页面ID
        print("错误: 应该检测页面ID不匹配")
    except ValueError as e:
        print(f"正确: 检测到页面ID不匹配 - {e}")


def run_all_tests():
    """运行所有测试"""
    print("SlottedPage 全功能测试")
    print("=" * 50)

    test_basic_operations()
    test_serialization()
    test_capacity_limits()
    test_edge_cases()

    print("\n" + "=" * 50)
    print("所有测试完成!")


if __name__ == "__main__":
    run_all_tests()