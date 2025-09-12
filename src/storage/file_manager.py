# 文件路径: MoonSQL/src/storage/file_manager.py

"""
FileManager - 表文件管理器

【功能说明】
- 管理.tbl表文件，每个表一个文件
- 文件由多个4KB页面组成
- 支持页面的分配、读取、写入
- 维护文件头信息(表元数据、页面数量等)

【文件格式】
文件头(4KB页面0) + 数据页面1 + 数据页面2 + ...

文件头格式:
- magic: 4B ('MTBL')
- version: 4B
- table_name: 64B
- page_count: 4B (总页面数，包括文件头)
- next_page_id: 4B (下一个可分配的页面ID)
- reserved: 剩余字节预留
"""

import os
import struct
from pathlib import Path
from typing import Optional, List
from page import SlottedPage, PAGE_SIZE

# 文件格式常量
FILE_MAGIC = b'MTBL'  # MoonSQL Table File
FILE_VERSION = 1
TABLE_NAME_SIZE = 64
FILE_HEADER_SIZE = 4096  # 文件头占用一个完整页面


class FileHeader:
    """表文件头信息"""

    def __init__(self, table_name: str, page_count: int = 1, next_page_id: int = 1):
        if len(table_name.encode('utf-8')) > TABLE_NAME_SIZE - 1:
            raise ValueError(f"表名过长，最大{TABLE_NAME_SIZE-1}字节")

        self.magic = FILE_MAGIC
        self.version = FILE_VERSION
        self.table_name = table_name
        self.page_count = page_count  # 包括文件头页面
        self.next_page_id = next_page_id  # 下一个可分配的页面ID

    def to_bytes(self) -> bytes:
        """序列化文件头为4KB数据"""
        # 编码表名为固定长度字节
        table_name_bytes = self.table_name.encode('utf-8')
        table_name_padded = table_name_bytes.ljust(TABLE_NAME_SIZE, b'\x00')

        # 打包核心字段
        header_data = struct.pack('<4sI64sII',
                                 self.magic,           # 4B: 文件魔数
                                 self.version,         # 4B: 版本号
                                 table_name_padded,    # 64B: 表名
                                 self.page_count,      # 4B: 页面总数
                                 self.next_page_id)    # 4B: 下一页面ID

        # 填充到4KB
        padding_size = FILE_HEADER_SIZE - len(header_data)
        return header_data + b'\x00' * padding_size

    @classmethod
    def from_bytes(cls, data: bytes) -> 'FileHeader':
        """从字节数据反序列化文件头"""
        if len(data) < 80:  # 最小头部大小
            raise ValueError("文件头数据不足")

        magic, version, table_name_bytes, page_count, next_page_id = \
            struct.unpack('<4sI64sII', data[:80])

        # 验证魔数
        if magic != FILE_MAGIC:
            raise ValueError(f"无效的文件魔数: {magic}")

        # 解码表名
        table_name = table_name_bytes.rstrip(b'\x00').decode('utf-8')

        return cls(table_name, page_count, next_page_id)


class FileManager:
    """表文件管理器"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self._open_files = {}  # table_name -> file_handle

    def _get_table_path(self, table_name: str) -> Path:
        """获取表文件路径"""
        return self.data_dir / f"{table_name}.tbl"

    def table_exists(self, table_name: str) -> bool:
        """检查表文件是否存在"""
        return self._get_table_path(table_name).exists()

    def create_table_file(self, table_name: str) -> None:
        """
        创建新的表文件

        Args:
            table_name: 表名

        Raises:
            FileExistsError: 表文件已存在
        """
        table_path = self._get_table_path(table_name)

        if table_path.exists():
            raise FileExistsError(f"表文件已存在: {table_name}")

        # 创建文件头
        header = FileHeader(table_name)
        header_bytes = header.to_bytes()

        # 写入文件
        with open(table_path, 'wb') as f:
            f.write(header_bytes)

        print(f"创建表文件: {table_path}")

    def delete_table_file(self, table_name: str) -> bool:
        """
        删除表文件

        Args:
            table_name: 表名

        Returns:
            是否删除成功
        """
        table_path = self._get_table_path(table_name)

        if not table_path.exists():
            return False

        # 关闭打开的文件句柄
        if table_name in self._open_files:
            self._open_files[table_name].close()
            del self._open_files[table_name]

        table_path.unlink()
        print(f"删除表文件: {table_path}")
        return True

    def get_file_header(self, table_name: str) -> FileHeader:
        """读取表文件头信息"""
        table_path = self._get_table_path(table_name)

        if not table_path.exists():
            raise FileNotFoundError(f"表文件不存在: {table_name}")

        with open(table_path, 'rb') as f:
            header_data = f.read(FILE_HEADER_SIZE)
            return FileHeader.from_bytes(header_data)

    def _get_file_handle(self, table_name: str, mode: str = 'r+b'):
        """获取文件句柄(支持复用)"""
        if table_name not in self._open_files:
            table_path = self._get_table_path(table_name)
            if not table_path.exists():
                raise FileNotFoundError(f"表文件不存在: {table_name}")
            self._open_files[table_name] = open(table_path, mode)

        return self._open_files[table_name]

    def read_page(self, table_name: str, page_id: int) -> SlottedPage:
        """
        读取指定页面

        Args:
            table_name: 表名
            page_id: 页面ID (0=文件头，1+为数据页)

        Returns:
            SlottedPage对象
        """
        if page_id == 0:
            raise ValueError("页面ID 0为文件头，不能作为数据页读取")

        file_handle = self._get_file_handle(table_name)

        # 定位到指定页面
        page_offset = page_id * PAGE_SIZE
        file_handle.seek(page_offset)

        # 读取页面数据
        page_data = file_handle.read(PAGE_SIZE)
        if len(page_data) != PAGE_SIZE:
            raise ValueError(f"页面{page_id}数据不完整: {len(page_data)}字节")

        return SlottedPage.from_bytes(page_id, page_data)

    def write_page(self, table_name: str, page: SlottedPage) -> None:
        """
        写入页面数据

        Args:
            table_name: 表名
            page: SlottedPage对象
        """
        page_id = page.page_id
        if page_id == 0:
            raise ValueError("页面ID 0为文件头，不能作为数据页写入")

        file_handle = self._get_file_handle(table_name)

        # 定位并写入
        page_offset = page_id * PAGE_SIZE
        file_handle.seek(page_offset)
        file_handle.write(page.to_bytes())
        file_handle.flush()  # 强制刷盘

    def allocate_new_page(self, table_name: str) -> int:
        """
        分配新的数据页面

        Args:
            table_name: 表名

        Returns:
            新分配的页面ID
        """
        # 读取当前文件头
        header = self.get_file_header(table_name)

        # 分配新页面ID
        new_page_id = header.next_page_id
        header.next_page_id += 1
        header.page_count += 1

        # 更新文件头
        file_handle = self._get_file_handle(table_name)
        file_handle.seek(0)
        file_handle.write(header.to_bytes())
        file_handle.flush()

        # 创建空页面并写入
        new_page = SlottedPage(new_page_id)
        self.write_page(table_name, new_page)

        print(f"表{table_name}分配新页面: {new_page_id}")
        return new_page_id

    def get_all_page_ids(self, table_name: str) -> List[int]:
        """获取表的所有数据页面ID"""
        header = self.get_file_header(table_name)
        # 返回所有数据页ID (跳过页面0文件头)
        return list(range(1, header.page_count))

    def get_table_stats(self, table_name: str) -> dict:
        """获取表文件统计信息"""
        header = self.get_file_header(table_name)
        table_path = self._get_table_path(table_name)

        # 计算文件大小
        file_size = table_path.stat().st_size if table_path.exists() else 0

        return {
            'table_name': header.table_name,
            'file_path': str(table_path),
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'total_pages': header.page_count,
            'data_pages': header.page_count - 1,  # 减去文件头页面
            'next_page_id': header.next_page_id
        }

    def close_table(self, table_name: str) -> None:
        """关闭表文件"""
        if table_name in self._open_files:
            self._open_files[table_name].close()
            del self._open_files[table_name]

    def close_all(self) -> None:
        """关闭所有打开的文件"""
        for file_handle in self._open_files.values():
            file_handle.close()
        self._open_files.clear()

    def __del__(self):
        """析构时关闭所有文件"""
        self.close_all()


# ==================== 测试代码 ====================

def test_file_manager():
    """测试文件管理器功能"""
    print("=== FileManager 功能测试 ===")

    # 创建文件管理器
    fm = FileManager("test_data")
    table_name = "test_table"

    # 清理可能存在的测试文件
    if fm.table_exists(table_name):
        fm.delete_table_file(table_name)

    print(f"1. 创建表文件: {table_name}")
    fm.create_table_file(table_name)

    print(f"2. 检查表是否存在: {fm.table_exists(table_name)}")

    print("3. 读取文件头信息:")
    header = fm.get_file_header(table_name)
    print(f"   表名: {header.table_name}")
    print(f"   页面数: {header.page_count}")
    print(f"   下一页ID: {header.next_page_id}")

    print("4. 分配新页面:")
    page_id1 = fm.allocate_new_page(table_name)
    page_id2 = fm.allocate_new_page(table_name)
    print(f"   分配的页面ID: {page_id1}, {page_id2}")

    print("5. 写入测试数据到页面:")
    page1 = fm.read_page(table_name, page_id1)
    page1.insert(b"Record 1 in page 1")
    page1.insert(b"Record 2 in page 1")
    fm.write_page(table_name, page1)

    page2 = fm.read_page(table_name, page_id2)
    page2.insert(b"Record 1 in page 2")
    fm.write_page(table_name, page2)

    print("6. 读取验证:")
    read_page1 = fm.read_page(table_name, page_id1)
    read_page2 = fm.read_page(table_name, page_id2)

    print(f"   页面{page_id1}记录: {len(read_page1.get_all_records())}条")
    print(f"   页面{page_id2}记录: {len(read_page2.get_all_records())}条")

    for slot_id, record in read_page1.get_all_records():
        print(f"     页面{page_id1}槽{slot_id}: {record.decode()}")

    for slot_id, record in read_page2.get_all_records():
        print(f"     页面{page_id2}槽{slot_id}: {record.decode()}")

    print("7. 表统计信息:")
    stats = fm.get_table_stats(table_name)
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("8. 获取所有页面ID:")
    page_ids = fm.get_all_page_ids(table_name)
    print(f"   数据页面ID: {page_ids}")

    # 清理测试文件
    print("9. 清理测试文件")
    fm.delete_table_file(table_name)

    print("FileManager测试完成!")


if __name__ == "__main__":
    test_file_manager()