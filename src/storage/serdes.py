# 文件路径: MoonSQL/src/storage/serdes.py

"""
SerDes - 记录序列化/反序列化

【功能说明】
- 将行数据(dict)编码为字节，存储到SlottedPage
- 从字节数据解码为行数据(dict)
- 支持NULL值处理
- 支持变长字段(VARCHAR)

【记录格式】
[NULL位图] [列偏移表] [变长数据区]

NULL位图: 每列1位，1表示NULL，0表示有值
列偏移表: 每列2字节，指向数据在记录中的位置
变长数据区: 实际的列数据

【支持的数据类型】
- INT: 4字节有符号整数
- VARCHAR(n): 变长字符串，最大n字节(UTF-8编码)
"""

import struct
import math
from typing import Dict, List, Any, Tuple, Optional


class ColumnType:
    """列类型定义"""
    INT = "INT"
    VARCHAR = "VARCHAR"


class ColumnDef:
    """列定义"""

    def __init__(self, name: str, col_type: str, max_length: int = None):
        self.name = name
        self.type = col_type
        self.max_length = max_length  # 仅VARCHAR使用

        if col_type == ColumnType.VARCHAR and max_length is None:
            raise ValueError("VARCHAR列必须指定max_length")

    def __repr__(self):
        if self.type == ColumnType.VARCHAR:
            return f"{self.name} {self.type}({self.max_length})"
        return f"{self.name} {self.type}"


class RecordEncoder:
    """记录编码器"""

    def __init__(self, columns: List[ColumnDef]):
        self.columns = columns
        self.column_count = len(columns)

        # 计算NULL位图大小(按字节对齐)
        self.null_bitmap_size = math.ceil(self.column_count / 8)

        # 列偏移表大小
        self.offset_table_size = self.column_count * 2  # 每列2字节偏移

        # 固定头部大小
        self.header_size = self.null_bitmap_size + self.offset_table_size

    def encode(self, row_data: Dict[str, Any]) -> bytes:
        """
        编码行数据为字节

        Args:
            row_data: 行数据字典 {列名: 值}

        Returns:
            编码后的字节数据
        """
        # 1. 准备NULL位图
        null_bitmap = bytearray(self.null_bitmap_size)

        # 2. 准备数据和偏移
        data_parts = []
        offsets = []
        current_offset = self.header_size

        for i, col in enumerate(self.columns):
            value = row_data.get(col.name)

            if value is None:
                # 设置NULL位
                byte_index = i // 8
                bit_index = i % 8
                null_bitmap[byte_index] |= (1 << bit_index)

                # NULL值的偏移设为0
                offsets.append(0)
            else:
                # 编码具体值
                encoded_value = self._encode_value(col, value)
                data_parts.append(encoded_value)

                # 记录偏移
                offsets.append(current_offset)
                current_offset += len(encoded_value)

        # 3. 组装最终字节数据
        result = bytearray()

        # NULL位图
        result.extend(null_bitmap)

        # 列偏移表
        for offset in offsets:
            result.extend(struct.pack('<H', offset))

        # 数据区
        for data_part in data_parts:
            result.extend(data_part)

        return bytes(result)

    def _encode_value(self, col: ColumnDef, value: Any) -> bytes:
        """编码单个值"""
        if col.type == ColumnType.INT:
            if not isinstance(value, int):
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    raise ValueError(f"列{col.name}期望INT类型，得到{type(value)}")
            return struct.pack('<i', value)

        elif col.type == ColumnType.VARCHAR:
            if not isinstance(value, str):
                value = str(value)

            # UTF-8编码
            encoded = value.encode('utf-8')

            # 检查长度限制
            if len(encoded) > col.max_length:
                raise ValueError(f"列{col.name}值过长: {len(encoded)} > {col.max_length}")

            # VARCHAR格式: 长度(2字节) + 数据
            return struct.pack('<H', len(encoded)) + encoded

        else:
            raise ValueError(f"不支持的列类型: {col.type}")


class RecordDecoder:
    """记录解码器"""

    def __init__(self, columns: List[ColumnDef]):
        self.columns = columns
        self.column_count = len(columns)
        self.null_bitmap_size = math.ceil(self.column_count / 8)
        self.offset_table_size = self.column_count * 2
        self.header_size = self.null_bitmap_size + self.offset_table_size

    def decode(self, record_bytes: bytes) -> Dict[str, Any]:
        """
        解码字节数据为行数据

        Args:
            record_bytes: 编码的字节数据

        Returns:
            行数据字典 {列名: 值}
        """
        if len(record_bytes) < self.header_size:
            raise ValueError("记录数据太短")

        # 1. 解析NULL位图
        null_bitmap = record_bytes[:self.null_bitmap_size]

        # 2. 解析列偏移表
        offsets = []
        offset_start = self.null_bitmap_size
        for i in range(self.column_count):
            offset_pos = offset_start + i * 2
            offset = struct.unpack('<H', record_bytes[offset_pos:offset_pos + 2])[0]
            offsets.append(offset)

        # 3. 解码各列数据
        row_data = {}

        for i, col in enumerate(self.columns):
            # 检查是否为NULL
            byte_index = i // 8
            bit_index = i % 8
            is_null = (null_bitmap[byte_index] & (1 << bit_index)) != 0

            if is_null:
                row_data[col.name] = None
            else:
                # 获取数据位置
                data_offset = offsets[i]
                if data_offset == 0:
                    raise ValueError(f"列{col.name}偏移为0但不是NULL")

                # 解码数据
                value = self._decode_value(col, record_bytes, data_offset)
                row_data[col.name] = value

        return row_data

    def _decode_value(self, col: ColumnDef, record_bytes: bytes, offset: int) -> Any:
        """解码单个值"""
        if col.type == ColumnType.INT:
            if offset + 4 > len(record_bytes):
                raise ValueError(f"INT数据超出记录边界")
            return struct.unpack('<i', record_bytes[offset:offset + 4])[0]

        elif col.type == ColumnType.VARCHAR:
            if offset + 2 > len(record_bytes):
                raise ValueError(f"VARCHAR长度超出记录边界")

            # 读取长度
            str_len = struct.unpack('<H', record_bytes[offset:offset + 2])[0]

            # 读取字符串数据
            str_start = offset + 2
            str_end = str_start + str_len

            if str_end > len(record_bytes):
                raise ValueError(f"VARCHAR数据超出记录边界")

            str_bytes = record_bytes[str_start:str_end]
            return str_bytes.decode('utf-8')

        else:
            raise ValueError(f"不支持的列类型: {col.type}")


class TableSchema:
    """表模式定义"""

    def __init__(self, table_name: str, columns: List[ColumnDef]):
        self.table_name = table_name
        self.columns = columns
        self.encoder = RecordEncoder(columns)
        self.decoder = RecordDecoder(columns)

    def encode_row(self, row_data: Dict[str, Any]) -> bytes:
        """编码行数据"""
        return self.encoder.encode(row_data)

    def decode_row(self, record_bytes: bytes) -> Dict[str, Any]:
        """解码行数据"""
        return self.decoder.decode(record_bytes)

    def get_column_names(self) -> List[str]:
        """获取列名列表"""
        return [col.name for col in self.columns]

    def get_column(self, name: str) -> Optional[ColumnDef]:
        """根据名称获取列定义"""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def __repr__(self):
        col_strs = [str(col) for col in self.columns]
        return f"TableSchema({self.table_name}, [{', '.join(col_strs)}])"


# ==================== 测试代码 ====================

def test_record_serdes():
    """测试记录序列化功能"""
    print("=== Record SerDes 功能测试 ===")

    # 1. 定义表模式
    print("1. 定义表模式:")
    columns = [
        ColumnDef("id", ColumnType.INT),
        ColumnDef("name", ColumnType.VARCHAR, 50),
        ColumnDef("age", ColumnType.INT),
        ColumnDef("email", ColumnType.VARCHAR, 100)
    ]

    schema = TableSchema("users", columns)
    print(f"   {schema}")

    # 2. 测试正常数据编码/解码
    print("\n2. 测试正常数据:")
    test_rows = [
        {"id": 1, "name": "Alice", "age": 25, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 30, "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"}
    ]

    for i, row_data in enumerate(test_rows):
        print(f"   原始数据{i + 1}: {row_data}")

        # 编码
        encoded = schema.encode_row(row_data)
        print(f"   编码后: {len(encoded)} 字节")

        # 解码
        decoded = schema.decode_row(encoded)
        print(f"   解码后: {decoded}")

        # 验证一致性
        is_match = row_data == decoded
        print(f"   数据一致: {'✓' if is_match else '✗'}")
        print()

    # 3. 测试NULL值处理
    print("3. 测试NULL值处理:")
    null_test_rows = [
        {"id": 4, "name": "David", "age": None, "email": "david@example.com"},
        {"id": 5, "name": None, "age": 28, "email": None},
        {"id": None, "name": "Eve", "age": None, "email": None}
    ]

    for i, row_data in enumerate(null_test_rows):
        print(f"   NULL测试{i + 1}: {row_data}")

        encoded = schema.encode_row(row_data)
        decoded = schema.decode_row(encoded)

        print(f"   解码结果: {decoded}")

        is_match = row_data == decoded
        print(f"   数据一致: {'✓' if is_match else '✗'}")
        print()

    # 4. 测试边界情况
    print("4. 测试边界情况:")

    # 最长字符串
    max_name = "x" * 50
    max_email = "y" * 100
    max_row = {"id": 999, "name": max_name, "age": 100, "email": max_email}

    print(f"   最大长度测试: name={len(max_name)}, email={len(max_email)}")
    encoded = schema.encode_row(max_row)
    decoded = schema.decode_row(encoded)
    print(f"   编码大小: {len(encoded)} 字节")
    print(f"   解码成功: {'✓' if max_row == decoded else '✗'}")

    # 空字符串
    empty_row = {"id": 0, "name": "", "age": 0, "email": ""}
    print(f"   空字符串测试: {empty_row}")
    encoded = schema.encode_row(empty_row)
    decoded = schema.decode_row(encoded)
    print(f"   解码成功: {'✓' if empty_row == decoded else '✗'}")

    # 5. 错误处理测试
    print("\n5. 错误处理测试:")

    # 字符串过长
    try:
        long_name_row = {"id": 1, "name": "x" * 51, "age": 25, "email": "test@example.com"}
        schema.encode_row(long_name_row)
        print("   错误: 应该拒绝过长字符串")
    except ValueError as e:
        print(f"   正确: 拒绝过长字符串 - {e}")

    # 类型错误
    try:
        wrong_type_row = {"id": "not_a_number", "name": "Alice", "age": 25, "email": "alice@example.com"}
        schema.encode_row(wrong_type_row)
        print("   错误: 应该拒绝错误类型")
    except ValueError as e:
        print(f"   正确: 拒绝错误类型 - {e}")

    print("\nRecord SerDes测试完成!")


def test_integration_with_page():
    """测试与SlottedPage的集成"""
    print("\n=== SerDes与SlottedPage集成测试 ===")

    from page import SlottedPage

    # 创建表模式
    columns = [
        ColumnDef("id", ColumnType.INT),
        ColumnDef("name", ColumnType.VARCHAR, 30),
        ColumnDef("score", ColumnType.INT)
    ]
    schema = TableSchema("students", columns)

    # 创建页面
    page = SlottedPage(1)

    # 准备测试数据
    students = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
        {"id": 3, "name": "Charlie", "score": 92},
        {"id": 4, "name": "Diana", "score": None},  # NULL分数
        {"id": 5, "name": None, "score": 88}  # NULL姓名
    ]

    print("1. 插入学生记录到页面:")
    slot_ids = []
    for student in students:
        # 编码为字节
        record_bytes = schema.encode_row(student)

        # 插入到页面
        slot_id = page.insert(record_bytes)
        slot_ids.append(slot_id)

        print(f"   学生 {student} -> 槽{slot_id}")

    print(f"\n页面状态: {page}")

    print("\n2. 从页面读取并解码:")
    for slot_id in slot_ids:
        # 从页面读取字节
        record_bytes = page.read(slot_id)

        # 解码为行数据
        student = schema.decode_row(record_bytes)

        print(f"   槽{slot_id}: {student}")

    print("\n3. 序列化页面测试:")
    # 序列化页面
    page_bytes = page.to_bytes()

    # 反序列化页面
    restored_page = SlottedPage.from_bytes(1, page_bytes)

    print("   页面序列化/反序列化成功")

    # 验证数据完整性
    print("   验证数据完整性:")
    for slot_id in slot_ids:
        original_bytes = page.read(slot_id)
        restored_bytes = restored_page.read(slot_id)

        if original_bytes == restored_bytes:
            student = schema.decode_row(restored_bytes)
            print(f"     槽{slot_id}: ✓ {student}")
        else:
            print(f"     槽{slot_id}: ✗ 数据不一致")

    print("\n集成测试完成!")


if __name__ == "__main__":
    test_record_serdes()
    test_integration_with_page()