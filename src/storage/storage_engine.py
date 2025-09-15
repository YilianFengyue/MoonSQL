# 文件路径: MoonSQL/src/storage/storage_engine.py

"""
StorageEngine - 统一存储引擎

【功能说明】
- 整合SlottedPage + FileManager + BufferPool + SerDes
- 提供表级别操作接口：create_table, insert_row, seq_scan, delete_where
- 管理表模式(schema)和记录编解码
- 支持持久化验证和系统重启

【设计架构】
SQL编译器 → StorageEngine → BufferPool → FileManager → SlottedPage → 磁盘文件

【关键接口】
- create_table(name, columns): 创建表
- insert_row(table, row_data): 插入记录
- seq_scan(table): 全表扫描迭代器
- delete_where(table, predicate): 按条件删除
"""

import os
import json
import time
from typing import Dict, List, Any, Iterator, Callable, Optional, Tuple
from storage.file_manager import FileManager
from storage.buffer import BufferPool
from storage.serdes import TableSchema, ColumnDef, ColumnType


class TableInfo:
    """表信息管理"""

    def __init__(self, name: str, schema: TableSchema):
        self.name = name
        self.schema = schema
        self.total_rows = 0
        self.total_pages = 0
        self.created_time = None
        self.last_modified = None

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            'name': self.name,
            'columns': [
                {
                    'name': col.name,
                    'type': col.type,
                    'max_length': col.max_length
                }
                for col in self.schema.columns
            ],
            'total_rows': self.total_rows,
            'total_pages': self.total_pages,
            'created_time': self.created_time,
            'last_modified': self.last_modified
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableInfo':
        """从字典反序列化"""
        columns = [
            ColumnDef(col['name'], col['type'], col.get('max_length'))
            for col in data['columns']
        ]
        schema = TableSchema(data['name'], columns)

        table_info = cls(data['name'], schema)
        table_info.total_rows = data.get('total_rows', 0)
        table_info.total_pages = data.get('total_pages', 0)
        table_info.created_time = data.get('created_time')
        table_info.last_modified = data.get('last_modified')

        return table_info


class StorageEngine:
    """统一存储引擎"""

    def __init__(self, data_dir: str = "data", buffer_capacity: int = 64, buffer_policy: str = "LRU"):
        """
        初始化存储引擎

        Args:
            data_dir: 数据目录
            buffer_capacity: 缓冲池容量(页数)
            buffer_policy: 缓冲池策略(LRU/FIFO)
        """
        self.data_dir = data_dir

        # 初始化存储层组件
        self.file_manager = FileManager(data_dir)
        self.buffer_pool = BufferPool(self.file_manager, buffer_capacity, buffer_policy)

        # 表信息管理
        self.tables: Dict[str, TableInfo] = {}
        self.metadata_file = os.path.join(data_dir, "tables_metadata.json")

        # 加载已有表的元数据
        self._load_metadata()

        print(f"StorageEngine初始化: 数据目录={data_dir}, 缓冲池={buffer_capacity}页({buffer_policy})")

    def _load_metadata(self) -> None:
        """加载表元数据"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                for table_name, table_data in metadata.items():
                    if self.file_manager.table_exists(table_name):
                        self.tables[table_name] = TableInfo.from_dict(table_data)
                        print(f"加载表元数据: {table_name}")
                    else:
                        print(f"警告: 元数据中的表{table_name}文件不存在")

            except (json.JSONDecodeError, KeyError) as e:
                print(f"元数据文件损坏，忽略: {e}")

    def _save_metadata(self) -> None:
        """保存表元数据"""
        metadata = {name: info.to_dict() for name, info in self.tables.items()}

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> None:
        """
        创建表

        Args:
            table_name: 表名
            columns: 列定义列表，格式: [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR", "max_length": 50}]

        Raises:
            ValueError: 表已存在或列定义无效
        """
        if table_name in self.tables:
            raise ValueError(f"表已存在: {table_name}")

        # 解析列定义
        try:
            column_defs = []
            for col_spec in columns:
                name = col_spec['name']
                col_type = col_spec['type']
                max_length = col_spec.get('max_length')

                column_defs.append(ColumnDef(name, col_type, max_length))

            # 创建表模式
            schema = TableSchema(table_name, column_defs)

        except (KeyError, ValueError) as e:
            raise ValueError(f"无效的列定义: {e}")

        # 创建表文件
        self.file_manager.create_table_file(table_name)

        # 记录表信息
        import time
        table_info = TableInfo(table_name, schema)
        table_info.created_time = time.time()
        table_info.last_modified = time.time()

        self.tables[table_name] = table_info
        self._save_metadata()

        print(f"创建表成功: {table_name} ({len(column_defs)}列)")

    def drop_table(self, table_name: str) -> bool:
        """
        删除表

        Args:
            table_name: 表名

        Returns:
            是否删除成功
        """
        if table_name not in self.tables:
            return False

        # 从缓冲池中淘汰该表的所有页面
        self.buffer_pool.evict_table_pages(table_name)

        # 删除表文件
        success = self.file_manager.delete_table_file(table_name)

        if success:
            # 删除元数据
            del self.tables[table_name]
            self._save_metadata()
            print(f"删除表成功: {table_name}")

        return success

    def insert_row(self, table_name: str, row_data: Dict[str, Any]) -> bool:
        """
        插入记录

        Args:
            table_name: 表名
            row_data: 行数据字典

        Returns:
            是否插入成功

        Raises:
            ValueError: 表不存在或数据格式错误
        """
        if table_name not in self.tables:
            raise ValueError(f"表不存在: {table_name}")

        table_info = self.tables[table_name]

        # 编码行数据
        try:
            record_bytes = table_info.schema.encode_row(row_data)
        except ValueError as e:
            raise ValueError(f"数据编码失败: {e}")

        # 获取表的所有数据页，尝试插入
        page_ids = self.file_manager.get_all_page_ids(table_name)

        for page_id in page_ids:
            try:
                # 通过缓冲池获取页面
                page = self.buffer_pool.get_page(table_name, page_id)

                # 尝试插入记录
                slot_id = page.insert(record_bytes)

                if slot_id != -1:
                    # 插入成功，写回缓冲池(标记脏页)
                    self.buffer_pool.put_page(table_name, page, mark_dirty=True)

                    # 更新表统计
                    table_info.total_rows += 1
                    table_info.last_modified = time.time()
                    self._save_metadata()

                    return True

            except Exception as e:
                print(f"插入记录到页面{page_id}失败: {e}")
                continue

        # 所有现有页面都满，分配新页面
        try:
            new_page_id = self.file_manager.allocate_new_page(table_name)
            new_page = self.buffer_pool.get_page(table_name, new_page_id)

            slot_id = new_page.insert(record_bytes)
            if slot_id != -1:
                self.buffer_pool.put_page(table_name, new_page, mark_dirty=True)

                # 更新表统计
                table_info.total_rows += 1
                table_info.total_pages += 1
                table_info.last_modified = time.time()
                self._save_metadata()

                return True

        except Exception as e:
            print(f"分配新页面插入失败: {e}")
            return False

        return False

    def seq_scan(self, table_name: str) -> Iterator[Dict[str, Any]]:
        """
        全表顺序扫描

        Args:
            table_name: 表名

        Yields:
            行数据字典

        Raises:
            ValueError: 表不存在
        """
        if table_name not in self.tables:
            raise ValueError(f"表不存在: {table_name}")

        table_info = self.tables[table_name]
        page_ids = self.file_manager.get_all_page_ids(table_name)

        for page_id in page_ids:
            try:
                # 通过缓冲池获取页面
                page = self.buffer_pool.get_page(table_name, page_id)

                # 扫描页面中的所有活跃记录
                for slot_id, record_bytes in page.get_all_records():
                    try:
                        # 解码记录
                        row_data = table_info.schema.decode_row(record_bytes)
                        yield row_data

                    except Exception as e:
                        print(f"解码记录失败 {table_name}.{page_id}.{slot_id}: {e}")
                        continue

            except Exception as e:
                print(f"扫描页面失败 {table_name}.{page_id}: {e}")
                continue

    def delete_where(self, table_name: str, predicate: Callable[[Dict[str, Any]], bool]) -> int:
        """
        按条件删除记录

        Args:
            table_name: 表名
            predicate: 删除条件函数，返回True的记录将被删除

        Returns:
            删除的记录数量

        Raises:
            ValueError: 表不存在
        """
        if table_name not in self.tables:
            raise ValueError(f"表不存在: {table_name}")

        table_info = self.tables[table_name]
        page_ids = self.file_manager.get_all_page_ids(table_name)
        deleted_count = 0

        for page_id in page_ids:
            try:
                page = self.buffer_pool.get_page(table_name, page_id)
                page_modified = False

                # 扫描页面中的所有活跃记录
                for slot_id in range(page.get_slot_count()):
                    if page.is_deleted(slot_id):
                        continue

                    try:
                        # 读取并解码记录
                        record_bytes = page.read(slot_id)
                        row_data = table_info.schema.decode_row(record_bytes)

                        # 检查删除条件
                        if predicate(row_data):
                            page.delete(slot_id)
                            deleted_count += 1
                            page_modified = True

                    except Exception as e:
                        print(f"检查删除条件失败 {table_name}.{page_id}.{slot_id}: {e}")
                        continue

                # 如果页面有修改，写回缓冲池
                if page_modified:
                    self.buffer_pool.put_page(table_name, page, mark_dirty=True)

            except Exception as e:
                print(f"删除操作失败 {table_name}.{page_id}: {e}")
                continue

        # 更新表统计
        if deleted_count > 0:
            table_info.total_rows -= deleted_count
            table_info.last_modified = time.time()
            self._save_metadata()

        return deleted_count

    def update_where(self, table_name: str, predicate: Callable[[Dict[str, Any]], bool],
                     update_func: Callable[[Dict[str, Any]], Dict[str, Any]]) -> int:
        """
        按条件更新记录

        Args:
            table_name: 表名
            predicate: 更新条件函数，返回True的记录将被更新
            update_func: 更新函数，接收原记录返回新记录

        Returns:
            更新的记录数量

        Raises:
            ValueError: 表不存在
        """
        if table_name not in self.tables:
            raise ValueError(f"表不存在: {table_name}")

        table_info = self.tables[table_name]
        page_ids = self.file_manager.get_all_page_ids(table_name)
        updated_count = 0

        for page_id in page_ids:
            try:
                page = self.buffer_pool.get_page(table_name, page_id)
                page_modified = False

                # 扫描页面中的所有活跃记录
                for slot_id in range(page.get_slot_count()):
                    if page.is_deleted(slot_id):
                        continue

                    try:
                        # 读取并解码记录
                        record_bytes = page.read(slot_id)
                        row_data = table_info.schema.decode_row(record_bytes)

                        # 检查更新条件
                        if predicate(row_data):
                            # 执行更新
                            updated_row = update_func(row_data)

                            # 编码更新后的记录
                            updated_bytes = table_info.schema.encode_row(updated_row)

                            # ★ 修复：删除旧记录，插入新记录
                            page.delete(slot_id)
                            new_slot = page.insert(updated_bytes)

                            if new_slot != -1:
                                updated_count += 1
                                page_modified = True

                    except Exception as e:
                        print(f"更新记录失败 {table_name}.{page_id}.{slot_id}: {e}")
                        continue

                # 如果页面有修改，写回缓冲池
                if page_modified:
                    self.buffer_pool.put_page(table_name, page, mark_dirty=True)

            except Exception as e:
                print(f"更新操作失败 {table_name}.{page_id}: {e}")
                continue

        # 更新表统计
        if updated_count > 0:
            table_info.last_modified = time.time()
            self._save_metadata()

        return updated_count


    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """获取表信息"""
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """列出所有表名"""
        return list(self.tables.keys())

    def get_stats(self) -> Dict[str, Any]:
        """获取存储引擎统计信息"""
        buffer_stats = self.buffer_pool.get_stats()

        total_rows = sum(info.total_rows for info in self.tables.values())
        total_tables = len(self.tables)

        return {
            'total_tables': total_tables,
            'total_rows': total_rows,
            'buffer_pool': buffer_stats,
            'data_directory': self.data_dir
        }

    def flush_all(self) -> None:
        """刷新所有脏页到磁盘"""
        self.buffer_pool.flush_dirty_pages()
        self._save_metadata()

    def close(self) -> None:
        """关闭存储引擎"""
        print("关闭存储引擎...")
        self.flush_all()
        self.buffer_pool.close()
        self.file_manager.close_all()


# ==================== 测试代码 ====================

def test_storage_engine_basic():
    """测试存储引擎基本功能"""
    print("=== StorageEngine 基本功能测试 ===")

    # 创建存储引擎
    engine = StorageEngine("test_storage_data", buffer_capacity=4, buffer_policy="LRU")

    # 清理可能存在的测试表
    test_tables = ["students", "courses"]
    for table in test_tables:
        if table in engine.list_tables():
            engine.drop_table(table)

    print("\n1. 创建表:")

    # 创建学生表
    students_columns = [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR", "max_length": 50},
        {"name": "age", "type": "INT"},
        {"name": "email", "type": "VARCHAR", "max_length": 100}
    ]
    engine.create_table("students", students_columns)

    # 创建课程表
    courses_columns = [
        {"name": "course_id", "type": "INT"},
        {"name": "title", "type": "VARCHAR", "max_length": 100},
        {"name": "credits", "type": "INT"}
    ]
    engine.create_table("courses", courses_columns)

    print(f"   已创建表: {engine.list_tables()}")

    print("\n2. 插入数据:")

    # 插入学生数据
    students_data = [
        {"id": 1, "name": "Alice", "age": 20, "email": "alice@university.edu"},
        {"id": 2, "name": "Bob", "age": 21, "email": "bob@university.edu"},
        {"id": 3, "name": "Charlie", "age": 19, "email": "charlie@university.edu"},
        {"id": 4, "name": "Diana", "age": 22, "email": "diana@university.edu"},
        {"id": 5, "name": "Eve", "age": 20, "email": None}  # NULL email
    ]

    for student in students_data:
        success = engine.insert_row("students", student)
        print(f"   插入学生: {student['name']} -> {'成功' if success else '失败'}")

    # 插入课程数据
    courses_data = [
        {"course_id": 101, "title": "Database Systems", "credits": 3},
        {"course_id": 102, "title": "Computer Networks", "credits": 3},
        {"course_id": 103, "title": "Operating Systems", "credits": 4}
    ]

    for course in courses_data:
        success = engine.insert_row("courses", course)
        print(f"   插入课程: {course['title']} -> {'成功' if success else '失败'}")

    print("\n3. 全表扫描:")

    print("   学生表:")
    for row in engine.seq_scan("students"):
        print(f"     {row}")

    print("   课程表:")
    for row in engine.seq_scan("courses"):
        print(f"     {row}")

    print("\n4. 条件删除:")

    # 删除年龄小于20的学生
    deleted_count = engine.delete_where("students", lambda row: row.get("age", 0) < 20)
    print(f"   删除年龄<20的学生: {deleted_count}条")

    print("   删除后的学生表:")
    for row in engine.seq_scan("students"):
        print(f"     {row}")

    print("\n5. 存储引擎统计:")
    stats = engine.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    # 返回引擎用于持久化测试
    return engine


def test_persistence():
    """测试持久化功能"""
    print("\n=== 持久化测试 ===")

    print("1. 第一次启动，创建数据:")

    # 创建存储引擎并插入数据
    engine1 = StorageEngine("test_persistence_data", buffer_capacity=4)

    # 清理旧数据
    if "test_persistence" in engine1.list_tables():
        engine1.drop_table("test_persistence")

    # 创建测试表
    columns = [
        {"name": "id", "type": "INT"},
        {"name": "message", "type": "VARCHAR", "max_length": 200}
    ]
    engine1.create_table("test_persistence", columns)

    # 插入测试数据
    test_data = [
        {"id": 1, "message": "First message"},
        {"id": 2, "message": "Second message"},
        {"id": 3, "message": "Third message with 中文"}
    ]

    for data in test_data:
        success = engine1.insert_row("test_persistence", data)
        print(f"   插入: {data} -> {'成功' if success else '失败'}")

    # 显示数据
    print("   第一次启动的数据:")
    first_run_data = list(engine1.seq_scan("test_persistence"))
    for row in first_run_data:
        print(f"     {row}")

    # 关闭引擎(模拟程序退出)
    engine1.close()
    print("   第一次启动结束，数据已保存")

    print("\n2. 第二次启动，验证数据:")

    # 重新创建存储引擎(模拟程序重启)
    engine2 = StorageEngine("test_persistence_data", buffer_capacity=4)

    print(f"   重启后的表列表: {engine2.list_tables()}")

    # 读取数据验证
    print("   重启后的数据:")
    second_run_data = list(engine2.seq_scan("test_persistence"))
    for row in second_run_data:
        print(f"     {row}")

    # 比较数据一致性
    data_consistent = (len(first_run_data) == len(second_run_data) and
                       all(row1 == row2 for row1, row2 in zip(first_run_data, second_run_data)))

    print(f"   数据一致性: {'通过' if data_consistent else '失败'}")

    # 继续插入新数据验证可写入性
    new_data = {"id": 4, "message": "Message after restart"}
    success = engine2.insert_row("test_persistence", new_data)
    print(f"   重启后插入新数据: {'成功' if success else '失败'}")

    engine2.close()

    return data_consistent


def run_all_storage_tests():
    """运行所有存储引擎测试"""
    print("StorageEngine 完整功能测试")
    print("=" * 70)

    # 基本功能测试
    engine = test_storage_engine_basic()

    # 持久化测试
    persistence_ok = test_persistence()

    print("\n" + "=" * 70)
    print(f"存储引擎测试完成!")
    print(f"持久化测试: {'通过' if persistence_ok else '失败'}")

    # 最终清理
    if engine:
        engine.close()


if __name__ == "__main__":
    run_all_storage_tests()