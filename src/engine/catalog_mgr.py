# 文件路径: MoonSQL/src/engine/catalog_mgr.py

"""
CatalogManager - 系统目录管理器

【功能说明】
- 管理数据库元数据：表结构、列信息、索引信息
- 维护系统表：sys_tables, sys_columns, sys_indexes
- 为SQL编译器提供schema查询接口
- 支持系统目录的持久化和一致性

【系统表设计】
sys_tables: 存储表的基本信息
- table_id(INT), table_name(VARCHAR), created_time(INT), row_count(INT)

sys_columns: 存储列的详细信息
- table_id(INT), column_name(VARCHAR), column_type(VARCHAR), max_length(INT), ordinal_position(INT)

sys_indexes: 存储索引信息
- index_id(INT), table_id(INT), index_name(VARCHAR), column_name(VARCHAR), index_type(VARCHAR)

【设计原则】
- 系统表本身也通过StorageEngine存储，确保一致性
- 内存缓存常用元数据，提升查询性能
- 支持事务性元数据操作(未来扩展)
"""

import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass


@dataclass
class TableMetadata:
    """表元数据"""
    table_id: int
    table_name: str
    created_time: int
    row_count: int = 0


@dataclass
class ColumnMetadata:
    """列元数据"""
    table_id: int
    column_name: str
    column_type: str
    max_length: Optional[int]
    ordinal_position: int


@dataclass
class IndexMetadata:
    """索引元数据"""
    index_id: int
    table_id: int
    index_name: str
    column_name: str
    index_type: str  # "ordered", "btree"


class CatalogManager:
    """系统目录管理器"""

    # 系统表名
    SYS_TABLES = "sys_tables"
    SYS_COLUMNS = "sys_columns"
    SYS_INDEXES = "sys_indexes"

    def __init__(self, storage_engine):
        self.storage_engine = storage_engine

        # 内存缓存
        self.table_cache: Dict[str, TableMetadata] = {}
        self.column_cache: Dict[int, List[ColumnMetadata]] = {}
        self.index_cache: Dict[int, List[IndexMetadata]] = {}

        # ID分配器
        self.next_table_id = 1
        self.next_index_id = 1

        # 初始化系统目录
        self._initialize_system_catalog()
        self._load_catalog_cache()

        print("CatalogManager初始化完成")

    def _initialize_system_catalog(self):
        """初始化系统目录表"""

        # 检查系统表是否已存在
        existing_tables = self.storage_engine.list_tables()

        if self.SYS_TABLES not in existing_tables:
            print("创建系统表: sys_tables")
            self.storage_engine.create_table(self.SYS_TABLES, [
                {"name": "table_id", "type": "INT"},
                {"name": "table_name", "type": "VARCHAR", "max_length": 64},
                {"name": "created_time", "type": "INT"},
                {"name": "row_count", "type": "INT"}
            ])

        if self.SYS_COLUMNS not in existing_tables:
            print("创建系统表: sys_columns")
            self.storage_engine.create_table(self.SYS_COLUMNS, [
                {"name": "table_id", "type": "INT"},
                {"name": "column_name", "type": "VARCHAR", "max_length": 64},
                {"name": "column_type", "type": "VARCHAR", "max_length": 20},
                {"name": "max_length", "type": "INT"},
                {"name": "ordinal_position", "type": "INT"}
            ])

        if self.SYS_INDEXES not in existing_tables:
            print("创建系统表: sys_indexes")
            self.storage_engine.create_table(self.SYS_INDEXES, [
                {"name": "index_id", "type": "INT"},
                {"name": "table_id", "type": "INT"},
                {"name": "index_name", "type": "VARCHAR", "max_length": 64},
                {"name": "column_name", "type": "VARCHAR", "max_length": 64},
                {"name": "index_type", "type": "VARCHAR", "max_length": 20}
            ])

    def _load_catalog_cache(self):
        """从系统表加载元数据到内存缓存"""
        print("加载系统目录到缓存...")

        # 加载表信息
        for row in self.storage_engine.seq_scan(self.SYS_TABLES):
            table_meta = TableMetadata(
                table_id=row['table_id'],
                table_name=row['table_name'],
                created_time=row['created_time'],
                row_count=row['row_count']
            )
            self.table_cache[table_meta.table_name] = table_meta
            self.next_table_id = max(self.next_table_id, table_meta.table_id + 1)

        # 加载列信息
        for row in self.storage_engine.seq_scan(self.SYS_COLUMNS):
            table_id = row['table_id']
            col_meta = ColumnMetadata(
                table_id=table_id,
                column_name=row['column_name'],
                column_type=row['column_type'],
                max_length=row['max_length'],
                ordinal_position=row['ordinal_position']
            )

            if table_id not in self.column_cache:
                self.column_cache[table_id] = []
            self.column_cache[table_id].append(col_meta)

        # 对列按ordinal_position排序
        for cols in self.column_cache.values():
            cols.sort(key=lambda c: c.ordinal_position)

        # 加载索引信息
        for row in self.storage_engine.seq_scan(self.SYS_INDEXES):
            table_id = row['table_id']
            idx_meta = IndexMetadata(
                index_id=row['index_id'],
                table_id=table_id,
                index_name=row['index_name'],
                column_name=row['column_name'],
                index_type=row['index_type']
            )

            if table_id not in self.index_cache:
                self.index_cache[table_id] = []
            self.index_cache[table_id].append(idx_meta)
            self.next_index_id = max(self.next_index_id, idx_meta.index_id + 1)

        print(
            f"缓存加载完成: {len(self.table_cache)}表, {sum(len(cols) for cols in self.column_cache.values())}列, {sum(len(idxs) for idxs in self.index_cache.values())}索引")

    def register_table(self, table_name: str, columns: List[Dict[str, Any]]) -> int:
        """
        注册新表到系统目录

        Args:
            table_name: 表名
            columns: 列定义列表

        Returns:
            分配的table_id
        """
        if table_name in self.table_cache:
            raise ValueError(f"表已存在: {table_name}")

        # 分配table_id
        table_id = self.next_table_id
        self.next_table_id += 1

        current_time = int(time.time())

        # 插入sys_tables
        table_row = {
            "table_id": table_id,
            "table_name": table_name,
            "created_time": current_time,
            "row_count": 0
        }
        self.storage_engine.insert_row(self.SYS_TABLES, table_row)

        # 插入sys_columns
        for ordinal, col_def in enumerate(columns):
            col_row = {
                "table_id": table_id,
                "column_name": col_def["name"],
                "column_type": col_def["type"],
                "max_length": col_def.get("max_length"),
                "ordinal_position": ordinal
            }
            self.storage_engine.insert_row(self.SYS_COLUMNS, col_row)

        # 更新缓存
        table_meta = TableMetadata(table_id, table_name, current_time, 0)
        self.table_cache[table_name] = table_meta

        col_metas = []
        for ordinal, col_def in enumerate(columns):
            col_meta = ColumnMetadata(
                table_id=table_id,
                column_name=col_def["name"],
                column_type=col_def["type"],
                max_length=col_def.get("max_length"),
                ordinal_position=ordinal
            )
            col_metas.append(col_meta)

        self.column_cache[table_id] = col_metas

        print(f"注册表到系统目录: {table_name} (table_id={table_id})")
        return table_id

    def unregister_table(self, table_name: str) -> bool:
        """
        从系统目录移除表

        Args:
            table_name: 表名

        Returns:
            是否成功移除
        """
        if table_name not in self.table_cache:
            return False

        table_meta = self.table_cache[table_name]
        table_id = table_meta.table_id

        # 从系统表删除记录
        self.storage_engine.delete_where(
            self.SYS_TABLES,
            lambda row: row['table_id'] == table_id
        )

        self.storage_engine.delete_where(
            self.SYS_COLUMNS,
            lambda row: row['table_id'] == table_id
        )

        self.storage_engine.delete_where(
            self.SYS_INDEXES,
            lambda row: row['table_id'] == table_id
        )

        # 清理缓存
        del self.table_cache[table_name]
        if table_id in self.column_cache:
            del self.column_cache[table_id]
        if table_id in self.index_cache:
            del self.index_cache[table_id]

        print(f"从系统目录移除表: {table_name}")
        return True

    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """获取表元数据"""
        return self.table_cache.get(table_name)

    def get_table_columns(self, table_name: str) -> List[ColumnMetadata]:
        """获取表的列信息"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return []

        return self.column_cache.get(table_meta.table_id, [])

    def get_table_indexes(self, table_name: str) -> List[IndexMetadata]:
        """获取表的索引信息"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return []

        return self.index_cache.get(table_meta.table_id, [])

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.table_cache

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        columns = self.get_table_columns(table_name)
        return any(col.column_name == column_name for col in columns)

    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取列的数据类型"""
        columns = self.get_table_columns(table_name)
        for col in columns:
            if col.column_name == column_name:
                return col.column_type
        return None

    def register_index(self, table_name: str, index_name: str, column_name: str, index_type: str = "ordered") -> int:
        """
        注册索引到系统目录

        Args:
            table_name: 表名
            index_name: 索引名
            column_name: 列名
            index_type: 索引类型

        Returns:
            分配的index_id
        """
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            raise ValueError(f"表不存在: {table_name}")

        if not self.column_exists(table_name, column_name):
            raise ValueError(f"列不存在: {table_name}.{column_name}")

        # 分配index_id
        index_id = self.next_index_id
        self.next_index_id += 1

        # 插入sys_indexes
        index_row = {
            "index_id": index_id,
            "table_id": table_meta.table_id,
            "index_name": index_name,
            "column_name": column_name,
            "index_type": index_type
        }
        self.storage_engine.insert_row(self.SYS_INDEXES, index_row)

        # 更新缓存
        index_meta = IndexMetadata(
            index_id=index_id,
            table_id=table_meta.table_id,
            index_name=index_name,
            column_name=column_name,
            index_type=index_type
        )

        if table_meta.table_id not in self.index_cache:
            self.index_cache[table_meta.table_id] = []
        self.index_cache[table_meta.table_id].append(index_meta)

        print(f"注册索引到系统目录: {index_name} on {table_name}.{column_name}")
        return index_id

    def update_table_row_count(self, table_name: str, delta: int):
        """更新表的行数统计"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return

        # 更新缓存
        table_meta.row_count += delta

        # 更新系统表(简化实现：全量更新)
        self.storage_engine.delete_where(
            self.SYS_TABLES,
            lambda row: row['table_id'] == table_meta.table_id
        )

        table_row = {
            "table_id": table_meta.table_id,
            "table_name": table_meta.table_name,
            "created_time": table_meta.created_time,
            "row_count": table_meta.row_count
        }
        self.storage_engine.insert_row(self.SYS_TABLES, table_row)

    def list_all_tables(self) -> List[str]:
        """列出所有用户表(排除系统表)"""
        system_tables = {self.SYS_TABLES, self.SYS_COLUMNS, self.SYS_INDEXES}
        return [name for name in self.table_cache.keys() if name not in system_tables]

    def get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """获取表的完整schema信息"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return None

        columns = self.get_table_columns(table_name)
        indexes = self.get_table_indexes(table_name)

        return {
            "table_name": table_meta.table_name,
            "table_id": table_meta.table_id,
            "created_time": table_meta.created_time,
            "row_count": table_meta.row_count,
            "columns": [
                {
                    "name": col.column_name,
                    "type": col.column_type,
                    "max_length": col.max_length,
                    "position": col.ordinal_position
                }
                for col in columns
            ],
            "indexes": [
                {
                    "name": idx.index_name,
                    "column": idx.column_name,
                    "type": idx.index_type
                }
                for idx in indexes
            ]
        }

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        user_tables = self.list_all_tables()
        total_rows = sum(meta.row_count for meta in self.table_cache.values()
                         if meta.table_name in user_tables)
        total_indexes = sum(len(idxs) for table_id, idxs in self.index_cache.items()
                            if any(self.table_cache[name].table_id == table_id
                                   for name in user_tables if name in self.table_cache))

        return {
            "total_tables": len(user_tables),
            "total_rows": total_rows,
            "total_indexes": total_indexes,
            "system_tables": 3,  # sys_tables, sys_columns, sys_indexes
            "next_table_id": self.next_table_id,
            "next_index_id": self.next_index_id
        }


# ==================== 测试代码 ====================

def test_catalog_manager():
    """测试系统目录管理器"""
    print("=== CatalogManager 功能测试 ===")

    # 导入存储引擎
    try:
        from ..storage.storage_engine import StorageEngine
    except ImportError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
        from src.storage.storage_engine import StorageEngine

    # 创建存储引擎和目录管理器
    storage = StorageEngine("test_catalog_data", buffer_capacity=8)
    catalog = CatalogManager(storage)

    print(f"\n初始状态: {catalog.get_database_stats()}")

    print("\n1. 测试表注册:")

    # 注册学生表
    students_cols = [
        {"name": "student_id", "type": "INT"},
        {"name": "name", "type": "VARCHAR", "max_length": 50},
        {"name": "major", "type": "VARCHAR", "max_length": 30},
        {"name": "gpa", "type": "INT"}  # 简化为INT，表示GPA*100
    ]
    table_id1 = catalog.register_table("students", students_cols)

    # 注册课程表
    courses_cols = [
        {"name": "course_id", "type": "INT"},
        {"name": "title", "type": "VARCHAR", "max_length": 100},
        {"name": "credits", "type": "INT"}
    ]
    table_id2 = catalog.register_table("courses", courses_cols)

    print(f"   注册students表: table_id={table_id1}")
    print(f"   注册courses表: table_id={table_id2}")

    print("\n2. 测试元数据查询:")

    # 查询表信息
    print(f"   students表存在: {catalog.table_exists('students')}")
    print(f"   nonexistent表存在: {catalog.table_exists('nonexistent')}")

    # 查询列信息
    student_cols = catalog.get_table_columns("students")
    print(f"   students表列数: {len(student_cols)}")
    for col in student_cols:
        print(f"     {col.column_name}: {col.column_type}" +
              (f"({col.max_length})" if col.max_length else ""))

    # 查询列类型
    print(f"   students.name类型: {catalog.get_column_type('students', 'name')}")
    print(f"   students.gpa类型: {catalog.get_column_type('students', 'gpa')}")

    print("\n3. 测试索引注册:")

    # 为students表的student_id创建索引
    idx_id1 = catalog.register_index("students", "idx_student_id", "student_id", "ordered")

    # 为courses表的course_id创建索引
    idx_id2 = catalog.register_index("courses", "idx_course_id", "course_id", "btree")

    print(f"   创建索引idx_student_id: index_id={idx_id1}")
    print(f"   创建索引idx_course_id: index_id={idx_id2}")

    # 查询索引信息
    student_indexes = catalog.get_table_indexes("students")
    print(f"   students表索引数: {len(student_indexes)}")
    for idx in student_indexes:
        print(f"     {idx.index_name}: {idx.column_name} ({idx.index_type})")

    print("\n4. 测试完整schema:")

    students_schema = catalog.get_schema_info("students")
    print(f"   students表完整信息:")
    print(f"     表名: {students_schema['table_name']}")
    print(f"     表ID: {students_schema['table_id']}")
    print(f"     行数: {students_schema['row_count']}")
    print(f"     列数: {len(students_schema['columns'])}")
    print(f"     索引数: {len(students_schema['indexes'])}")

    print("\n5. 测试行数统计更新:")

    # 模拟插入数据，更新行数
    catalog.update_table_row_count("students", 5)
    catalog.update_table_row_count("courses", 3)

    print(f"   更新后students行数: {catalog.get_table_metadata('students').row_count}")
    print(f"   更新后courses行数: {catalog.get_table_metadata('courses').row_count}")

    print("\n6. 测试数据库统计:")

    db_stats = catalog.get_database_stats()
    print(f"   数据库统计:")
    for key, value in db_stats.items():
        print(f"     {key}: {value}")

    print(f"\n7. 测试表列表:")

    all_tables = catalog.list_all_tables()
    print(f"   用户表列表: {all_tables}")

    # 清理测试表
    print(f"\n8. 清理测试:")
    success1 = catalog.unregister_table("students")
    success2 = catalog.unregister_table("courses")
    print(f"   删除students: {success1}")
    print(f"   删除courses: {success2}")

    final_stats = catalog.get_database_stats()
    print(f"   清理后统计: {final_stats}")

    # 关闭存储引擎
    storage.close()

    return len(all_tables)


def test_catalog_persistence():
    """测试目录持久化"""
    print("\n=== 目录持久化测试 ===")

    try:
        from ..storage.storage_engine import StorageEngine
    except ImportError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
        from src.storage.storage_engine import StorageEngine

    print("1. 第一次启动，创建目录:")

    # 第一次启动
    storage1 = StorageEngine("test_catalog_persist", buffer_capacity=4)
    catalog1 = CatalogManager(storage1)

    # 创建测试表
    test_cols = [
        {"name": "id", "type": "INT"},
        {"name": "data", "type": "VARCHAR", "max_length": 100}
    ]
    table_id = catalog1.register_table("test_persist", test_cols)
    index_id = catalog1.register_index("test_persist", "idx_test_id", "id", "ordered")

    catalog1.update_table_row_count("test_persist", 10)

    print(f"   创建表test_persist: table_id={table_id}")
    print(f"   创建索引: index_id={index_id}")
    print(f"   设置行数: 10")

    # 获取第一次的信息
    first_schema = catalog1.get_schema_info("test_persist")
    first_stats = catalog1.get_database_stats()

    storage1.close()
    print("   第一次启动结束")

    print("\n2. 第二次启动，验证持久化:")

    # 第二次启动
    storage2 = StorageEngine("test_catalog_persist", buffer_capacity=4)
    catalog2 = CatalogManager(storage2)

    # 验证数据恢复
    second_schema = catalog2.get_schema_info("test_persist")
    second_stats = catalog2.get_database_stats()

    print(f"   重启后表存在: {catalog2.table_exists('test_persist')}")
    print(f"   重启后行数: {second_schema['row_count'] if second_schema else 'N/A'}")
    print(f"   重启后索引数: {len(second_schema['indexes']) if second_schema else 0}")

    # 比较一致性
    schema_consistent = (first_schema == second_schema)
    stats_consistent = (first_stats == second_stats)

    print(f"   schema一致性: {'通过' if schema_consistent else '失败'}")
    print(f"   统计一致性: {'通过' if stats_consistent else '失败'}")

    # 清理
    catalog2.unregister_table("test_persist")
    storage2.close()

    return schema_consistent and stats_consistent


def run_all_catalog_tests():
    """运行所有目录管理器测试"""
    print("CatalogManager 系统目录测试")
    print("=" * 60)

    table_count = test_catalog_manager()
    persistence_ok = test_catalog_persistence()

    print("\n" + "=" * 60)
    print(f"系统目录测试完成!")
    print(f"最大表数: {table_count}, 持久化测试: {'通过' if persistence_ok else '失败'}")


if __name__ == "__main__":
    run_all_catalog_tests()