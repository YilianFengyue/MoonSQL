# 文件路径: MoonSQL/src/engine/constraints.py

"""
约束元数据管理器
负责外键约束的存储、查询和管理
"""

from typing import List, Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class ForeignKeyConstraint:
    """外键约束定义"""
    fk_id: int
    table_id: int  # 子表ID
    column_name: str  # 子表列名
    ref_table_id: int  # 父表ID
    ref_column_name: str  # 父表列名
    constraint_name: str  # 约束名


class ConstraintManager:
    """约束管理器 - 扩展 CatalogManager 的约束功能"""

    # 系统表名
    SYS_FOREIGN_KEYS = "sys_foreign_keys"

    def __init__(self, storage_engine, catalog_mgr):
        self.storage_engine = storage_engine
        self.catalog_mgr = catalog_mgr

        # 外键缓存
        self.fk_cache: Dict[int, List[ForeignKeyConstraint]] = {}
        self.next_fk_id = 1

        # 初始化外键系统表
        self._initialize_fk_table()
        self._load_fk_cache()

    def _initialize_fk_table(self):
        """初始化外键系统表"""
        existing_tables = self.storage_engine.list_tables()

        if self.SYS_FOREIGN_KEYS not in existing_tables:
            print("创建系统表: sys_foreign_keys")
            self.storage_engine.create_table(self.SYS_FOREIGN_KEYS, [
                {"name": "fk_id", "type": "INT"},
                {"name": "table_id", "type": "INT"},
                {"name": "column_name", "type": "VARCHAR", "max_length": 64},
                {"name": "ref_table_id", "type": "INT"},
                {"name": "ref_column_name", "type": "VARCHAR", "max_length": 64},
                {"name": "constraint_name", "type": "VARCHAR", "max_length": 128}
            ])

    def _load_fk_cache(self):
        """从系统表加载外键到缓存"""
        try:
            for row in self.storage_engine.seq_scan(self.SYS_FOREIGN_KEYS):
                fk = ForeignKeyConstraint(
                    fk_id=row['fk_id'],
                    table_id=row['table_id'],
                    column_name=row['column_name'],
                    ref_table_id=row['ref_table_id'],
                    ref_column_name=row['ref_column_name'],
                    constraint_name=row['constraint_name']
                )

                if fk.table_id not in self.fk_cache:
                    self.fk_cache[fk.table_id] = []
                self.fk_cache[fk.table_id].append(fk)

                self.next_fk_id = max(self.next_fk_id, fk.fk_id + 1)

        except Exception as e:
            print(f"加载外键缓存失败: {e}")

    def add_foreign_key(self, table_name: str, column_name: str,
                        ref_table_name: str, ref_column_name: str,
                        constraint_name: str = None) -> int:
        """
        添加外键约束

        Args:
            table_name: 子表名
            column_name: 子表列名
            ref_table_name: 父表名
            ref_column_name: 父表列名
            constraint_name: 约束名（可选）

        Returns:
            外键ID

        Raises:
            ValueError: 表或列不存在
        """
        # 验证表和列存在性
        table_meta = self.catalog_mgr.get_table_metadata(table_name)
        if not table_meta:
            raise ValueError(f"子表不存在: {table_name}")

        ref_table_meta = self.catalog_mgr.get_table_metadata(ref_table_name)
        if not ref_table_meta:
            raise ValueError(f"父表不存在: {ref_table_name}")

        if not self.catalog_mgr.column_exists(table_name, column_name):
            raise ValueError(f"子表列不存在: {table_name}.{column_name}")

        if not self.catalog_mgr.column_exists(ref_table_name, ref_column_name):
            raise ValueError(f"父表列不存在: {ref_table_name}.{ref_column_name}")

        # 生成约束名
        if not constraint_name:
            constraint_name = f"fk_{table_name}_{column_name}_{ref_table_name}_{ref_column_name}"

        # 分配外键ID
        fk_id = self.next_fk_id
        self.next_fk_id += 1

        # 创建外键对象
        fk = ForeignKeyConstraint(
            fk_id=fk_id,
            table_id=table_meta.table_id,
            column_name=column_name,
            ref_table_id=ref_table_meta.table_id,
            ref_column_name=ref_column_name,
            constraint_name=constraint_name
        )

        # 插入系统表
        fk_row = {
            "fk_id": fk.fk_id,
            "table_id": fk.table_id,
            "column_name": fk.column_name,
            "ref_table_id": fk.ref_table_id,
            "ref_column_name": fk.ref_column_name,
            "constraint_name": fk.constraint_name
        }
        self.storage_engine.insert_row(self.SYS_FOREIGN_KEYS, fk_row)

        # 更新缓存
        if fk.table_id not in self.fk_cache:
            self.fk_cache[fk.table_id] = []
        self.fk_cache[fk.table_id].append(fk)

        print(f"添加外键约束: {constraint_name}")
        return fk_id

    def get_table_foreign_keys(self, table_name: str) -> List[ForeignKeyConstraint]:
        """获取表的所有外键约束"""
        table_meta = self.catalog_mgr.get_table_metadata(table_name)
        if not table_meta:
            return []

        return self.fk_cache.get(table_meta.table_id, [])

    def get_referencing_foreign_keys(self, ref_table_name: str) -> List[ForeignKeyConstraint]:
        """获取引用指定表的所有外键约束"""
        ref_table_meta = self.catalog_mgr.get_table_metadata(ref_table_name)
        if not ref_table_meta:
            return []

        referencing_fks = []
        for fks in self.fk_cache.values():
            for fk in fks:
                if fk.ref_table_id == ref_table_meta.table_id:
                    referencing_fks.append(fk)

        return referencing_fks

    def drop_table_foreign_keys(self, table_name: str) -> int:
        """删除表的所有外键约束"""
        table_meta = self.catalog_mgr.get_table_metadata(table_name)
        if not table_meta:
            return 0

        table_id = table_meta.table_id

        # 从系统表删除
        deleted_count = self.storage_engine.delete_where(
            self.SYS_FOREIGN_KEYS,
            lambda row: row['table_id'] == table_id
        )

        # 清理缓存
        if table_id in self.fk_cache:
            del self.fk_cache[table_id]

        return deleted_count