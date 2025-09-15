# 文件路径: MoonSQL/src/engine/constraint_validator.py

"""
约束校验器
实现外键约束的RESTRICT语义校验
"""

from typing import Dict, Any, List, Optional
from .constraints import ForeignKeyConstraint


class ForeignKeyValidationError(Exception):
    """外键校验错误"""

    def __init__(self, message: str, constraint_name: str = None, table: str = None, column: str = None):
        self.constraint_name = constraint_name
        self.table = table
        self.column = column
        super().__init__(message)


class ConstraintValidator:
    """约束校验器 - 专门负责外键校验逻辑"""

    def __init__(self, storage_engine, catalog_mgr, constraint_mgr):
        self.storage_engine = storage_engine
        self.catalog_mgr = catalog_mgr
        self.constraint_mgr = constraint_mgr

    def validate_insert_foreign_keys(self, table_name: str, row_data: Dict[str, Any]):
        """
        验证插入操作的外键约束

        Args:
            table_name: 要插入的表名
            row_data: 插入的行数据

        Raises:
            ForeignKeyValidationError: 外键约束违反
        """
        # 获取表的所有外键约束
        foreign_keys = self.constraint_mgr.get_table_foreign_keys(table_name)

        for fk in foreign_keys:
            child_value = row_data.get(fk.column_name)

            # NULL值允许（外键允许NULL）
            if child_value is None:
                continue

            # 检查父表中是否存在对应值
            if not self._parent_key_exists(fk, child_value):
                ref_table_name = self._get_table_name_by_id(fk.ref_table_id)
                raise ForeignKeyValidationError(
                    f"外键约束违反: 在父表 '{ref_table_name}.{fk.ref_column_name}' 中未找到值 '{child_value}'",
                    constraint_name=fk.constraint_name,
                    table=table_name,
                    column=fk.column_name
                )

    def validate_update_foreign_keys(self, table_name: str, old_row: Dict[str, Any], new_row: Dict[str, Any]):
        """
        验证更新操作的外键约束

        Args:
            table_name: 要更新的表名
            old_row: 原始行数据
            new_row: 更新后行数据

        Raises:
            ForeignKeyValidationError: 外键约束违反
        """
        # 获取表的所有外键约束
        foreign_keys = self.constraint_mgr.get_table_foreign_keys(table_name)

        for fk in foreign_keys:
            old_value = old_row.get(fk.column_name)
            new_value = new_row.get(fk.column_name)

            # 如果外键列的值没有变化，跳过检查
            if old_value == new_value:
                continue

            # NULL值允许
            if new_value is None:
                continue

            # 检查新值在父表中是否存在
            if not self._parent_key_exists(fk, new_value):
                ref_table_name = self._get_table_name_by_id(fk.ref_table_id)
                raise ForeignKeyValidationError(
                    f"外键约束违反: 在父表 '{ref_table_name}.{fk.ref_column_name}' 中未找到值 '{new_value}'",
                    constraint_name=fk.constraint_name,
                    table=table_name,
                    column=fk.column_name
                )

    def validate_delete_referenced_keys(self, table_name: str, row_data: Dict[str, Any]):
        """
        验证删除操作 - 检查是否被其他表引用（RESTRICT语义）

        Args:
            table_name: 要删除记录的表名
            row_data: 要删除的行数据

        Raises:
            ForeignKeyValidationError: 存在引用，不允许删除
        """
        # 获取所有引用此表的外键约束
        referencing_fks = self.constraint_mgr.get_referencing_foreign_keys(table_name)

        for fk in referencing_fks:
            ref_value = row_data.get(fk.ref_column_name)

            # 如果被删除的值为NULL，无需检查引用
            if ref_value is None:
                continue

            # 检查是否有子表记录引用此值
            if self._child_key_exists(fk, ref_value):
                child_table_name = self._get_table_name_by_id(fk.table_id)
                raise ForeignKeyValidationError(
                    f"外键约束违反: 无法删除记录，子表 '{child_table_name}.{fk.column_name}' 中存在引用值 '{ref_value}'",
                    constraint_name=fk.constraint_name,
                    table=table_name,
                    column=fk.ref_column_name
                )

    def validate_update_referenced_keys(self, table_name: str, old_row: Dict[str, Any], new_row: Dict[str, Any]):
        """
        验证更新父表被引用键的操作

        Args:
            table_name: 要更新的表名（父表）
            old_row: 原始行数据
            new_row: 更新后行数据

        Raises:
            ForeignKeyValidationError: 存在引用，不允许更新被引用的键
        """
        # 获取所有引用此表的外键约束
        referencing_fks = self.constraint_mgr.get_referencing_foreign_keys(table_name)

        for fk in referencing_fks:
            old_value = old_row.get(fk.ref_column_name)
            new_value = new_row.get(fk.ref_column_name)

            # 如果被引用的列值没有变化，跳过检查
            if old_value == new_value:
                continue

            # 如果原值为NULL，无需检查引用
            if old_value is None:
                continue

            # 检查是否有子表记录引用原值
            if self._child_key_exists(fk, old_value):
                child_table_name = self._get_table_name_by_id(fk.table_id)
                raise ForeignKeyValidationError(
                    f"外键约束违反: 无法更新被引用键，子表 '{child_table_name}.{fk.column_name}' 中存在引用值 '{old_value}'",
                    constraint_name=fk.constraint_name,
                    table=table_name,
                    column=fk.ref_column_name
                )

    def _parent_key_exists(self, fk: ForeignKeyConstraint, value: Any) -> bool:
        """检查父表中是否存在指定值"""
        try:
            ref_table_name = self._get_table_name_by_id(fk.ref_table_id)
            if not ref_table_name:
                return False

            # 在父表中查找匹配值
            for row in self.storage_engine.seq_scan(ref_table_name):
                if row.get(fk.ref_column_name) == value:
                    return True

            return False

        except Exception as e:
            print(f"检查父键存在性失败: {e}")
            return False

    def _child_key_exists(self, fk: ForeignKeyConstraint, value: Any) -> bool:
        """检查子表中是否存在引用指定值的记录"""
        try:
            child_table_name = self._get_table_name_by_id(fk.table_id)
            if not child_table_name:
                return False

            # 在子表中查找引用此值的记录
            for row in self.storage_engine.seq_scan(child_table_name):
                if row.get(fk.column_name) == value:
                    return True

            return False

        except Exception as e:
            print(f"检查子键存在性失败: {e}")
            return False

    def _get_table_name_by_id(self, table_id: int) -> Optional[str]:
        """根据table_id获取表名"""
        for table_name in self.catalog_mgr.list_all_tables():
            meta = self.catalog_mgr.get_table_metadata(table_name)
            if meta and meta.table_id == table_id:
                return table_name

        # 检查系统表
        for table_name in [self.catalog_mgr.SYS_TABLES, self.catalog_mgr.SYS_COLUMNS, self.catalog_mgr.SYS_INDEXES]:
            meta = self.catalog_mgr.get_table_metadata(table_name)
            if meta and meta.table_id == table_id:
                return table_name

        return None

    def get_constraint_info(self, table_name: str) -> Dict[str, Any]:
        """获取表的约束信息汇总"""
        foreign_keys = self.constraint_mgr.get_table_foreign_keys(table_name)
        referencing_keys = self.constraint_mgr.get_referencing_foreign_keys(table_name)

        return {
            "foreign_keys": [
                {
                    "constraint_name": fk.constraint_name,
                    "column": fk.column_name,
                    "ref_table": self._get_table_name_by_id(fk.ref_table_id),
                    "ref_column": fk.ref_column_name
                }
                for fk in foreign_keys
            ],
            "referenced_by": [
                {
                    "constraint_name": fk.constraint_name,
                    "child_table": self._get_table_name_by_id(fk.table_id),
                    "child_column": fk.column_name,
                    "ref_column": fk.ref_column_name
                }
                for fk in referencing_keys
            ]
        }