# 文件路径: MoonSQL/src/engine/executor.py

"""
Executor - SQL执行引擎（含 DDL: SHOW/DESC/ALTER）

【功能说明】
- 解释SQL编译器生成的Plan JSON
- 实现五大基础算子：CreateTable, Insert, SeqScan, Filter, Project, Delete
- 新增DDL算子：ShowTables, Desc, AlterTable
- 调用StorageEngine执行具体的数据库操作
- 支持算子组合和流水线处理
- 集成CatalogManager进行元数据管理

【设计架构】
SQL编译器 → Plan JSON → Executor → StorageEngine → 数据

【Plan JSON格式】
{
  "op": "Project",
  "columns": ["id", "name"],
  "child": {
    "op": "Filter",
    "condition": {"type": "compare", "left": "age", "op": ">", "right": 18},
    "child": {
      "op": "SeqScan",
      "table": "students"
    }
  }
}
"""

import re
from typing import Dict, List, Any, Iterator, Optional, Union, Callable
from abc import ABC, abstractmethod

from storage import storage_engine


class ExecutionError(Exception):
    """执行错误"""
    pass


class Operator(ABC):
    """算子基类"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        self.plan = plan
        self.children = []
        self.catalog_mgr = catalog_mgr

    @abstractmethod
    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行算子，返回结果迭代器"""
        pass

    def add_child(self, child: 'Operator'):
        """添加子算子"""
        self.children.append(child)


class CreateTableOperator(Operator):
    """CREATE TABLE 算子"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        table_name = self.plan.get('table')
        columns = self.plan.get('columns', [])
        table_constraints = self.plan.get('table_constraints', [])  # ★ 新增：获取表级约束

        if not table_name:
            raise ExecutionError("CREATE TABLE: 缺少表名")
        if not columns:
            raise ExecutionError("CREATE TABLE: 缺少列定义")

        # ★ 修改：规范化列定义，保留约束信息
        normalized = []
        for col in columns:
            name = col.get("name")
            raw_type = str(col.get("type", "")).upper()
            col_def = {"name": name}

            if raw_type.startswith("VARCHAR("):
                m = re.match(r"VARCHAR\((\d+)\)", raw_type)
                if not m:
                    raise ExecutionError(f"CREATE TABLE: 无效的VARCHAR定义: {raw_type}")
                col_def["type"] = "VARCHAR"
                col_def["max_length"] = int(m.group(1))
            else:
                col_def["type"] = raw_type

            # ★ 新增：处理约束信息
            if "constraints" in col:
                col_def["constraints"] = col["constraints"]

            normalized.append(col_def)

        try:
            storage_engine.create_table(table_name, normalized)
            # ★ 修改：传递约束信息到catalog
            if self.catalog_mgr:
                self.catalog_mgr.register_table(table_name, normalized)

            # ★ 修复：处理外键约束 - 正确访问对象属性
            for fk_constraint in table_constraints:
                try:
                    self.catalog_mgr.add_foreign_key(
                        table_name=table_name,
                        column_name=fk_constraint.column_name,  # ★ 修复：对象属性访问
                        ref_table_name=fk_constraint.ref_table,  # ★ 修复：对象属性访问
                        ref_column_name=fk_constraint.ref_column,  # ★ 修复：对象属性访问
                        constraint_name=fk_constraint.constraint_name  # ★ 修复：对象属性访问
                    )
                    print(f"★ 外键约束已添加: {fk_constraint.constraint_name or 'auto_generated'}")
                except Exception as e:
                    raise ExecutionError(f"外键约束创建失败: {e}")
            yield {"status": "success", "message": f"表 {table_name} 创建成功"}
        except Exception as e:
            raise ExecutionError(f"CREATE TABLE失败: {e}")


class InsertOperator(Operator):
    """INSERT 算子"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行插入操作"""
        table_name = self.plan.get('table')
        # 替换为：
        raw_values = self.plan.get('values', [])
        # ★ 兼容 Planner 的对象数组：提取真实值
        values = []
        for v in raw_values:
            if isinstance(v, dict) and 'value' in v:
                values.append(v['value'])
            else:
                values.append(v)

        columns = self.plan.get('columns')  # 可选，指定列名

        if not table_name:
            raise ExecutionError("INSERT: 缺少表名")

        if not values:
            raise ExecutionError("INSERT: 缺少插入值")

        try:
            # 获取表信息
            table_info = storage_engine.get_table_info(table_name)
            if not table_info:
                raise ExecutionError(f"表不存在: {table_name}")

            # 构造行数据
            # 构造行数据（支持DEFAULT）
            if columns:
                # 指定了列名
                if len(columns) != len(values):
                    raise ExecutionError(f"列数不匹配: {len(columns)} vs {len(values)}")
                row_data = dict(zip(columns, values))

                # ★ 新增：为未指定的列填入DEFAULT值
                if self.catalog_mgr:
                    all_columns = self.catalog_mgr.get_table_columns(table_name)
                    for col in all_columns:
                        if col.column_name not in row_data:
                            if col.constraints.has_default:
                                row_data[col.column_name] = col.constraints.default_value
                            else:
                                row_data[col.column_name] = None
            else:
                # 未指定列名：按schema顺序填充
                schema_columns = [col.name for col in table_info.schema.columns]
                if len(values) != len(schema_columns):
                    raise ExecutionError(f"值数量不匹配表列数: {len(values)} vs {len(schema_columns)}")
                row_data = dict(zip(schema_columns, values))
            # 执行插入前的约束检查
            if self.catalog_mgr:
                self._check_constraints(table_name, row_data, storage_engine)
                # ★ 新增：外键约束检查
                try:
                    self.catalog_mgr.validate_foreign_key_constraints("INSERT", table_name, row_data)
                except Exception as e:
                    raise ExecutionError(f"外键约束违反: {e}")
            # 执行插入
            success = storage_engine.insert_row(table_name, row_data)
            if success:
                # ★ 可选：更新行数统计
                if self.catalog_mgr:
                    self.catalog_mgr.update_table_row_count(table_name, +1)
                yield {"status": "success", "message": f"插入成功", "affected_rows": 1}
            else:
                raise ExecutionError("插入失败")

        except Exception as e:
            raise ExecutionError(f"INSERT失败: {e}")

    def _check_constraints(self, table_name: str, row_data: Dict[str, Any], storage_engine):
        """检查约束"""
        columns = self.catalog_mgr.get_table_columns(table_name)

        for col in columns:
            col_name = col.column_name
            value = row_data.get(col_name)
            constraints = col.constraints

            # NOT NULL检查
            if constraints.not_null and value is None:
                raise ExecutionError(f"列 '{col_name}' 不能为NULL")

            # UNIQUE/PRIMARY KEY检查 - ★ 修复：只在非NULL值时检查
            if (constraints.unique or constraints.primary_key) and value is not None:
                # ★ 修复：调用storage_engine而不是未定义的变量
                if self._check_unique_value(table_name, col_name, value, storage_engine):
                    constraint_type = "主键" if constraints.primary_key else "唯一约束"
                    raise ExecutionError(f"{constraint_type}冲突，列'{col_name}'的值'{value}'已存在")

    def _check_unique_value(self, table_name: str, column_name: str, value: Any, storage_engine) -> bool:
        """检查唯一值冲突"""
        try:
            for row in storage_engine.seq_scan(table_name):
                if row.get(column_name) == value:
                    return True
            return False
        except:
            return False



class SeqScanOperator(Operator):
    """顺序扫描算子"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行全表扫描"""
        table_name = self.plan.get('table')

        if not table_name:
            raise ExecutionError("SeqScan: 缺少表名")

        try:
            for row in storage_engine.seq_scan(table_name):
                yield row

        except Exception as e:
            raise ExecutionError(f"SeqScan失败: {e}")


class FilterOperator(Operator):
    """过滤算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        self.condition = plan.get('condition')

        # ★ 新增：兼容 Planner 的 predicate（字符串）
        if not self.condition:
            pred = plan.get('predicate')
            if isinstance(pred, str):
                self.condition = self._parse_predicate_string(pred)

        if not self.condition:
            raise ExecutionError("Filter: 缺少过滤条件")

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行过滤操作"""
        if not self.children:
            raise ExecutionError("Filter: 缺少子算子")

        # 获取子算子的结果
        child_results = self.children[0].execute(storage_engine)

        # 应用过滤条件
        for row in child_results:
            if self._evaluate_condition(row, self.condition):
                yield row

    def _evaluate_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """评估过滤条件"""
        cond_type = condition.get('type')

        if cond_type == 'compare':
            # 比较条件: {"type": "compare", "left": "age", "op": ">", "right": 18}
            left_val = self._get_value(row, condition['left'])
            right_val = self._get_value(row, condition['right'])
            op = condition['op']

            return self._compare_values(left_val, right_val, op)

        elif cond_type == 'and':
            # AND条件: {"type": "and", "left": cond1, "right": cond2}
            left_result = self._evaluate_condition(row, condition['left'])
            right_result = self._evaluate_condition(row, condition['right'])
            return left_result and right_result

        elif cond_type == 'or':
            # OR条件: {"type": "or", "left": cond1, "right": cond2}
            left_result = self._evaluate_condition(row, condition['left'])
            right_result = self._evaluate_condition(row, condition['right'])
            return left_result or right_result

        elif cond_type == 'not':
            # NOT条件: {"type": "not", "condition": cond}
            inner_result = self._evaluate_condition(row, condition['condition'])
            return not inner_result

        else:
            raise ExecutionError(f"不支持的条件类型: {cond_type}")

    def _get_value(self, row: Dict[str, Any], ref) -> Any:
        """获取值：可能是列名或常量"""
        if isinstance(ref, str):
            # 列名
            return row.get(ref)
        else:
            # 常量
            return ref

    def _parse_predicate_string(self, pred: str):
        """
        ★ 将 'col op value' 形式的字符串解析为 condition 字典
          支持: =, ==, !=, <>, <, <=, >, >=, LIKE
          覆盖 WHERE age > 20 / name = 'Alice' 这类简单场景
        """
        s = pred.strip()
        m = re.match(r"^\s*([A-Za-z_]\w*)\s*(=|==|!=|<>|<=|>=|<|>|LIKE)\s*(.+?)\s*$", s, re.IGNORECASE)
        if not m:
            return None
        col, op, right = m.groups()
        right = right.strip()
        # 去引号/尝试转数字
        if (right.startswith("'") and right.endswith("'")) or (right.startswith('"') and right.endswith('"')):
            rv = right[1:-1]
        else:
            try:
                rv = int(right)
            except ValueError:
                try:
                    rv = float(right)
                except ValueError:
                    rv = right
        return {"type": "compare", "left": col, "op": op.upper(), "right": rv}

    def _compare_values(self, left: Any, right: Any, op: str) -> bool:
        """比较两个值"""
        # 处理NULL值
        if left is None or right is None:
            if op in ['=', '==']:
                return left is None and right is None
            elif op in ['!=', '<>', '≠']:
                return not (left is None and right is None)
            else:
                return False  # NULL与任何值比较大小都返回False

        # 类型转换
        try:
            if isinstance(left, str) and isinstance(right, (int, float)):
                left = float(left) if '.' in left else int(left)
            elif isinstance(right, str) and isinstance(left, (int, float)):
                right = float(right) if '.' in right else int(right)
        except ValueError:
            pass  # 转换失败，按原类型比较

        # 执行比较
        if op in ['=', '==']:
            return left == right
        elif op in ['!=', '<>', '≠']:
            return left != right
        elif op == '<':
            return left < right
        elif op == '<=':
            return left <= right
        elif op == '>':
            return left > right
        elif op == '>=':
            return left >= right
        elif op.upper() == 'LIKE':
            return self._like_match(str(left), str(right))
        else:
            raise ExecutionError(f"不支持的比较操作符: {op}")

    def _like_match(self, text: str, pattern: str) -> bool:
        """LIKE模式匹配"""
        # 简单实现：将SQL的LIKE转为Python正则
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        return re.match(f"^{regex_pattern}$", text, re.IGNORECASE) is not None


class ProjectOperator(Operator):
    """投影算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        self.columns = plan.get('columns', [])
        if not self.columns:
            raise ExecutionError("Project: 缺少投影列")

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行投影操作"""
        if not self.children:
            raise ExecutionError("Project: 缺少子算子")

        # 获取子算子的结果
        child_results = self.children[0].execute(storage_engine)

        # 投影指定列
        for row in child_results:
            projected_row = {}
            for col in self.columns:
                if col == '*':
                    # SELECT * 展开所有列
                    projected_row.update(row)
                else:
                    # 具体列名
                    projected_row[col] = row.get(col)
            yield projected_row


class DeleteOperator(Operator):
    """DELETE 算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        # 可能没有直接给 condition，先记录；不要抛错
        self.condition = plan.get('condition')
        # 不在这里解析/报错，执行时会从 child Filter 兜底

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行删除操作"""
        table_name = self.plan.get('table')

        if not table_name:
            raise ExecutionError("DELETE: 缺少表名")

        try:
            cond = self.condition
            if cond is None and self.children:
                child = self.children[0]
                if isinstance(child, FilterOperator):
                    cond = child.condition

            # ★ 新增：删除前的外键约束检查
            if self.catalog_mgr:
                # 先扫描要删除的行，检查外键约束
                for row in storage_engine.seq_scan(table_name):
                    should_delete = False
                    if cond:
                        should_delete = FilterOperator({"condition": cond})._evaluate_condition(row, cond)
                    else:
                        should_delete = True

                    if should_delete:
                        try:
                            self.catalog_mgr.validate_foreign_key_constraints("DELETE", table_name, row)
                        except Exception as e:
                            raise ExecutionError(f"外键约束违反: {e}")

            # ★ 原有删除逻辑保持不变
            if cond:
                def predicate(row):
                    return FilterOperator({"condition": cond})._evaluate_condition(row, cond)

                deleted_count = storage_engine.delete_where(table_name, predicate)
            else:
                deleted_count = storage_engine.delete_where(table_name, lambda row: True)

            # 可选：更新行数统计
            if self.catalog_mgr and deleted_count:
                self.catalog_mgr.update_table_row_count(table_name, -deleted_count)

            yield {"status": "success", "message": f"删除成功", "affected_rows": deleted_count}

        except Exception as e:
            raise ExecutionError(f"DELETE失败: {e}")

    def _evaluate_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """评估删除条件(复用Filter的逻辑)"""
        filter_op = FilterOperator({"condition": condition})
        return filter_op._evaluate_condition(row, condition)


class UpdateOperator(Operator):
    """UPDATE 算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        self.set_dict = plan.get('set', {})
        self.condition = plan.get('condition')

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行更新操作"""
        table_name = self.plan.get('table')

        if not table_name:
            raise ExecutionError("UPDATE: 缺少表名")
        if not self.set_dict:
            raise ExecutionError("UPDATE: 缺少SET子句")

        try:
            cond = self.condition
            if cond is None and self.children:
                child = self.children[0]
                if isinstance(child, FilterOperator):
                    cond = child.condition

                # ★ 修改：构造更新函数，加入约束检查

            def update_func(row):
                updated_row = dict(row)
                for column, value_info in self.set_dict.items():
                    updated_row[column] = value_info['value']

                # ★ 新增：外键约束检查
                if self.catalog_mgr:
                    try:
                        self.catalog_mgr.validate_foreign_key_constraints("UPDATE", table_name, updated_row, row)
                    except Exception as e:
                        raise ExecutionError(f"外键约束违反: {e}")

                return updated_row

            if cond:
                def predicate(row):
                    return FilterOperator({"condition": cond})._evaluate_condition(row, cond)

                updated_count = storage_engine.update_where(table_name, predicate, update_func)
            else:
                # 全表更新
                updated_count = storage_engine.update_where(table_name, lambda row: True, update_func)

            yield {"status": "success", "message": f"更新成功", "affected_rows": updated_count}

        except Exception as e:
            raise ExecutionError(f"UPDATE失败: {e}")

# -------------------- 新增：DDL算子 --------------------

class ShowTablesOperator(Operator):
    """SHOW TABLES 算子：返回一列 table"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        try:
            if self.catalog_mgr:
                names = self.catalog_mgr.list_all_tables()
            else:
                # 兜底（不排系统表）
                names = storage_engine.list_tables()
            for n in names:
                yield {"table": n}
        except Exception as e:
            raise ExecutionError(f"SHOW TABLES失败: {e}")


class DescOperator(Operator):
    """DESC 表结构：每行一个列定义"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        table = self.plan.get("table")
        if not table:
            raise ExecutionError("DESC: 缺少表名")
        try:
            if not self.catalog_mgr:
                raise ExecutionError("DESC 需要 CatalogManager 支持")
            schema = self.catalog_mgr.get_schema_info(table)
            if not schema:
                raise ExecutionError(f"表不存在: {table}")
            for col in schema["columns"]:
                yield {
                    "Field": col["name"],
                    "Type": f"{col['type']}" + (f"({col['max_length']})" if col.get("max_length") else ""),
                    "Position": col["position"]
                }
        except Exception as e:
            raise ExecutionError(f"DESC失败: {e}")


class AlterTableOperator(Operator):
    """ALTER TABLE 算子：通过"重写法"实现"""

    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        table = self.plan.get("table")
        action = self.plan.get("action")
        payload = self.plan.get("payload", {}) or {}

        if not table or not action:
            raise ExecutionError("ALTER: 缺少参数")

        if not self.catalog_mgr:
            raise ExecutionError("ALTER 需要 CatalogManager 支持")

        # 读取当前列定义
        schema = self.catalog_mgr.get_schema_info(table)
        if not schema:
            raise ExecutionError(f"表不存在: {table}")

        old_cols = schema["columns"]  # list of dict{name,type,max_length,position}
        # 规范化为简洁 [{name, type, max_length}]
        cols_norm = [{"name": c["name"], "type": c["type"], "max_length": c.get("max_length")} for c in old_cols]

        # 分派动作
        if action == "RENAME":
            new_name = payload.get("new_name")
            if not new_name:
                raise ExecutionError("RENAME: 缺少 new_name")

            self._rewrite_table(storage_engine, table, cols_norm, new_name,
                                lambda row: row)  # 逐行原样复制
            # 更新目录：旧表注销，新表登记
            self.catalog_mgr.unregister_table(table)
            self.catalog_mgr.register_table(new_name, cols_norm)
            # 行数回填
            row_count = 0
            for _ in storage_engine.seq_scan(new_name):
                row_count += 1
            if row_count:
                self.catalog_mgr.update_table_row_count(new_name, row_count)
            yield {"status": "success", "message": f"表已重命名为 {new_name}"}
            return

        # 组装新列定义
        def type_to_dict(t: str) -> Dict[str, Any]:
            t = (t or "").upper()
            if t.startswith("VARCHAR("):
                m = re.match(r"VARCHAR\((\d+)\)", t)
                if not m:
                    raise ExecutionError(f"无效的类型: {t}")
                return {"type": "VARCHAR", "max_length": int(m.group(1))}
            return {"type": t, "max_length": None}

        if action == "ADD_COLUMN":
            name = payload.get("name")
            t = payload.get("type")
            if not name or not t:
                raise ExecutionError("ADD COLUMN: 缺少 name 或 type")
            new_cols = cols_norm + [{"name": name, **type_to_dict(t)}]

            def mapper(row):
                new_row = dict(row)
                new_row[name] = None  # 默认 NULL
                return new_row

            self._rewrite_table(storage_engine, table, new_cols, table, mapper)
            # 目录：先注销再登记（保持位置序号）
            self.catalog_mgr.unregister_table(table)
            self.catalog_mgr.register_table(table, new_cols)
            # 行数保持
            self._sync_row_count(table, storage_engine)
            yield {"status": "success", "message": f"已添加列 {name}"}
            return

        if action == "DROP_COLUMN":
            name = payload.get("name")
            if not name:
                raise ExecutionError("DROP COLUMN: 缺少 name")
            exists = any(c["name"] == name for c in cols_norm)
            if not exists:
                raise ExecutionError(f"列不存在: {name}")
            new_cols = [c for c in cols_norm if c["name"] != name]

            def mapper(row):
                new_row = dict(row)
                new_row.pop(name, None)
                return new_row

            self._rewrite_table(storage_engine, table, new_cols, table, mapper)
            self.catalog_mgr.unregister_table(table)
            self.catalog_mgr.register_table(table, new_cols)
            self._sync_row_count(table, storage_engine)
            yield {"status": "success", "message": f"已删除列 {name}"}
            return

        if action == "MODIFY_COLUMN":
            name = payload.get("name")
            t = payload.get("type")
            if not name or not t:
                raise ExecutionError("MODIFY COLUMN: 缺少 name 或 type")
            found = False
            new_cols = []
            for c in cols_norm:
                if c["name"] == name:
                    new_cols.append({"name": name, **type_to_dict(t)})
                    found = True
                else:
                    new_cols.append(c)
            if not found:
                raise ExecutionError(f"列不存在: {name}")

            def mapper(row):
                # 数据不强转，保持原值
                return dict(row)

            self._rewrite_table(storage_engine, table, new_cols, table, mapper)
            self.catalog_mgr.unregister_table(table)
            self.catalog_mgr.register_table(table, new_cols)
            self._sync_row_count(table, storage_engine)
            yield {"status": "success", "message": f"已修改列 {name} 类型为 {t}"}
            return

        if action == "CHANGE_COLUMN":
            old_name = payload.get("old_name")
            new_name = payload.get("new_name")
            t = payload.get("type")
            if not old_name or not new_name or not t:
                raise ExecutionError("CHANGE COLUMN: 缺少 old_name/new_name/type")
            exists = any(c["name"] == old_name for c in cols_norm)
            if not exists:
                raise ExecutionError(f"列不存在: {old_name}")
            new_cols = []
            for c in cols_norm:
                if c["name"] == old_name:
                    new_cols.append({"name": new_name, **type_to_dict(t)})
                else:
                    new_cols.append(c)

            def mapper(row):
                new_row = dict(row)
                if old_name in new_row:
                    new_row[new_name] = new_row.pop(old_name)
                else:
                    new_row[new_name] = None
                return new_row

            self._rewrite_table(storage_engine, table, new_cols, table, mapper)
            self.catalog_mgr.unregister_table(table)
            self.catalog_mgr.register_table(table, new_cols)
            self._sync_row_count(table, storage_engine)
            yield {"status": "success", "message": f"已将列 {old_name} 重命名为 {new_name} 并修改类型为 {t}"}
            return

        raise ExecutionError(f"不支持的 ALTER 操作: {action}")

    # ---------- 工具函数 ----------

    def _rewrite_table(self, storage_engine, src_table: str,
                       target_cols: List[Dict[str, Any]],
                       dest_table: str,
                       row_mapper: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """
        重写法：通过一个临时表搬运数据，从 src_table -> 临时表 -> （可选重建）dest_table
        """
        tmp = f"__alter_tmp_{src_table}"
        # 1) 建临时表（按新列定义）
        storage_engine.create_table(tmp, target_cols)
        # 2) 复制并映射
        for row in storage_engine.seq_scan(src_table):
            storage_engine.insert_row(tmp, row_mapper(row))
        # 3) 删除目标表（如果目标名与源相同则先删源）
        if dest_table == src_table:
            storage_engine.drop_table(src_table)
            # 重新创建 dest_table
            storage_engine.create_table(dest_table, target_cols)
            # 从 tmp 回填到 dest
            for row in storage_engine.seq_scan(tmp):
                storage_engine.insert_row(dest_table, row)
            # 清理 tmp
            storage_engine.drop_table(tmp)
        else:
            # 目标不同名（RENAME）
            storage_engine.create_table(dest_table, target_cols)
            for row in storage_engine.seq_scan(tmp):
                storage_engine.insert_row(dest_table, row)
            storage_engine.drop_table(tmp)
            storage_engine.drop_table(src_table)

    def _sync_row_count(self, table: str, storage_engine):
        cnt = 0
        for _ in storage_engine.seq_scan(table):
            cnt += 1
        if self.catalog_mgr:
            # 因为 register_table 初始 row_count=0，所以直接 +cnt
            self.catalog_mgr.update_table_row_count(table, cnt)


class Executor:
    """SQL执行引擎（支持 SHOW/DESC/ALTER）"""

    def __init__(self, storage_engine, catalog_mgr=None):
        self.storage_engine = storage_engine
        self.catalog_mgr = catalog_mgr

        # 算子工厂
        self.operator_classes = {
            'CreateTable': CreateTableOperator,
            'Insert': InsertOperator,
            'SeqScan': SeqScanOperator,
            'Filter': FilterOperator,
            'Project': ProjectOperator,
            'Delete': DeleteOperator,
            'Update': UpdateOperator,
            # ★ 新增DDL算子
            'ShowTables': ShowTablesOperator,
            'Desc': DescOperator,
            'AlterTable': AlterTableOperator,
        }

    def execute(self, plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        执行计划

        Args:
            plan: 执行计划JSON

        Yields:
            执行结果行
        """
        try:
            # 构建算子树
            root_operator = self._build_operator_tree(plan)

            # 执行并返回结果
            for result in root_operator.execute(self.storage_engine):
                yield result

        except Exception as e:
            raise ExecutionError(f"执行失败: {e}")

    def _build_operator_tree(self, plan: Dict[str, Any]) -> Operator:
        """构建算子树"""
        op_type = plan.get('op')
        if not op_type:
            raise ExecutionError("计划缺少操作类型")

        if op_type not in self.operator_classes:
            raise ExecutionError(f"不支持的操作类型: {op_type}")

        # 创建算子
        operator_class = self.operator_classes[op_type]
        operator = operator_class(plan, catalog_mgr=self.catalog_mgr)

        # 递归构建子算子
        child_plan = plan.get('child')
        if child_plan:
            child_operator = self._build_operator_tree(child_plan)
            operator.add_child(child_operator)

        return operator

    def execute_simple(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """执行计划并返回完整结果列表(便于测试)"""
        return list(self.execute(plan))


# ==================== 测试代码 ====================

def test_executor_basic():
    """测试执行引擎基本功能"""
    print("=== Executor 基本功能测试 ===")

    # 导入存储引擎
    try:
        from ..storage.storage_engine import StorageEngine
    except ImportError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
        from src.storage.storage_engine import StorageEngine

    # 创建存储引擎和执行器
    storage = StorageEngine("test_executor_data", buffer_capacity=4)
    executor = Executor(storage)

    # 清理测试表
    if "test_exec" in storage.list_tables():
        storage.drop_table("test_exec")

    print("\n1. 测试CREATE TABLE:")
    create_plan = {
        "op": "CreateTable",
        "table": "test_exec",
        "columns": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR", "max_length": 30},
            {"name": "score", "type": "INT"}
        ]
    }

    results = executor.execute_simple(create_plan)
    for result in results:
        print(f"   {result}")

    print("\n2. 测试INSERT:")
    test_students = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
        {"id": 3, "name": "Charlie", "score": 92},
        {"id": 4, "name": "Diana", "score": 76},
        {"id": 5, "name": "Eve", "score": 89}
    ]

    for student in test_students:
        insert_plan = {
            "op": "Insert",
            "table": "test_exec",
            "values": [student["id"], student["name"], student["score"]]
        }
        results = executor.execute_simple(insert_plan)
        for result in results:
            print(f"   插入{student['name']}: {result}")

    print("\n3. 测试SeqScan:")
    seqscan_plan = {
        "op": "SeqScan",
        "table": "test_exec"
    }

    results = executor.execute_simple(seqscan_plan)
    print(f"   全表扫描结果({len(results)}条):")
    for result in results:
        print(f"     {result}")

    print("\n4. 测试Filter (score > 85):")
    filter_plan = {
        "op": "Filter",
        "condition": {
            "type": "compare",
            "left": "score",
            "op": ">",
            "right": 85
        },
        "child": {
            "op": "SeqScan",
            "table": "test_exec"
        }
    }

    results = executor.execute_simple(filter_plan)
    print(f"   过滤结果({len(results)}条):")
    for result in results:
        print(f"     {result}")

    print("\n5. 测试Project (只选择id和name):")
    project_plan = {
        "op": "Project",
        "columns": ["id", "name"],
        "child": {
            "op": "Filter",
            "condition": {
                "type": "compare",
                "left": "score",
                "op": ">=",
                "right": 90
            },
            "child": {
                "op": "SeqScan",
                "table": "test_exec"
            }
        }
    }

    results = executor.execute_simple(project_plan)
    print(f"   投影结果({len(results)}条):")
    for result in results:
        print(f"     {result}")

    print("\n6. 测试Delete (删除score < 80):")
    delete_plan = {
        "op": "Delete",
        "table": "test_exec",
        "condition": {
            "type": "compare",
            "left": "score",
            "op": "<",
            "right": 80
        }
    }

    results = executor.execute_simple(delete_plan)
    for result in results:
        print(f"   {result}")

    # 验证删除结果
    print("\n   删除后的表内容:")
    final_results = executor.execute_simple(seqscan_plan)
    for result in final_results:
        print(f"     {result}")

    # 清理
    storage.close()

    return len(final_results)


def test_complex_conditions():
    """测试复杂条件"""
    print("\n=== 复杂条件测试 ===")

    try:
        from ..storage.storage_engine import StorageEngine
    except ImportError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
        from src.storage.storage_engine import StorageEngine

    storage = StorageEngine("test_complex_data", buffer_capacity=4)
    executor = Executor(storage)

    # 创建测试表
    if "employees" in storage.list_tables():
        storage.drop_table("employees")

    create_plan = {
        "op": "CreateTable",
        "table": "employees",
        "columns": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR", "max_length": 30},
            {"name": "department", "type": "VARCHAR", "max_length": 20},
            {"name": "salary", "type": "INT"}
        ]
    }
    executor.execute_simple(create_plan)

    # 插入测试数据
    employees = [
        [1, "Alice", "Engineering", 75000],
        [2, "Bob", "Sales", 65000],
        [3, "Charlie", "Engineering", 80000],
        [4, "Diana", "Marketing", 70000],
        [5, "Eve", "Sales", 68000]
    ]

    for emp in employees:
        insert_plan = {
            "op": "Insert",
            "table": "employees",
            "values": emp
        }
        executor.execute_simple(insert_plan)

    print("1. 测试AND条件 (Engineering部门 AND 薪水>70000):")
    and_plan = {
        "op": "Project",
        "columns": ["name", "department", "salary"],
        "child": {
            "op": "Filter",
            "condition": {
                "type": "and",
                "left": {
                    "type": "compare",
                    "left": "department",
                    "op": "=",
                    "right": "Engineering"
                },
                "right": {
                    "type": "compare",
                    "left": "salary",
                    "op": ">",
                    "right": 70000
                }
            },
            "child": {
                "op": "SeqScan",
                "table": "employees"
            }
        }
    }

    results = executor.execute_simple(and_plan)
    for result in results:
        print(f"   {result}")

    print("\n2. 测试OR条件 (Sales部门 OR 薪水>75000):")
    or_plan = {
        "op": "Filter",
        "condition": {
            "type": "or",
            "left": {
                "type": "compare",
                "left": "department",
                "op": "=",
                "right": "Sales"
            },
            "right": {
                "type": "compare",
                "left": "salary",
                "op": ">",
                "right": 75000
            }
        },
        "child": {
            "op": "SeqScan",
            "table": "employees"
        }
    }

    results = executor.execute_simple(or_plan)
    print(f"   OR条件结果({len(results)}条):")
    for result in results:
        print(f"     {result}")

    storage.close()


def test_ddl_operations():
    """测试DDL操作（需要CatalogManager）"""
    print("\n=== DDL操作测试 ===")

    try:
        from ..storage.storage_engine import StorageEngine
    except ImportError:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
        from src.storage.storage_engine import StorageEngine

    # 尝试导入CatalogManager
    try:
        from ..catalog.catalog_manager import CatalogManager
        catalog_mgr = CatalogManager("test_catalog_data")
        print("   找到CatalogManager，启用完整DDL测试")
    except ImportError:
        catalog_mgr = None
        print("   未找到CatalogManager，跳过DDL测试")
        return

    storage = StorageEngine("test_ddl_data", buffer_capacity=4)
    executor = Executor(storage, catalog_mgr)

    # 测试SHOW TABLES
    print("\n1. 测试SHOW TABLES:")
    show_plan = {"op": "ShowTables"}
    results = executor.execute_simple(show_plan)
    for result in results:
        print(f"   {result}")

    # 创建测试表
    if catalog_mgr and "test_ddl" in catalog_mgr.list_all_tables():
        storage.drop_table("test_ddl")
        catalog_mgr.unregister_table("test_ddl")

    create_plan = {
        "op": "CreateTable",
        "table": "test_ddl",
        "columns": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR", "max_length": 50}
        ]
    }
    executor.execute_simple(create_plan)

    # 测试DESC
    print("\n2. 测试DESC:")
    desc_plan = {"op": "Desc", "table": "test_ddl"}
    results = executor.execute_simple(desc_plan)
    for result in results:
        print(f"   {result}")

    print("\n   DDL功能测试完成")
    storage.close()
    if catalog_mgr:
        catalog_mgr.close()


def run_all_executor_tests():
    """运行所有执行引擎测试"""
    print("Executor 执行引擎测试")
    print("=" * 60)

    remaining_rows = test_executor_basic()
    test_complex_conditions()
    test_ddl_operations()

    print("\n" + "=" * 60)
    print(f"执行引擎测试完成! 最终表剩余{remaining_rows}行记录")


if __name__ == "__main__":
    run_all_executor_tests()