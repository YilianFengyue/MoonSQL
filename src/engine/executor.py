# 文件路径: MoonSQL/src/engine/executor.py

"""
Executor - SQL执行引擎

【功能说明】
- 解释SQL编译器生成的Plan JSON
- 实现五大基础算子：CreateTable, Insert, SeqScan, Filter, Project
- 调用StorageEngine执行具体的数据库操作
- 支持算子组合和流水线处理

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


class ExecutionError(Exception):
    """执行错误"""
    pass


class Operator(ABC):
    """算子基类"""

    def __init__(self, plan: Dict[str, Any]):
        self.plan = plan
        self.children = []

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

        if not table_name:
            raise ExecutionError("CREATE TABLE: 缺少表名")
        if not columns:
            raise ExecutionError("CREATE TABLE: 缺少列定义")

        # ★ 规范化列定义：兼容 Planner 输出的 "VARCHAR(50)"
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
                # 其他类型直接传递（INT/INTEGER 等）
                col_def["type"] = raw_type

            normalized.append(col_def)

        try:
            storage_engine.create_table(table_name, normalized)  # ★ 用 normalized
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
            if columns:
                # 指定了列名: INSERT INTO table(col1,col2) VALUES (val1,val2)
                if len(columns) != len(values):
                    raise ExecutionError(f"列数不匹配: {len(columns)} vs {len(values)}")
                row_data = dict(zip(columns, values))
            else:
                # 未指定列名: INSERT INTO table VALUES (val1,val2,...)
                schema_columns = [col.name for col in table_info.schema.columns]
                if len(values) != len(schema_columns):
                    raise ExecutionError(f"值数量不匹配表列数: {len(values)} vs {len(schema_columns)}")
                row_data = dict(zip(schema_columns, values))

            # 执行插入
            success = storage_engine.insert_row(table_name, row_data)
            if success:
                yield {"status": "success", "message": f"插入成功", "affected_rows": 1}
            else:
                raise ExecutionError("插入失败")

        except Exception as e:
            raise ExecutionError(f"INSERT失败: {e}")


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

    """过滤算子"""

    def __init__(self, plan: Dict[str, Any]):
        super().__init__(plan)
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

    def __init__(self, plan: Dict[str, Any]):
        super().__init__(plan)
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

    def __init__(self, plan: Dict[str, Any]):
        super().__init__(plan)
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

            if cond:
                def predicate(row):
                    return FilterOperator({"condition": cond})._evaluate_condition(row, cond)

                deleted_count = storage_engine.delete_where(table_name, predicate)
            else:
                deleted_count = storage_engine.delete_where(table_name, lambda row: True)

            yield {"status": "success", "message": f"删除成功", "affected_rows": deleted_count}

        except Exception as e:
            raise ExecutionError(f"DELETE失败: {e}")

    def _evaluate_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """评估删除条件(复用Filter的逻辑)"""
        filter_op = FilterOperator({"condition": condition})
        return filter_op._evaluate_condition(row, condition)


class Executor:
    """SQL执行引擎"""

    def __init__(self, storage_engine):
        self.storage_engine = storage_engine

        # 算子工厂
        self.operator_classes = {
            'CreateTable': CreateTableOperator,
            'Insert': InsertOperator,
            'SeqScan': SeqScanOperator,
            'Filter': FilterOperator,
            'Project': ProjectOperator,
            'Delete': DeleteOperator
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
        operator = operator_class(plan)

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


def run_all_executor_tests():
    """运行所有执行引擎测试"""
    print("Executor 执行引擎测试")
    print("=" * 60)

    remaining_rows = test_executor_basic()
    test_complex_conditions()

    print("\n" + "=" * 60)
    print(f"执行引擎测试完成! 最终表剩余{remaining_rows}行记录")


if __name__ == "__main__":
    run_all_executor_tests()