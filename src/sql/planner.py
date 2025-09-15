"""
SQL执行计划生成器 - A4阶段核心实现
将AST转换为JSON格式的执行计划

【设计原理】
1. 算子树：执行计划是算子的树形组合
2. 五个基础算子：CreateTable/Insert/SeqScan/Filter/Project
3. JSON格式：标准化的计划表示，便于执行器解释
4. 语义依赖：基于A3的语义分析结果生成计划

【算子设计】
- CreateTable: 创建表算子
- Insert: 插入数据算子
- SeqScan: 顺序扫描算子
- Filter: 条件过滤算子
- Project: 列投影算子

【计划树示例】
SELECT id, name FROM student WHERE age > 18;
=>
Project(cols=["id","name"])
└── Filter(pred="age > 18")
    └── SeqScan(table="student")

【JSON格式】
{
  "op": "Project",
  "cols": ["id", "name"],
  "child": {
    "op": "Filter",
    "pred": "age > 18",
    "child": {
      "op": "SeqScan",
      "table": "student"
    }
  }
}
"""

import sys
import json
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

# 导入依赖
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from sql.parser import (
        Parser, ASTNode, CreateTableNode, InsertNode, SelectNode,
        DeleteNode, ColumnDefNode, ValueNode, ColumnNode,
        BinaryOpNode, WhereClauseNode, ParseError, UpdateNode
)
    from sql.semantic import SemanticAnalyzer, Catalog, SemanticError
    from sql.lexer import SqlError
else:
    from .parser import (
        Parser, ASTNode, CreateTableNode, InsertNode, SelectNode,
        DeleteNode, UpdateNode, ColumnDefNode, ValueNode, ColumnNode,
        BinaryOpNode, WhereClauseNode, ParseError
    )
    from .semantic import SemanticAnalyzer, Catalog, SemanticError
    from .lexer import SqlError


class PlanError(SqlError):
    """计划生成错误"""

    def __init__(self, line: int, col: int, hint: str):
        super().__init__("PlanError", line, col, hint)


class ExecutionPlan:
    """执行计划封装"""

    def __init__(self, plan_dict: Dict[str, Any]):
        self.plan = plan_dict

    def to_json(self, indent: int = 2) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.plan, indent=indent, ensure_ascii=False)

    def to_dict(self) -> Dict[str, Any]:
        """获取计划字典"""
        return self.plan

    def get_operator(self) -> str:
        """获取根算子类型"""
        return self.plan.get("op", "Unknown")


class Planner:
    """执行计划生成器"""

    def __init__(self, catalog: Catalog = None):
        self.catalog = catalog if catalog else Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)

    def plan(self, sql_text: str) -> ExecutionPlan:
        """
        生成执行计划主函数
        Args:
            sql_text: SQL语句文本
        Returns:
            ExecutionPlan: 执行计划对象
        Raises:
            PlanError: 计划生成错误
        """
        try:
            # 1. 语法分析
            parser = Parser()
            ast = parser.parse(sql_text)

            # 2. 语义分析（可选，用于验证）
            # semantic_result = self.semantic_analyzer.analyze(ast)

            # 3. 生成执行计划
            plan_dict = self._generate_plan(ast)

            return ExecutionPlan(plan_dict)

        except (ParseError, SemanticError) as e:
            raise PlanError(e.line, e.col, f"Cannot generate plan: {e.hint}")
        except Exception as e:
            raise PlanError(0, 0, f"Plan generation error: {str(e)}")

    def _generate_plan(self, ast: ASTNode) -> Dict[str, Any]:
        """根据AST生成执行计划"""
        if isinstance(ast, CreateTableNode):
            return self._plan_create_table(ast)
        elif isinstance(ast, InsertNode):
            return self._plan_insert(ast)
        elif isinstance(ast, SelectNode):
            return self._plan_select(ast)
        elif isinstance(ast, DeleteNode):
            return self._plan_delete(ast)
        # ★ 新增：
        elif ast.__class__.__name__ == "ShowTablesNode":
            return self._plan_show_tables()
        elif ast.__class__.__name__ == "DescTableNode":
            return self._plan_desc(ast)
        elif ast.__class__.__name__ == "AlterTableNode":
            return self._plan_alter_table(ast)
        elif isinstance(ast, UpdateNode):
            return self._plan_update(ast)
        else:
            raise PlanError(ast.line, ast.col,
                            f"Unsupported statement type for planning: {type(ast).__name__}")

    def _plan_create_table(self, node: CreateTableNode) -> Dict[str, Any]:
        """生成CREATE TABLE执行计划"""
        columns = []
        for col_def in node.columns:
            # 保留约束信息
            column_info = {
                "name": col_def.name,
                "type": col_def.data_type
            }

            # 传递约束信息到Plan
            if col_def.constraints:
                column_info["constraints"] = col_def.constraints

            columns.append(column_info)

        # ★ 修复：添加table_constraints到计划
        plan = {
            "op": "CreateTable",
            "table": node.table_name,
            "columns": columns,
            "estimated_cost": 1.0,
            "description": f"Create table '{node.table_name}' with {len(columns)} columns"
        }

        # ★ 新增：传递表级约束
        if hasattr(node, 'table_constraints') and node.table_constraints:
            plan["table_constraints"] = node.table_constraints
            print(f"★ PLANNER: 传递了 {len(node.table_constraints)} 个外键约束")

        return plan

    def _plan_update(self, node) -> Dict[str, Any]:
        """生成UPDATE执行计划"""
        # 构造SET字典
        set_dict = {}
        for clause in node.set_clauses:
            column = clause["column"]
            value_node = clause["value"]
            set_dict[column] = {
                "value": value_node.value,
                "type": value_node.value_type
            }

        # 基础计划：扫描表
        base_plan = {
            "op": "SeqScan",
            "table": node.table_name,
            "estimated_cost": 10.0,
            "estimated_rows": 100,
            "description": f"Sequential scan on table '{node.table_name}'"
        }

        current_plan = base_plan

        # 如果有WHERE条件，添加Filter
        if node.where_clause:
            filter_plan = {
                "op": "Filter",
                "predicate": self._serialize_condition(node.where_clause.condition),
                "estimated_cost": current_plan["estimated_cost"] + 5.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                "description": "Filter rows to update",
                "child": current_plan
            }
            current_plan = filter_plan

        # 最终的Update算子
        update_plan = {
            "op": "Update",
            "table": node.table_name,
            "set": set_dict,
            "estimated_cost": current_plan["estimated_cost"] + 2.0,
            "estimated_rows": current_plan["estimated_rows"],
            "description": f"Update rows in table '{node.table_name}'",
            "child": current_plan
        }

        return update_plan
    def _plan_show_tables(self) -> Dict[str, Any]:
        return {
            "op": "ShowTables",
            "description": "List user tables"
        }

    def _plan_desc(self, node) -> Dict[str, Any]:
        return {
            "op": "Desc",
            "table": node.table_name,
            "description": f"Describe table '{node.table_name}'"
        }

    def _plan_alter_table(self, node) -> Dict[str, Any]:
        plan = {
            "op": "AlterTable",
            "table": node.table_name,
            "action": node.action,
            "payload": node.payload,
            "description": f"Alter table '{node.table_name}' with action {node.action}"
        }
        return plan

    def _plan_insert(self, node: InsertNode) -> Dict[str, Any]:
        """生成INSERT执行计划"""
        values = []
        for value_node in node.values:
            values.append({
                "value": value_node.value,
                "type": value_node.value_type
            })

        plan = {
            "op": "Insert",
            "table": node.table_name,
            "values": values,
            "estimated_cost": 1.0,
            "estimated_rows": 1,
            "description": f"Insert 1 row into table '{node.table_name}'"
        }

        # 如果指定了列名
        if node.columns:
            plan["columns"] = node.columns

        return plan

    def _plan_select(self, node) -> Dict[str, Any]:
        """★ 完整替换：生成SELECT执行计划（支持DISTINCT和别名）"""
        # 检查节点类型兼容性
        if hasattr(node, 'table_name'):
            table_name = node.table_name
        else:
            table_name = getattr(node, 'table_name', 'unknown')

        # 从底层开始构建计划树

        # 1. 基础扫描算子
        base_plan = {
            "op": "SeqScan",
            "table": table_name,
            "estimated_cost": 10.0,
            "estimated_rows": 100,
            "description": f"Sequential scan on table '{table_name}'"
        }

        current_plan = base_plan

        # 2. 添加Filter算子（如果有WHERE子句）
        if hasattr(node, 'where_clause') and node.where_clause:
            filter_plan = {
                "op": "Filter",
                "condition": self._convert_condition_to_dict(node.where_clause.condition),  # ★ 修改：使用新转换器
                "estimated_cost": current_plan["estimated_cost"] + 5.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                "description": "Filter rows with complex condition",
                "child": current_plan
            }
            current_plan = filter_plan

        # 3. 确定是否需要投影和去重
        has_distinct = getattr(node, 'distinct', False)
        needs_projection = not (len(node.columns) == 1 and node.columns[0] == "*")

        # ★ 新增：根据情况选择算子组合策略
        if has_distinct and needs_projection:
            # 策略1：先投影再去重（适合大部分情况）
            project_plan = {
                "op": "Project",
                "columns": self._convert_columns_to_plan_format(node.columns),  # ★ 新方法
                "estimated_cost": current_plan["estimated_cost"] + 1.0,
                "estimated_rows": current_plan["estimated_rows"],
                "description": f"Project columns with aliases",
                "child": current_plan
            }

            distinct_plan = {
                "op": "Distinct",
                "estimated_cost": project_plan["estimated_cost"] + 3.0,
                "estimated_rows": max(1, project_plan["estimated_rows"] // 3),
                "description": "Remove duplicate rows",
                "child": project_plan
            }
            current_plan = distinct_plan

        elif has_distinct and not needs_projection:
            # 策略2：直接去重（SELECT DISTINCT *）
            distinct_plan = {
                "op": "Distinct",
                "estimated_cost": current_plan["estimated_cost"] + 3.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 3),
                "description": "Remove duplicate rows from all columns",
                "child": current_plan
            }
            current_plan = distinct_plan

        elif not has_distinct and needs_projection:
            # 策略3：仅投影
            project_plan = {
                "op": "Project",
                "columns": self._convert_columns_to_plan_format(node.columns),
                "estimated_cost": current_plan["estimated_cost"] + 1.0,
                "estimated_rows": current_plan["estimated_rows"],
                "description": f"Project specified columns",
                "child": current_plan
            }
            current_plan = project_plan

        # 策略4：无投影无去重（SELECT *），current_plan保持不变

        return current_plan

    def _convert_columns_to_plan_format(self, columns: List) -> List[Dict[str, Any]]:
        """★ 新增：将列定义转换为计划格式（支持别名）"""
        plan_columns = []

        for col in columns:
            if col == "*":
                plan_columns.append("*")
            elif isinstance(col, str):
                # 简单列名
                plan_columns.append(col)
            elif hasattr(col, '__class__'):
                # AST节点
                if col.__class__.__name__ == "ColumnNode":
                    plan_columns.append(col.name)
                elif col.__class__.__name__ == "AliasColumnNode":
                    # ★ 别名列
                    plan_columns.append({
                        "name": col.column_name,
                        "alias": col.alias
                    })
            else:
                # 兜底处理
                plan_columns.append(str(col))

        return plan_columns

    def _convert_condition_to_dict(self, condition_node) -> Dict[str, Any]:
        """★ 完整替换：将复杂条件AST转换为执行器格式"""
        if not condition_node:
            return {}

        # 获取节点类型名称
        if hasattr(condition_node, '__class__'):
            node_type = condition_node.__class__.__name__
        else:
            # 向后兼容：可能已经是字典格式
            if isinstance(condition_node, dict):
                return condition_node
            else:
                return {"type": "compare", "left": "unknown", "op": "=", "right": None}

        # ★ 根据节点类型进行转换
        if node_type == "BinaryOpNode":
            return {
                "type": "compare",
                "left": self._extract_expression_value(condition_node.left),
                "op": condition_node.operator,
                "right": self._extract_expression_value(condition_node.right)
            }

        elif node_type == "LogicalOpNode":
            return {
                "type": condition_node.operator.lower(),  # "AND" -> "and"
                "left": self._convert_condition_to_dict(condition_node.left),
                "right": self._convert_condition_to_dict(condition_node.right)
            }

        elif node_type == "NotNode":
            return {
                "type": "not",
                "condition": self._convert_condition_to_dict(condition_node.expr)
            }

        elif node_type == "LikeNode":
            return {
                "type": "like",
                "left": self._extract_expression_value(condition_node.left),
                "right": self._extract_expression_value(condition_node.pattern)
            }

        elif node_type == "InNode":
            result = {
                "type": "in",
                "left": self._extract_expression_value(condition_node.left)
            }

            if hasattr(condition_node, 'subquery') and condition_node.subquery:
                # 子查询
                result["subquery"] = self._generate_plan(condition_node.subquery)
            else:
                # 常量列表
                result["values"] = [self._extract_expression_value(v) for v in condition_node.values]

            return result

        elif node_type == "BetweenNode":
            return {
                "type": "between",
                "left": self._extract_expression_value(condition_node.expr),
                "min": self._extract_expression_value(condition_node.min_val),
                "max": self._extract_expression_value(condition_node.max_val)
            }

        elif node_type == "IsNullNode":
            return {
                "type": "is_null",
                "left": self._extract_expression_value(condition_node.expr),
                "is_null": not condition_node.is_not  # is_not=True表示IS NOT NULL
            }

        else:
            # 兜底：尝试向后兼容的转换
            return self._serialize_condition_legacy(condition_node)

    def _extract_expression_value(self, expr_node) -> Any:
        """★ 新增：从表达式节点提取值"""
        if not expr_node:
            return None

        if hasattr(expr_node, '__class__'):
            node_type = expr_node.__class__.__name__

            if node_type == "ColumnNode":
                return expr_node.name
            elif node_type == "ValueNode":
                return expr_node.value
            elif node_type == "AliasColumnNode":
                return expr_node.column_name  # 使用原列名，不是别名

        # 如果是基本类型，直接返回
        if isinstance(expr_node, (str, int, float, bool)) or expr_node is None:
            return expr_node

        # 兜底
        return str(expr_node)

    def _serialize_condition_legacy(self, condition) -> Dict[str, Any]:
        """★ 保留：向后兼容的条件序列化"""
        # 这是原有的_serialize_condition方法，用于兜底
        try:
            if hasattr(condition, '__class__') and condition.__class__.__name__ == "BinaryOpNode":
                left_str = self._serialize_expression(condition.left)
                right_str = self._serialize_expression(condition.right)
                return {
                    "type": "compare",
                    "left": left_str,
                    "op": condition.operator,
                    "right": right_str
                }
            else:
                return {
                    "type": "compare",
                    "left": "unknown",
                    "op": "=",
                    "right": None
                }
        except:
            return {"type": "compare", "left": "unknown", "op": "=", "right": None}

    def _plan_delete(self, node: DeleteNode) -> Dict[str, Any]:
        """生成DELETE执行计划"""
        # DELETE通常需要先找到要删除的行，然后删除

        # 基础计划：扫描表
        base_plan = {
            "op": "SeqScan",
            "table": node.table_name,
            "estimated_cost": 10.0,
            "estimated_rows": 100,
            "description": f"Sequential scan on table '{node.table_name}'"
        }

        current_plan = base_plan

        # 如果有WHERE条件，添加Filter
        if node.where_clause:
            filter_plan = {
                "op": "Filter",
                "predicate": self._serialize_condition(node.where_clause.condition),
                "estimated_cost": current_plan["estimated_cost"] + 5.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                "description": "Filter rows to delete",
                "child": current_plan
            }
            current_plan = filter_plan

        # 最终的Delete算子
        delete_plan = {
            "op": "Delete",
            "table": node.table_name,
            "estimated_cost": current_plan["estimated_cost"] + 2.0,
            "estimated_rows": current_plan["estimated_rows"],
            "description": f"Delete rows from table '{node.table_name}'",
            "child": current_plan
        }

        return delete_plan

    def _serialize_condition(self, condition) -> str:
        """向后兼容：条件序列化为字符串（已弃用，保留接口）"""
        # 将字典格式的条件转换为简单字符串表示
        if isinstance(condition, dict):
            cond_type = condition.get("type")
            if cond_type == "compare":
                left = condition.get("left", "")
                op = condition.get("op", "=")
                right = condition.get("right", "")
                if isinstance(right, str):
                    return f"{left} {op} '{right}'"
                else:
                    return f"{left} {op} {right}"

        # 原有逻辑的兜底
        try:
            if hasattr(condition, '__class__') and condition.__class__.__name__ == "BinaryOpNode":
                left_str = self._serialize_expression(condition.left)
                right_str = self._serialize_expression(condition.right)
                return f"{left_str} {condition.operator} {right_str}"
            else:
                return str(condition)
        except:
            return "true"

    def _serialize_expression(self, expr: ASTNode) -> str:
        """将表达式序列化为字符串"""
        if isinstance(expr, ColumnNode):
            return expr.name
        elif isinstance(expr, ValueNode):
            if expr.value_type == "STRING":
                return f"'{expr.value}'"
            else:
                return str(expr.value)
        else:
            return str(expr)


def format_execution_plan(plan: ExecutionPlan, indent: int = 0) -> str:
    """格式化执行计划为树形字符串"""
    plan_dict = plan.to_dict()
    return _format_plan_dict(plan_dict, indent)


def _format_plan_dict(plan_dict: Dict[str, Any], indent: int = 0) -> str:
    """递归格式化计划字典"""
    prefix = "  " * indent
    lines = []

    # 算子名称和基本信息
    op = plan_dict.get("op", "Unknown")
    lines.append(f"{prefix}{op}")

    # 显示关键属性
    for key, value in plan_dict.items():
        if key in ["child", "op"]:
            continue
        elif key == "description":
            lines.append(f"{prefix}├─ 描述: {value}")
        elif key == "estimated_cost":
            lines.append(f"{prefix}├─ 预估代价: {value}")
        elif key == "estimated_rows":
            lines.append(f"{prefix}├─ 预估行数: {value}")
        elif isinstance(value, (str, int, float)):
            lines.append(f"{prefix}├─ {key}: {value}")
        elif isinstance(value, list):
            lines.append(f"{prefix}├─ {key}: {value}")

    # 递归显示子节点
    if "child" in plan_dict:
        lines.append(f"{prefix}└─ 子计划:")
        lines.append(_format_plan_dict(plan_dict["child"], indent + 1))

    return "\n".join(lines)


def plan_sql(sql_text: str, catalog: Catalog = None) -> ExecutionPlan:
    """
    便捷函数：直接从SQL生成执行计划
    Args:
        sql_text: SQL语句
        catalog: 系统目录（可选）
    Returns:
        ExecutionPlan: 执行计划
    """
    planner = Planner(catalog)
    return planner.plan(sql_text)


def test_planner():
    """测试执行计划生成器"""
    print("=== Testing SQL Planner (A4) ===")

    # 创建测试catalog
    catalog = Catalog()
    planner = Planner(catalog)

    # 先创建表（为后续测试做准备）
    try:
        catalog.create_table("student", [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "age", "type": "INT"}
        ])
        catalog.create_table("teacher", [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"}
        ])
        print("✓ 测试catalog准备完成")
    except Exception as e:
        print(f"❌ Catalog准备失败: {e}")
        return

    test_cases = [
        # 基本语句
        ("CREATE TABLE test(id INT, name VARCHAR);", "CREATE TABLE计划"),
        ("INSERT INTO student VALUES(1, 'Alice', 20);", "INSERT计划"),
        ("SELECT * FROM student;", "简单SELECT计划"),
        ("SELECT id, name FROM student;", "列投影SELECT计划"),
        ("SELECT * FROM student WHERE age > 18;", "带条件SELECT计划"),
        ("SELECT id, name FROM student WHERE age > 18;", "复杂SELECT计划"),
        ("DELETE FROM student WHERE id = 1;", "DELETE计划"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print("✓ 计划生成成功")
            print("=== 计划树 ===")
            print(format_execution_plan(plan))
            print("=== JSON格式 ===")
            print(plan.to_json())
        except (PlanError, ParseError, SemanticError) as e:
            print(f"❌ {e.error_type}: {e.hint}")
        except Exception as e:
            print(f"❌ 意外错误: {e}")


def test_s5_planner_features():
    """测试S5计划生成功能"""
    print("=== S5 Planner功能测试 ===")

    from sql.semantic import Catalog

    # 创建测试catalog
    catalog = Catalog()
    catalog.create_table("users", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "age", "type": "INT"},
        {"name": "email", "type": "VARCHAR"}
    ])

    planner = Planner(catalog)

    test_cases = [
        # DISTINCT测试
        ("SELECT DISTINCT name FROM users;", "DISTINCT单列"),
        ("SELECT DISTINCT id, name FROM users;", "DISTINCT多列"),
        ("SELECT DISTINCT * FROM users;", "DISTINCT全部列"),

        # 别名测试
        ("SELECT id AS user_id FROM users;", "AS别名"),
        ("SELECT id user_id FROM users;", "隐式别名"),
        ("SELECT id AS user_id, name AS username FROM users;", "多列别名"),

        # 复杂WHERE测试
        ("SELECT * FROM users WHERE age > 18 AND name LIKE 'A%';", "AND + LIKE"),
        ("SELECT * FROM users WHERE age IN (18, 19, 20);", "IN常量"),
        ("SELECT * FROM users WHERE age BETWEEN 18 AND 65;", "BETWEEN"),
        ("SELECT * FROM users WHERE email IS NULL;", "IS NULL"),
        ("SELECT * FROM users WHERE age > 25 OR name = 'Admin';", "OR逻辑"),
        ("SELECT * FROM users WHERE NOT (age < 18);", "NOT逻辑"),

        # 组合功能
        ("SELECT DISTINCT name AS username FROM users WHERE age > 18;", "DISTINCT+别名+WHERE"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print("✓ 计划生成成功")

            plan_dict = plan.to_dict()

            # 检查关键特性
            if plan_dict.get("op") == "Distinct":
                print("   特性: 包含DISTINCT算子")

            if plan_dict.get("op") == "Project" or (
                    plan_dict.get("child", {}).get("op") == "Project"
            ):
                project_node = plan_dict if plan_dict.get("op") == "Project" else plan_dict.get("child", {})
                columns = project_node.get("columns", [])
                for col in columns:
                    if isinstance(col, dict) and "alias" in col:
                        print(f"   特性: 别名 {col['name']} AS {col['alias']}")

            # 检查过滤条件
            def check_filter(node):
                if isinstance(node, dict):
                    if node.get("op") == "Filter":
                        condition = node.get("condition", {})
                        cond_type = condition.get("type", "unknown")
                        print(f"   特性: 过滤条件类型 {cond_type}")

                    child = node.get("child")
                    if child:
                        check_filter(child)

            check_filter(plan_dict)

        except Exception as e:
            print(f"❌ 计划生成失败: {e}")


def test_condition_conversion():
    """测试条件转换功能"""
    print("\n=== 条件转换测试 ===")

    planner = Planner()

    # 模拟AST节点
    class MockBinaryOpNode:
        def __init__(self, left, op, right):
            self.left = left
            self.operator = op
            self.right = right
            self.__class__.__name__ = "BinaryOpNode"

    class MockColumnNode:
        def __init__(self, name):
            self.name = name
            self.__class__.__name__ = "ColumnNode"

    class MockValueNode:
        def __init__(self, value):
            self.value = value
            self.__class__.__name__ = "ValueNode"

    # 测试条件转换
    condition_ast = MockBinaryOpNode(
        MockColumnNode("age"),
        ">",
        MockValueNode(18)
    )

    condition_dict = planner._convert_condition_to_dict(condition_ast)
    print(f"AST转换结果: {condition_dict}")

    expected = {
        "type": "compare",
        "left": "age",
        "op": ">",
        "right": 18
    }

    print(f"转换正确性: {condition_dict == expected}")\

if __name__ == "__main__":
    test_s5_planner_features()
    test_condition_conversion()