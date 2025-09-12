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
        BinaryOpNode, WhereClauseNode, ParseError
    )
    from sql.semantic import SemanticAnalyzer, Catalog, SemanticError
    from sql.lexer import SqlError
else:
    from .parser import (
        Parser, ASTNode, CreateTableNode, InsertNode, SelectNode,
        DeleteNode, ColumnDefNode, ValueNode, ColumnNode,
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
        else:
            raise PlanError(ast.line, ast.col,
                            f"Unsupported statement type for planning: {type(ast).__name__}")

    def _plan_create_table(self, node: CreateTableNode) -> Dict[str, Any]:
        """生成CREATE TABLE执行计划"""
        columns = []
        for col_def in node.columns:
            columns.append({
                "name": col_def.name,
                "type": col_def.data_type
            })

        return {
            "op": "CreateTable",
            "table": node.table_name,
            "columns": columns,
            "estimated_cost": 1.0,
            "description": f"Create table '{node.table_name}' with {len(columns)} columns"
        }

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

    def _plan_select(self, node: SelectNode) -> Dict[str, Any]:
        """生成SELECT执行计划"""
        # 从底层开始构建计划树

        # 1. 基础扫描算子
        base_plan = {
            "op": "SeqScan",
            "table": node.table_name,
            "estimated_cost": 10.0,
            "estimated_rows": 100,
            "description": f"Sequential scan on table '{node.table_name}'"
        }

        current_plan = base_plan

        # 2. 添加Filter算子（如果有WHERE子句）
        if node.where_clause:
            filter_plan = {
                "op": "Filter",
                "predicate": self._serialize_condition(node.where_clause.condition),
                "estimated_cost": current_plan["estimated_cost"] + 5.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                "description": f"Filter rows with condition",
                "child": current_plan
            }
            current_plan = filter_plan

        # 3. 添加Project算子（列投影）
        if not (len(node.columns) == 1 and node.columns[0] == "*"):
            # 不是SELECT *，需要投影
            columns = []
            for col in node.columns:
                if isinstance(col, ColumnNode):
                    columns.append(col.name)
                elif isinstance(col, str):
                    columns.append(col)

            project_plan = {
                "op": "Project",
                "columns": columns,
                "estimated_cost": current_plan["estimated_cost"] + 1.0,
                "estimated_rows": current_plan["estimated_rows"],
                "description": f"Project {len(columns)} columns",
                "child": current_plan
            }
            current_plan = project_plan

        return current_plan

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

    def _serialize_condition(self, condition: ASTNode) -> str:
        """将条件表达式序列化为字符串"""
        if isinstance(condition, BinaryOpNode):
            left_str = self._serialize_expression(condition.left)
            right_str = self._serialize_expression(condition.right)
            return f"{left_str} {condition.operator} {right_str}"
        else:
            return self._serialize_expression(condition)

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


if __name__ == "__main__":
    test_planner()