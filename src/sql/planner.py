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
        """生成完整SELECT执行计划（修复 HAVING）"""
        # 表名
        table_name = node.table_name if hasattr(node, 'table_name') else getattr(node, 'table_name', 'unknown')

        # 1) SeqScan
        base_plan = {
            "op": "SeqScan",
            "table": table_name,
            "estimated_cost": 10.0,
            "estimated_rows": 100,
            "description": f"Sequential scan on table '{table_name}'"
        }
        current_plan = base_plan

        # 2) WHERE（分组前）
        if getattr(node, 'where_clause', None):
            current_plan = {
                "op": "Filter",
                "condition": self._convert_condition_to_dict(node.where_clause.condition),
                "estimated_cost": current_plan["estimated_cost"] + 5.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                "description": "Filter rows before aggregation",
                "child": current_plan
            }

        # 3) 聚合（来自 SELECT 或 GROUP BY 或 HAVING）
        has_group_by = getattr(node, 'group_by', None) is not None

        # 关键：收集 SELECT + HAVING 的所有聚合
        agg_map = self._collect_aggs_and_aliases(node)
        has_any_agg = (len(agg_map) > 0)

        if has_any_agg or has_group_by:
            # 3.1 生成 GroupAggregate（包含 HAVING 里用到的聚合）
            agg_plan = self._generate_group_aggregate_plan(node, current_plan, agg_map)
            current_plan = agg_plan

            # 3.2 HAVING：把聚合函数改写成聚合结果列名，再作为普通 Filter 放在聚合之后
            if getattr(node, 'having', None):
                # 先把 HAVING 的聚合节点改写为别名列
                group_keys = node.group_by.columns if getattr(node, 'group_by', None) else []
                self._validate_having_against_group_keys(node.having.condition, group_keys)  # 先对“原始 HAVING AST”做校验
                rewritten = self._rewrite_having_to_columns(node.having.condition, agg_map)  # 再把聚合改写成别名列
                having_cond = self._convert_condition_to_dict(rewritten)

                current_plan = {
                    "op": "Filter",
                    "condition": having_cond,
                    "estimated_cost": current_plan["estimated_cost"] + 2.0,
                    "estimated_rows": max(1, current_plan["estimated_rows"] // 2),
                    "description": "HAVING filter after aggregation",
                    "child": current_plan
                }

        # 4) 投影（SELECT 列表）
        needs_projection = self._needs_projection(node, has_any_agg)
        if needs_projection:
            project_plan = {
                "op": "Project",
                "columns": self._convert_columns_to_plan_format(node.columns, has_any_agg),
                "estimated_cost": current_plan["estimated_cost"] + 1.0,
                "estimated_rows": current_plan["estimated_rows"],
                "description": "Project SELECT columns",
                "child": current_plan
            }
            current_plan = project_plan

        # 5) DISTINCT
        if getattr(node, 'distinct', False):
            current_plan = {
                "op": "Distinct",
                "estimated_cost": current_plan["estimated_cost"] + 3.0,
                "estimated_rows": max(1, current_plan["estimated_rows"] // 3),
                "description": "Remove duplicate rows",
                "child": current_plan
            }

        # 6) ORDER BY
        if getattr(node, 'order_by', None):
            current_plan = self._generate_sort_plan(node.order_by, current_plan)

        # 7) LIMIT
        if getattr(node, 'limit', None):
            current_plan = self._generate_limit_plan(node.limit, current_plan)

        # 8) 结束（分号已在 parser 处理）
        return current_plan

    def _has_aggregate_functions(self, columns: List) -> bool:
        """★ 新增：检查SELECT列中是否包含聚合函数"""
        for col in columns:
            if hasattr(col, '__class__') and col.__class__.__name__ == "AggregateFuncNode":
                return True
        return False

    def _needs_projection(self, node, has_aggregates: bool) -> bool:
        """★ 新增：判断是否需要投影算子"""
        # SELECT * 且无聚合：不需要投影
        if (len(node.columns) == 1 and node.columns[0] == "*" and not has_aggregates):
            return False

        # 有别名、聚合函数、或非*选择：需要投影
        return True

    def _generate_group_aggregate_plan(self, node, child_plan: Dict[str, Any], agg_map: Dict[tuple, str]) -> Dict[
        str, Any]:
        """生成分组聚合计划：既包含 SELECT 里的聚合，也包含仅出现在 HAVING 的聚合"""
        # 分组键
        group_keys = node.group_by.columns if getattr(node, 'group_by', None) else []

        aggregates: List[Dict[str, Any]] = []
        used = set()

        # 先把 SELECT 列里的聚合放进去（优先使用显式别名）
        for col in node.columns:
            if hasattr(col, '__class__') and col.__class__.__name__ == "AggregateFuncNode":
                k = (col.func_name.upper(), col.column)
                if k in used:
                    continue
                alias = col.alias or agg_map.get(k)  # 与收集表一致
                used.add(k)
                aggregates.append({"func": k[0], "column": k[1], "alias": alias})

        # 再补上 HAVING 里出现但 SELECT 未出现的聚合
        for (func, col), alias in agg_map.items():
            if (func, col) not in used:
                aggregates.append({"func": func, "column": col, "alias": alias})

        if not aggregates:
            # 有 GROUP BY 时，理论上必须要有至少一个聚合；无 GROUP BY 也可以是全局聚合
            raise ValueError("GROUP BY requires at least one aggregate function in SELECT or HAVING")

        # 语义检查：非聚合列必须在 GROUP BY 中（你已有的函数可复用）
        self._validate_grouping_semantics(node.columns, group_keys)

        return {
            "op": "GroupAggregate",
            "group_keys": group_keys,
            "aggregates": aggregates,
            "estimated_cost": child_plan["estimated_cost"] + 8.0,
            "estimated_rows": max(1, child_plan["estimated_rows"] // (len(group_keys) or 1)),
            "description": f"Group by {group_keys} with {len(aggregates)} aggregates" if group_keys else "Global aggregation",
            "child": child_plan
        }



    # ======== 【新增】聚合收集/重写/校验工具 ========

    def _collect_aggs_and_aliases(self, node) -> Dict[tuple, str]:
        """
        收集 SELECT 和 HAVING 中出现的聚合，返回 {(FUNC, col): alias}
        """
        agg_map: Dict[tuple, str] = {}

        def default_alias(func: str, col: str) -> str:
            c = col if col != "*" else "star"
            return f"{func.lower()}_{c}"

        def see(func: str, col: str, alias: Optional[str]):
            k = (func.upper(), col)
            if k not in agg_map:
                agg_map[k] = alias or default_alias(func, col)

        # 1) 扫描 SELECT 列
        for c in node.columns:
            t = getattr(c, "__class__", None)
            tname = t.__name__ if t else ""
            if tname == "AggregateFuncNode":
                see(c.func_name, c.column, c.alias)

        # 2) 扫描 HAVING（如有）
        def walk(expr):
            # 原生类型 / None 直接返回
            if expr is None or isinstance(expr, (int, float, str, bool)):
                return
            if not hasattr(expr, "__class__"):
                return

            name = expr.__class__.__name__
            if name == "AggregateFuncNode":
                see(expr.func_name, expr.column, getattr(expr, "alias", None))
                return

            # 安全地遍历子属性：只在有 __dict__ 时访问
            d = getattr(expr, "__dict__", None)
            if not d:
                return
            for v in d.values():
                if isinstance(v, list):
                    for e in v:
                        walk(e)
                else:
                    walk(v)

        if getattr(node, "having", None):
            walk(node.having.condition)

        return agg_map

    def _rewrite_having_to_columns(self, expr, agg_map):
        """
        把 HAVING 表达式中的 AggregateFuncNode 改写为 ColumnNode(alias)
        """
        # 原生类型 / None：直接返回
        if expr is None or isinstance(expr, (int, float, str, bool)):
            return expr
        if not hasattr(expr, "__class__"):
            return expr

        name = expr.__class__.__name__
        if name == "AggregateFuncNode":
            key = (expr.func_name.upper(), expr.column)
            if key not in agg_map:
                raise ValueError(f"HAVING uses aggregate {key} that was not collected")
            alias = agg_map[key]
            # 用 ColumnNode(alias) 替换
            return ColumnNode(alias)

        # 安全递归：只有有 __dict__ 才遍历属性
        d = getattr(expr, "__dict__", None)
        if not d:
            return expr

        for k, v in list(d.items()):
            if isinstance(v, list):
                d[k] = [self._rewrite_having_to_columns(x, agg_map) for x in v]
            else:
                d[k] = self._rewrite_having_to_columns(v, agg_map)
        return expr

    def _validate_having_against_group_keys(self, expr, group_keys: List[str]):
        """
        HAVING 里裸用的列（ColumnNode）必须出现在 GROUP BY 列里
        """
        if expr is None or isinstance(expr, (int, float, str, bool)):
            return
        if not hasattr(expr, "__class__"):
            return

        name = expr.__class__.__name__
        if name == "ColumnNode":
            if expr.name not in group_keys:
                raise PlanError(getattr(expr, "line", 0), getattr(expr, "col", 0),
                                f"Column '{expr.name}' must appear in GROUP BY")
            return

        d = getattr(expr, "__dict__", None)
        if not d:
            return
        for v in d.values():
            if isinstance(v, list):
                for e in v:
                    self._validate_having_against_group_keys(e, group_keys)
            else:
                self._validate_having_against_group_keys(v, group_keys)

    def _validate_grouping_semantics(self, columns: List, group_keys: List[str]):
        """★ 新增：验证分组语义：非聚合列必须在GROUP BY中"""
        for col in columns:
            if col == "*":
                if group_keys:
                    # GROUP BY存在时，不允许SELECT *
                    raise ValueError("Cannot use SELECT * with GROUP BY")
                continue

            # 跳过聚合函数
            if hasattr(col, '__class__') and col.__class__.__name__ == "AggregateFuncNode":
                continue

            # 获取列名
            col_name = None
            if hasattr(col, '__class__'):
                if col.__class__.__name__ == "ColumnNode":
                    col_name = col.name
                elif col.__class__.__name__ == "AliasColumnNode":
                    col_name = col.column_name
            elif isinstance(col, str):
                col_name = col

            # 非聚合列必须在GROUP BY中
            if col_name and col_name not in group_keys:
                raise ValueError(f"Column '{col_name}' must appear in GROUP BY clause or be an aggregate function")

    def _generate_sort_plan(self, order_by_node, child_plan: Dict[str, Any]) -> Dict[str, Any]:
        """★ 新增：生成排序计划"""

        # ★ 转换排序键（处理列序号和别名）
        sort_keys = []
        for key_spec in order_by_node.sort_keys:
            column = key_spec["column"]
            order = key_spec["order"]

            # ★ 处理列序号：__pos_1 → 转换为实际列名
            if column.startswith("__pos_"):
                pos_str = column[6:]  # 去掉"__pos_"前缀
                try:
                    pos = int(pos_str)
                    # 这里简化处理：由执行器根据投影列顺序解析
                    # 实际项目中可以在此阶段就解析为实际列名
                    column = f"__position_{pos}"
                except ValueError:
                    raise ValueError(f"Invalid column position: {pos_str}")

            sort_keys.append({"column": column, "order": order})

        sort_plan = {
            "op": "Sort",
            "keys": sort_keys,
            "estimated_cost": child_plan["estimated_cost"] + child_plan["estimated_rows"] * 0.1,  # O(n log n)
            "estimated_rows": child_plan["estimated_rows"],
            "description": f"Sort by {len(sort_keys)} keys",
            "child": child_plan
        }

        return sort_plan

    def _generate_limit_plan(self, limit_node, child_plan: Dict[str, Any]) -> Dict[str, Any]:
        """★ 新增：生成分页计划"""

        limit_plan = {
            "op": "Limit",
            "offset": limit_node.offset,
            "count": limit_node.count,
            "estimated_cost": child_plan["estimated_cost"] + 1.0,
            "estimated_rows": min(limit_node.count, child_plan["estimated_rows"] - limit_node.offset),
            "description": f"Limit {limit_node.offset}, {limit_node.count}",
            "child": child_plan
        }

        return limit_plan

    def _convert_columns_to_plan_format(self, columns: List, has_aggregates: bool = False) -> List[Dict[str, Any]]:
        """★ 修改：扩展列转换，支持聚合函数"""
        plan_columns = []

        for col in columns:
            if col == "*":
                plan_columns.append("*")
            elif isinstance(col, str):
                plan_columns.append(col)
            elif hasattr(col, '__class__'):
                node_type = col.__class__.__name__

                if node_type == "ColumnNode":
                    plan_columns.append(col.name)
                elif node_type == "AliasColumnNode":
                    plan_columns.append({
                        "name": col.column_name,
                        "alias": col.alias
                    })
                elif node_type == "AggregateFuncNode":
                    # ★ 聚合函数：在GroupAggregate阶段已处理，此处使用别名
                    alias = col.alias or f"{col.func_name.lower()}_{col.column}"
                    plan_columns.append(alias)
            else:
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

    print(f"转换正确性: {condition_dict == expected}")

def test_s6s7_planner_features():
    print("=== S6+S7 Planner功能测试 ===")
    from sql.semantic import Catalog
    # 创建测试catalog
    catalog = Catalog()
    catalog.create_table("employees", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "dept", "type": "VARCHAR"},
        {"name": "salary", "type": "INT"},
        {"name": "age", "type": "INT"}
    ])
    planner = Planner(catalog)
    test_cases = [
        # S6聚合测试
        ("SELECT COUNT(*) FROM employees;", "全局聚合"),
        ("SELECT dept, COUNT(*), AVG(salary) FROM employees GROUP BY dept;", "分组聚合"),
        ("SELECT dept, AVG(salary) as avg_sal FROM employees GROUP BY dept HAVING AVG(salary) > 70000;", "HAVING过滤"),

        # S7排序分页测试
        ("SELECT * FROM employees ORDER BY salary DESC;", "单列排序"),
        ("SELECT * FROM employees ORDER BY dept ASC, salary DESC;", "多列排序"),
        ("SELECT * FROM employees ORDER BY 1, 2;", "序号排序"),
        ("SELECT * FROM employees LIMIT 5;", "简单分页"),
        ("SELECT * FROM employees LIMIT 5, 10;", "偏移分页"),

        # 完整管线测试
        ("SELECT dept, AVG(salary) as avg_sal, COUNT(*) as cnt FROM employees WHERE age > 25 GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY avg_sal DESC LIMIT 3;",
         "完整管线"),
    ]
    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print("✓ 计划生成成功")

            plan_dict = plan.to_dict()

            # 检查关键算子
            def check_operators(node, path=""):
                if isinstance(node, dict):
                    op = node.get("op")
                    if op:
                        print(f"   算子: {path}{op}")

                        # 显示关键参数
                        if op == "GroupAggregate":
                            group_keys = node.get("group_keys", [])
                            aggregates = node.get("aggregates", [])
                            print(f"      分组键: {group_keys}")
                            print(f"      聚合函数: {[a.get('func') for a in aggregates]}")

                        elif op == "Sort":
                            keys = node.get("keys", [])
                            key_desc = [f"{k.get('column')} {k.get('order')}" for k in keys]
                            print(f"      排序键: {key_desc}")

                        elif op == "Limit":
                            offset = node.get("offset", 0)
                            count = node.get("count", 0)
                            print(f"      分页: offset={offset}, count={count}")

                    child = node.get("child")
                    if child:
                        check_operators(child, path + "  ")

            check_operators(plan_dict)
        except Exception as e:
            print(f"❌ 计划生成失败: {e}")


def test_semantic_validation():
    """测试语义验证"""
    print("\n=== 语义验证测试 ===")

    from src.sql.semantic import Catalog

    catalog = Catalog()
    catalog.create_table("test", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "dept", "type": "VARCHAR"}
    ])

    planner = Planner(catalog)

    # 错误用例：非聚合列不在GROUP BY中
    error_cases = [
        ("SELECT name, COUNT(*) FROM test;", "非聚合列不在GROUP BY中"),
        ("SELECT * FROM test GROUP BY dept;", "SELECT * 与 GROUP BY冲突"),
        ("SELECT COUNT(*) FROM test HAVING id > 1;", "HAVING without GROUP BY"),
    ]

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n[错误测试 {i}] {expected_error}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print(f"❌ 应该报错但生成成功了")
        except Exception as e:
            print(f"✓ 正确捕获错误: {e}")


# ==================== 测试函数 ====================

def test_s6s7_planner_features():
    """测试S6+S7计划生成功能"""
    print("=== S6+S7 Planner功能测试 ===")

    from src.sql.semantic import Catalog

    # 创建测试catalog
    catalog = Catalog()
    catalog.create_table("employees", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "dept", "type": "VARCHAR"},
        {"name": "salary", "type": "INT"},
        {"name": "age", "type": "INT"}
    ])

    planner = Planner(catalog)

    test_cases = [
        # S6聚合测试
        ("SELECT COUNT(*) FROM employees;", "全局聚合"),
        ("SELECT dept, COUNT(*), AVG(salary) FROM employees GROUP BY dept;", "分组聚合"),
        ("SELECT dept, AVG(salary) as avg_sal FROM employees GROUP BY dept HAVING AVG(salary) > 70000;", "HAVING过滤"),

        # S7排序分页测试
        ("SELECT * FROM employees ORDER BY salary DESC;", "单列排序"),
        ("SELECT * FROM employees ORDER BY dept ASC, salary DESC;", "多列排序"),
        ("SELECT * FROM employees ORDER BY 1, 2;", "序号排序"),
        ("SELECT * FROM employees LIMIT 5;", "简单分页"),
        ("SELECT * FROM employees LIMIT 5, 10;", "偏移分页"),

        # 完整管线测试
        ("SELECT dept, AVG(salary) as avg_sal, COUNT(*) as cnt FROM employees WHERE age > 25 GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY avg_sal DESC LIMIT 3;",
         "完整管线"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print("✓ 计划生成成功")

            plan_dict = plan.to_dict()

            # 检查关键算子
            def check_operators(node, path=""):
                if isinstance(node, dict):
                    op = node.get("op")
                    if op:
                        print(f"   算子: {path}{op}")

                        # 显示关键参数
                        if op == "GroupAggregate":
                            group_keys = node.get("group_keys", [])
                            aggregates = node.get("aggregates", [])
                            print(f"      分组键: {group_keys}")
                            print(f"      聚合函数: {[a.get('func') for a in aggregates]}")

                        elif op == "Sort":
                            keys = node.get("keys", [])
                            key_desc = [f"{k.get('column')} {k.get('order')}" for k in keys]
                            print(f"      排序键: {key_desc}")

                        elif op == "Limit":
                            offset = node.get("offset", 0)
                            count = node.get("count", 0)
                            print(f"      分页: offset={offset}, count={count}")

                    child = node.get("child")
                    if child:
                        check_operators(child, path + "  ")

            check_operators(plan_dict)

        except Exception as e:
            print(f"❌ 计划生成失败: {e}")


def test_semantic_validation():
    """测试语义验证"""
    print("\n=== 语义验证测试 ===")

    from src.sql.semantic import Catalog

    catalog = Catalog()
    catalog.create_table("test", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR"},
        {"name": "dept", "type": "VARCHAR"}
    ])

    planner = Planner(catalog)

    # 错误用例：非聚合列不在GROUP BY中
    error_cases = [
        ("SELECT name, COUNT(*) FROM test;", "非聚合列不在GROUP BY中"),
        ("SELECT * FROM test GROUP BY dept;", "SELECT * 与 GROUP BY冲突"),
        ("SELECT COUNT(*) FROM test HAVING id > 1;", "HAVING without GROUP BY"),
    ]

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n[错误测试 {i}] {expected_error}")
        print(f"SQL: {sql}")
        try:
            plan = planner.plan(sql)
            print(f"❌ 应该报错但生成成功了")
        except Exception as e:
            print(f"✓ 正确捕获错误: {e}")


if __name__ == "__main__":
    test_s6s7_planner_features()
    test_semantic_validation()