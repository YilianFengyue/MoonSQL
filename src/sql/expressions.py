# 文件路径: MoonSQL/src/sql/expressions.py

"""
SQL表达式求值引擎 - S5核心组件

【功能说明】
- 统一的表达式求值接口
- 支持比较运算符：=, !=, <>, <, <=, >, >=
- 支持LIKE模式匹配：%, _通配符  
- 支持IN/NOT IN：常量列表 + 子查询
- 支持BETWEEN范围判断
- 支持IS NULL/IS NOT NULL
- 支持AND/OR逻辑组合

【表达式格式】
{
  "type": "compare",           # compare/like/in/between/is_null/and/or/not
  "left": "column_name",       # 列名或嵌套表达式
  "op": "=",                   # 操作符
  "right": "value",            # 值或嵌套表达式
  "values": [1,2,3],          # IN操作的值列表
  "subquery": {...},          # 子查询表达式
  "min": 10, "max": 20,       # BETWEEN范围
  "is_null": true             # IS NULL标志
}

【设计原则】
- 递归求值：支持嵌套表达式
- 类型容错：自动类型转换
- NULL语义：遵循SQL NULL处理规则
- 性能优化：避免重复计算
"""

import re
from typing import Any, Dict, List, Union, Optional, Callable
from abc import ABC, abstractmethod


class ExpressionError(Exception):
    """表达式求值错误"""
    pass


class ExpressionEvaluator:
    """SQL表达式求值器"""

    def __init__(self, subquery_executor: Callable = None):
        """
        初始化求值器
        Args:
            subquery_executor: 子查询执行函数，签名为 (subquery_expr) -> List[Any]
        """
        self.subquery_executor = subquery_executor

    def evaluate(self, expression: Dict[str, Any], row_data: Dict[str, Any]) -> Any:
        """
        求值主函数
        Args:
            expression: 表达式字典
            row_data: 当前行数据
        Returns:
            求值结果
        """
        if not isinstance(expression, dict):
            return expression

        expr_type = expression.get("type")

        if expr_type == "compare":
            return self._eval_compare(expression, row_data)
        elif expr_type == "like":
            return self._eval_like(expression, row_data)
        elif expr_type == "in":
            return self._eval_in(expression, row_data)
        elif expr_type == "between":
            return self._eval_between(expression, row_data)
        elif expr_type == "is_null":
            return self._eval_is_null(expression, row_data)
        elif expr_type == "and":
            return self._eval_and(expression, row_data)
        elif expr_type == "or":
            return self._eval_or(expression, row_data)
        elif expr_type == "not":
            return self._eval_not(expression, row_data)
        else:
            raise ExpressionError(f"不支持的表达式类型: {expr_type}")

    def _eval_compare(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估比较表达式: =, !=, <>, <, <=, >, >="""
        left_val = self._get_value(expr["left"], row_data)
        right_val = self._get_value(expr["right"], row_data)
        op = expr["op"]

        return self._compare_values(left_val, right_val, op)

    def _eval_like(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估LIKE表达式"""
        text_val = self._get_value(expr["left"], row_data)
        pattern_val = self._get_value(expr["right"], row_data)

        if text_val is None or pattern_val is None:
            return False

        return self._like_match(str(text_val), str(pattern_val))

    def _eval_in(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估IN表达式: 支持常量列表和子查询"""
        left_val = self._get_value(expr["left"], row_data)

        if left_val is None:
            return False

        # 检查是否有子查询
        if "subquery" in expr:
            if not self.subquery_executor:
                raise ExpressionError("子查询功能需要subquery_executor支持")

            subquery_results = self.subquery_executor(expr["subquery"])
            # 子查询结果应该是单列值的列表
            compare_values = [row[list(row.keys())[0]] if isinstance(row, dict) else row
                              for row in subquery_results]
        else:
            # 常量列表
            compare_values = expr.get("values", [])

        # 执行IN比较
        for val in compare_values:
            if self._values_equal(left_val, val):
                return True

        return False

    def _eval_between(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估BETWEEN表达式"""
        value = self._get_value(expr["left"], row_data)
        min_val = self._get_value(expr["min"], row_data)
        max_val = self._get_value(expr["max"], row_data)

        if value is None or min_val is None or max_val is None:
            return False

        try:
            return min_val <= value <= max_val
        except TypeError:
            # 类型不可比较
            return False

    def _eval_is_null(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估IS NULL表达式"""
        value = self._get_value(expr["left"], row_data)
        is_null_check = expr.get("is_null", True)

        if is_null_check:
            return value is None
        else:
            return value is not None

    def _eval_and(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估AND表达式"""
        left_result = self.evaluate(expr["left"], row_data)
        if not left_result:
            return False  # 短路求值

        right_result = self.evaluate(expr["right"], row_data)
        return bool(right_result)

    def _eval_or(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估OR表达式"""
        left_result = self.evaluate(expr["left"], row_data)
        if left_result:
            return True  # 短路求值

        right_result = self.evaluate(expr["right"], row_data)
        return bool(right_result)

    def _eval_not(self, expr: Dict[str, Any], row_data: Dict[str, Any]) -> bool:
        """评估NOT表达式"""
        inner_result = self.evaluate(expr["condition"], row_data)
        return not bool(inner_result)

    # 文件: src/sql/expressions.py
    # 类: ExpressionEvaluator

    def _get_value(self, ref: Union[str, Dict, Any], row_data: Dict[str, Any]) -> Any:
        """
        获取值：可能是列名、字面量、或嵌套表达式
        规则（最小修复版）：
        1) 若是 dict 且显式字面量: {"type":"literal","value":...} → 返回 value
        2) 若是 dict 且非字面量: 递归 evaluate（支持嵌套表达式）
        3) 若是 str:
           - 若 str 在当前行的列名集合中：视为列名，返回 row_data[str]
           - 否则：视为字面量字符串本身，直接返回 ref
        4) 其他类型：当作常量直接返回
        """
        # ★ 显式字面量：兼容可选的 {"type":"literal","value":...}
        if isinstance(ref, dict):
            if ref.get("type") == "literal":
                return ref.get("value")
            return self.evaluate(ref, row_data)

        # ★ 字符串：优先当列名匹配；否则作为字面量字符串
        if isinstance(ref, str):
            if ref in row_data:
                return row_data[ref]  # 列名
            return ref  # 字面量字符串

        # ★ 其他：数字/None/布尔等，直接返回
        return ref

    def _compare_values(self, left: Any, right: Any, op: str) -> bool:
        """比较两个值"""
        # 处理NULL值 - SQL NULL语义
        if left is None or right is None:
            if op in ['=', '==']:
                return left is None and right is None
            elif op in ['!=', '<>', '≠']:
                return not (left is None and right is None)
            else:
                return False  # NULL与任何值比较大小都返回False

        # 类型转换
        try:
            left, right = self._normalize_types(left, right)
        except (ValueError, TypeError):
            # 类型不兼容，返回False
            return False

        # 执行比较
        try:
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
            else:
                raise ExpressionError(f"不支持的比较操作符: {op}")
        except TypeError:
            return False

    def _values_equal(self, left: Any, right: Any) -> bool:
        """判断两个值是否相等（用于IN操作）"""
        if left is None and right is None:
            return True
        if left is None or right is None:
            return False

        try:
            left, right = self._normalize_types(left, right)
            return left == right
        except (ValueError, TypeError):
            return False

    def _normalize_types(self, left: Any, right: Any) -> tuple:
        """类型规范化：尝试将两个值转换为可比较的类型"""
        # 如果已经是相同类型，直接返回
        if type(left) == type(right):
            return left, right

        # 数字类型转换
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return left, right

        # 字符串转数字
        if isinstance(left, str) and isinstance(right, (int, float)):
            try:
                if '.' in left:
                    return float(left), float(right)
                else:
                    return int(left), right
            except ValueError:
                pass

        if isinstance(right, str) and isinstance(left, (int, float)):
            try:
                if '.' in right:
                    return float(left), float(right)
                else:
                    return left, int(right)
            except ValueError:
                pass

        # 都转换为字符串进行比较
        return str(left), str(right)

    def _like_match(self, text: str, pattern: str) -> bool:
        """
        LIKE模式匹配
        % 匹配任意长度字符串（包括空字符串）
        _ 匹配单个字符
        """
        # 将LIKE模式转换为正则表达式
        regex_pattern = ""
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == '%':
                regex_pattern += '.*'
            elif char == '_':
                regex_pattern += '.'
            elif char in r'[]{}()*+?^$\|':
                # 转义正则表达式特殊字符
                regex_pattern += '\\' + char
            else:
                regex_pattern += char
            i += 1

        # 执行匹配（不区分大小写）
        try:
            return re.match(f"^{regex_pattern}$", text, re.IGNORECASE) is not None
        except re.error:
            return False


def parse_simple_expression(expr_str: str) -> Dict[str, Any]:
    """
    解析简单表达式字符串为表达式字典
    主要用于向后兼容和测试

    支持格式:
    - "col = value"
    - "col LIKE 'pattern'"
    - "col IN (1,2,3)"
    - "col BETWEEN 10 AND 20"
    - "col IS NULL"
    """
    expr_str = expr_str.strip()

    # IS NULL / IS NOT NULL
    null_match = re.match(r'(\w+)\s+IS\s+(NOT\s+)?NULL', expr_str, re.IGNORECASE)
    if null_match:
        column = null_match.group(1)
        is_not_null = null_match.group(2) is not None
        return {
            "type": "is_null",
            "left": column,
            "is_null": not is_not_null
        }

    # BETWEEN
    between_match = re.match(r'(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)', expr_str, re.IGNORECASE)
    if between_match:
        column = between_match.group(1)
        min_val = _parse_value(between_match.group(2))
        max_val = _parse_value(between_match.group(3))
        return {
            "type": "between",
            "left": column,
            "min": min_val,
            "max": max_val
        }

    # IN
    in_match = re.match(r'(\w+)\s+IN\s*\((.+?)\)', expr_str, re.IGNORECASE)
    if in_match:
        column = in_match.group(1)
        values_str = in_match.group(2)
        values = [_parse_value(v.strip()) for v in values_str.split(',')]
        return {
            "type": "in",
            "left": column,
            "values": values
        }

    # LIKE
    like_match = re.match(r'(\w+)\s+LIKE\s+(.+)', expr_str, re.IGNORECASE)
    if like_match:
        column = like_match.group(1)
        pattern = _parse_value(like_match.group(2))
        return {
            "type": "like",
            "left": column,
            "right": pattern
        }

    # 基本比较
    compare_match = re.match(r'(\w+)\s*(=|!=|<>|<=|>=|<|>)\s*(.+)', expr_str)
    if compare_match:
        column = compare_match.group(1)
        operator = compare_match.group(2)
        value = _parse_value(compare_match.group(3))
        return {
            "type": "compare",
            "left": column,
            "op": operator,
            "right": value
        }

    raise ExpressionError(f"无法解析表达式: {expr_str}")


def _parse_value(value_str: str) -> Any:
    """解析值字符串"""
    value_str = value_str.strip()

    # 字符串值
    if (value_str.startswith("'") and value_str.endswith("'")) or \
            (value_str.startswith('"') and value_str.endswith('"')):
        return value_str[1:-1]

    # 数字值
    try:
        if '.' in value_str:
            return float(value_str)
        else:
            return int(value_str)
    except ValueError:
        pass

    # NULL
    if value_str.upper() == 'NULL':
        return None

    # 其他情况当作字符串处理
    return value_str


# ==================== 测试代码 ====================

def test_expression_evaluator():
    """测试表达式求值器"""
    print("=== 表达式求值器测试 ===")

    evaluator = ExpressionEvaluator()

    # 测试数据
    test_row = {
        "id": 1,
        "name": "Alice",
        "age": 25,
        "score": 95.5,
        "active": None
    }

    test_cases = [
        # 基本比较
        ({"type": "compare", "left": "age", "op": "=", "right": 25}, True, "等于比较"),
        ({"type": "compare", "left": "age", "op": ">", "right": 20}, True, "大于比较"),
        ({"type": "compare", "left": "name", "op": "=", "right": "Alice"}, True, "字符串比较"),
        ({"type": "compare", "left": "score", "op": ">=", "right": 95}, True, "浮点数比较"),

        # LIKE匹配
        ({"type": "like", "left": "name", "right": "A%"}, True, "LIKE前缀匹配"),
        ({"type": "like", "left": "name", "right": "%ice"}, True, "LIKE后缀匹配"),
        ({"type": "like", "left": "name", "right": "A_ice"}, True, "LIKE单字符匹配"),
        ({"type": "like", "left": "name", "right": "B%"}, False, "LIKE不匹配"),

        # IN操作
        ({"type": "in", "left": "age", "values": [20, 25, 30]}, True, "IN匹配"),
        ({"type": "in", "left": "age", "values": [18, 19, 20]}, False, "IN不匹配"),
        ({"type": "in", "left": "name", "values": ["Alice", "Bob"]}, True, "字符串IN匹配"),

        # BETWEEN
        ({"type": "between", "left": "age", "min": 20, "max": 30}, True, "BETWEEN范围内"),
        ({"type": "between", "left": "age", "min": 30, "max": 40}, False, "BETWEEN范围外"),

        # IS NULL
        ({"type": "is_null", "left": "active", "is_null": True}, True, "IS NULL"),
        ({"type": "is_null", "left": "name", "is_null": True}, False, "IS NULL非空值"),
        ({"type": "is_null", "left": "active", "is_null": False}, False, "IS NOT NULL"),

        # 逻辑操作
        ({
             "type": "and",
             "left": {"type": "compare", "left": "age", "op": ">", "right": 20},
             "right": {"type": "compare", "left": "name", "op": "=", "right": "Alice"}
         }, True, "AND逻辑"),

        ({
             "type": "or",
             "left": {"type": "compare", "left": "age", "op": "<", "right": 18},
             "right": {"type": "compare", "left": "name", "op": "=", "right": "Alice"}
         }, True, "OR逻辑"),
    ]

    for i, (expr, expected, desc) in enumerate(test_cases, 1):
        try:
            result = evaluator.evaluate(expr, test_row)
            status = "✓" if result == expected else "❌"
            print(f"[{i:2d}] {status} {desc}: {result} (期望: {expected})")
            if result != expected:
                print(f"     表达式: {expr}")
        except Exception as e:
            print(f"[{i:2d}] ❌ {desc}: 错误 - {e}")


def test_simple_expression_parser():
    """测试简单表达式解析器"""
    print("\n=== 简单表达式解析测试 ===")

    test_cases = [
        ("age = 25", "等于表达式"),
        ("name LIKE 'A%'", "LIKE表达式"),
        ("age IN (20,25,30)", "IN表达式"),
        ("score BETWEEN 90 AND 100", "BETWEEN表达式"),
        ("active IS NULL", "IS NULL表达式"),
        ("status IS NOT NULL", "IS NOT NULL表达式"),
    ]

    for expr_str, desc in test_cases:
        try:
            expr_dict = parse_simple_expression(expr_str)
            print(f"✓ {desc}: {expr_str}")
            print(f"   解析结果: {expr_dict}")
        except Exception as e:
            print(f"❌ {desc}: {expr_str} - 错误: {e}")


if __name__ == "__main__":
    test_expression_evaluator()
    test_simple_expression_parser()