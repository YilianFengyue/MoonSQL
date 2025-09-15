# 文件路径: MoonSQL/src/engine/aggregate.py

"""
GROUP BY分组聚合算子 - S6核心组件

【功能说明】
- 实现GROUP BY分组和聚合函数计算
- 支持五大聚合函数：COUNT/SUM/AVG/MIN/MAX
- 支持COUNT(*)和COUNT(column)的区别处理
- 正确处理NULL值：聚合函数忽略NULL，COUNT(*)包含NULL行
- 支持无GROUP BY的全局聚合（空分组键）
- 哈希分组算法，高效处理多列组合分组

【算子接口】
继承自Operator基类，实现execute()方法
输入：子算子的结果流
输出：分组聚合后的结果流

【计划格式】
{
  "op": "GroupAggregate",
  "group_keys": ["dept"],  # 分组列，空列表表示全局聚合
  "aggregates": [
    {"func": "COUNT", "column": "*", "alias": "cnt"},
    {"func": "AVG", "column": "salary", "alias": "avg_sal"}
  ],
  "having": {"type": "compare", "left": "cnt", "op": ">", "right": 5}  # 可选
}

【聚合函数语义】
- COUNT(*): 计数所有行，包括NULL值
- COUNT(col): 计数非NULL值
- SUM(col): 数值列求和，忽略NULL，空集返回NULL
- AVG(col): 数值列平均值，忽略NULL，空集返回NULL
- MIN/MAX(col): 最值比较，忽略NULL，空集返回NULL

【NULL处理原则】
遵循SQL标准：聚合函数忽略NULL参与计算，但COUNT(*)统计所有行
"""

from typing import Dict, List, Any, Iterator, Optional, Union
from abc import ABC, abstractmethod

from src.sql.expressions import ExpressionEvaluator, ExpressionError


class AggregateFunction:
    """聚合函数计算器"""

    def __init__(self, func_name: str, column: str, alias: str = None):
        self.func_name = func_name.upper()
        self.column = column
        self.alias = alias or f"{func_name.lower()}_{column}"
        self.reset()

    def reset(self):
        """重置聚合状态"""
        self.count = 0  # 参与计算的非NULL值数量
        self.count_all = 0  # 所有行数（包括NULL）
        self.sum_value = 0  # 数值和
        self.min_value = None  # 最小值
        self.max_value = None  # 最大值
        self.has_values = False  # 是否有任何值参与计算

    def accumulate(self, value: Any):
        """累加一个值"""
        self.count_all += 1

        # ★ COUNT(*) 特殊处理：统计所有行包括NULL
        if self.func_name == "COUNT" and self.column == "*":
            self.count += 1
            self.has_values = True
            return

        # 其他聚合函数：忽略NULL值
        if value is None:
            return

        self.has_values = True
        self.count += 1

        if self.func_name == "COUNT":
            pass  # COUNT(col)只需要计数非NULL值

        elif self.func_name in ["SUM", "AVG"]:
            # ★ 数值类型转换和累加
            try:
                numeric_value = float(value) if not isinstance(value, (int, float)) else value
                self.sum_value += numeric_value
            except (ValueError, TypeError):
                raise ValueError(f"Cannot apply {self.func_name} to non-numeric value: {value}")

        elif self.func_name in ["MIN", "MAX"]:
            # ★ 最值比较（支持数值和字符串）
            if self.min_value is None:
                self.min_value = value
                self.max_value = value
            else:
                try:
                    if value < self.min_value:
                        self.min_value = value
                    if value > self.max_value:
                        self.max_value = value
                except TypeError:
                    # 类型不可比较时转为字符串比较
                    str_value = str(value)
                    str_min = str(self.min_value)
                    str_max = str(self.max_value)
                    if str_value < str_min:
                        self.min_value = value
                    if str_value > str_max:
                        self.max_value = value

    def get_result(self) -> Any:
        """获取聚合结果"""
        if self.func_name == "COUNT":
            return self.count

        # ★ 空集处理：除COUNT外所有聚合函数返回NULL
        if not self.has_values:
            return None

        if self.func_name == "SUM":
            return self.sum_value

        elif self.func_name == "AVG":
            # ★ 避免除零，确保浮点精度
            if self.count == 0:
                return None
            return float(self.sum_value) / float(self.count)

        elif self.func_name == "MIN":
            return self.min_value

        elif self.func_name == "MAX":
            return self.max_value

        else:
            raise ValueError(f"Unsupported aggregate function: {self.func_name}")


class GroupAggregateOperator:
    """GROUP BY分组聚合算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        self.plan = plan
        self.catalog_mgr = catalog_mgr

        # 解析计划参数
        self.group_keys = plan.get('group_keys', [])  # ★ 空列表表示全局聚合
        self.aggregates = plan.get('aggregates', [])
        self.having_condition = plan.get('having')

        if not self.aggregates:
            raise ValueError("GroupAggregate requires at least one aggregate function")

        # 初始化表达式求值器（用于HAVING）
        self.expression_evaluator = ExpressionEvaluator()

        # 分组存储：{group_key_tuple: {agg_alias: AggregateFunction}}
        self.groups = {}

    def execute(self, child_results: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """执行分组聚合"""
        # ★ 第一阶段：分组和聚合计算
        self._perform_grouping_and_aggregation(child_results)

        # ★ 第二阶段：生成结果并应用HAVING过滤
        for result_row in self._generate_results():
            # HAVING过滤
            if self.having_condition:
                try:
                    if not self.expression_evaluator.evaluate(self.having_condition, result_row):
                        continue
                except ExpressionError:
                    continue  # HAVING条件失败，跳过该组

            yield result_row

    def _perform_grouping_and_aggregation(self, rows: Iterator[Dict[str, Any]]):
        """执行分组和聚合计算"""
        for row in rows:
            # ★ 计算分组键
            if self.group_keys:
                # 有GROUP BY列：提取分组键值
                group_key = tuple(row.get(col) for col in self.group_keys)
            else:
                # ★ 无GROUP BY：全局聚合，使用空元组作为唯一分组
                group_key = ()

            # 为该分组初始化聚合函数
            if group_key not in self.groups:
                self.groups[group_key] = {}
                for agg_spec in self.aggregates:
                    func_name = agg_spec['func']
                    column = agg_spec['column']
                    alias = agg_spec.get('alias', f"{func_name.lower()}_{column}")
                    self.groups[group_key][alias] = AggregateFunction(func_name, column, alias)

            # 对该行应用所有聚合函数
            for alias, agg_func in self.groups[group_key].items():
                if agg_func.column == "*":
                    # COUNT(*) 特殊处理
                    agg_func.accumulate(1)  # 传入非NULL值让其计数
                else:
                    # 普通聚合函数：从行中提取列值
                    column_value = row.get(agg_func.column)
                    agg_func.accumulate(column_value)

    def _generate_results(self) -> Iterator[Dict[str, Any]]:
        """生成聚合结果行"""
        for group_key, agg_funcs in self.groups.items():
            result_row = {}

            # ★ 添加分组键到结果
            for i, key_col in enumerate(self.group_keys):
                if i < len(group_key):
                    result_row[key_col] = group_key[i]

            # ★ 添加聚合结果到结果
            for alias, agg_func in agg_funcs.items():
                result_row[alias] = agg_func.get_result()

            yield result_row


# ==================== 测试代码 ====================
def test_aggregate_functions():
    """测试聚合函数计算器"""
    print("=== 聚合函数测试 ===")

    # 测试COUNT
    count_func = AggregateFunction("COUNT", "id")
    count_func.accumulate(1)
    count_func.accumulate(2)
    count_func.accumulate(None)  # 应该被忽略
    count_func.accumulate(3)
    assert count_func.get_result() == 3, f"COUNT错误: {count_func.get_result()}"
    print("✓ COUNT函数测试通过")

    # 测试COUNT(*)
    count_star = AggregateFunction("COUNT", "*")
    count_star.accumulate(1)
    count_star.accumulate(None)  # 应该被计算
    count_star.accumulate(2)
    assert count_star.get_result() == 3, f"COUNT(*)错误: {count_star.get_result()}"
    print("✓ COUNT(*)函数测试通过")

    # 测试SUM
    sum_func = AggregateFunction("SUM", "salary")
    sum_func.accumulate(1000)
    sum_func.accumulate(2000)
    sum_func.accumulate(None)  # 应该被忽略
    sum_func.accumulate(3000)
    assert sum_func.get_result() == 6000, f"SUM错误: {sum_func.get_result()}"
    print("✓ SUM函数测试通过")

    # 测试AVG
    avg_func = AggregateFunction("AVG", "salary")
    avg_func.accumulate(1000)
    avg_func.accumulate(2000)
    avg_func.accumulate(None)  # 应该被忽略
    avg_func.accumulate(3000)
    expected_avg = 6000.0 / 3
    assert abs(avg_func.get_result() - expected_avg) < 0.001, f"AVG错误: {avg_func.get_result()}"
    print("✓ AVG函数测试通过")

    # 测试MIN/MAX
    min_func = AggregateFunction("MIN", "age")
    max_func = AggregateFunction("MAX", "age")

    for value in [30, 25, None, 35, 20]:
        min_func.accumulate(value)
        max_func.accumulate(value)

    assert min_func.get_result() == 20, f"MIN错误: {min_func.get_result()}"
    assert max_func.get_result() == 35, f"MAX错误: {max_func.get_result()}"
    print("✓ MIN/MAX函数测试通过")

    # 测试空集
    empty_sum = AggregateFunction("SUM", "value")
    assert empty_sum.get_result() is None, f"空集SUM应为NULL: {empty_sum.get_result()}"
    print("✓ 空集处理测试通过")


def test_group_aggregate_operator():
    """测试分组聚合算子"""
    print("\n=== 分组聚合算子测试 ===")

    # 测试数据
    test_data = [
        {"dept": "Engineering", "salary": 75000, "age": 25},
        {"dept": "Sales", "salary": 65000, "age": 30},
        {"dept": "Engineering", "salary": 80000, "age": 28},
        {"dept": "Sales", "salary": 70000, "age": 26},
        {"dept": "Engineering", "salary": 85000, "age": 30},
    ]

    # 测试1: 分组聚合
    print("\n1. 测试分组聚合:")
    plan1 = {
        "group_keys": ["dept"],
        "aggregates": [
            {"func": "COUNT", "column": "*", "alias": "cnt"},
            {"func": "AVG", "column": "salary", "alias": "avg_salary"}
        ]
    }

    operator1 = GroupAggregateOperator(plan1)
    results1 = list(operator1.execute(iter(test_data)))

    print(f"分组聚合结果: {len(results1)} 组")
    for result in results1:
        print(f"   {result}")

    # 验证结果
    eng_results = [r for r in results1 if r.get("dept") == "Engineering"]
    sales_results = [r for r in results1 if r.get("dept") == "Sales"]

    assert len(eng_results) == 1, "Engineering应该有1组"
    assert len(sales_results) == 1, "Sales应该有1组"
    assert eng_results[0]["cnt"] == 3, f"Engineering计数错误: {eng_results[0]['cnt']}"
    assert sales_results[0]["cnt"] == 2, f"Sales计数错误: {sales_results[0]['cnt']}"
    print("✓ 分组聚合验证通过")

    # 测试2: 全局聚合（无GROUP BY）
    print("\n2. 测试全局聚合:")
    plan2 = {
        "group_keys": [],  # ★ 无分组键
        "aggregates": [
            {"func": "COUNT", "column": "*", "alias": "total_count"},
            {"func": "AVG", "column": "salary", "alias": "overall_avg"}
        ]
    }

    operator2 = GroupAggregateOperator(plan2)
    results2 = list(operator2.execute(iter(test_data)))

    print(f"全局聚合结果: {results2}")
    assert len(results2) == 1, "全局聚合应该只有1行结果"
    assert results2[0]["total_count"] == 5, f"总计数错误: {results2[0]['total_count']}"
    print("✓ 全局聚合验证通过")

    # 测试3: HAVING过滤
    print("\n3. 测试HAVING过滤:")
    plan3 = {
        "group_keys": ["dept"],
        "aggregates": [
            {"func": "COUNT", "column": "*", "alias": "cnt"},
            {"func": "AVG", "column": "salary", "alias": "avg_salary"}
        ],
        "having": {
            "type": "compare",
            "left": "cnt",
            "op": ">",
            "right": 2
        }
    }

    operator3 = GroupAggregateOperator(plan3)
    results3 = list(operator3.execute(iter(test_data)))

    print(f"HAVING过滤结果: {results3}")
    # 只有Engineering部门有3个人，应该通过HAVING过滤
    assert len(results3) == 1, f"HAVING过滤后应该只有1组: {len(results3)}"
    assert results3[0]["dept"] == "Engineering", f"应该是Engineering: {results3[0]}"
    print("✓ HAVING过滤验证通过")


if __name__ == "__main__":
    test_aggregate_functions()
    test_group_aggregate_operator()
    print("\n🎉 聚合算子测试全部通过!")