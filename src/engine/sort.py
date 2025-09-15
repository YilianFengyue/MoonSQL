# 文件路径: MoonSQL/src/engine/sort.py

"""
ORDER BY排序和LIMIT分页算子 - S7核心组件

【功能说明】
- 实现ORDER BY多列排序：支持ASC/DESC混合
- 实现LIMIT分页：支持offset,count和count OFFSET offset两种语法
- 稳定排序：相同键值保持原始顺序
- NULL值排序：MySQL风格，NULL视为最小值
- 类型兼容排序：数值/字符串/混合类型的统一比较

【算子接口】
继承自Operator基类，实现execute()方法
输入：子算子的结果流
输出：排序后的结果流（SortOperator）或分页后的结果流（LimitOperator）

【计划格式】
SortOperator:
{
  "op": "Sort",
  "keys": [
    {"column": "salary", "order": "DESC"},
    {"column": "name", "order": "ASC"}
  ]
}

LimitOperator:
{
  "op": "Limit",
  "offset": 10,    # 跳过的行数，默认0
  "count": 20      # 返回的行数
}

【排序规则】
1. NULL值排序：NULL < 任何非NULL值（MySQL风格）
2. 数值排序：按数值大小比较
3. 字符串排序：按字典序比较
4. 混合类型：NULL < 数值 < 字符串 < 其他类型
5. 稳定排序：相同值保持原始相对顺序

【性能考虑】
- 内存排序：适用于中等数据量（<10万行）
- 外部排序：大数据量时的预留接口（本期未实现）
- 分页优化：只排序到需要的位置（可选优化）
"""

from typing import Dict, List, Any, Iterator, Optional, Union, Callable
from abc import ABC, abstractmethod


class SortKey:
    """排序键定义"""

    def __init__(self, column: str, order: str = "ASC"):
        self.column = column
        self.order = order.upper()
        if self.order not in ["ASC", "DESC"]:
            raise ValueError(f"Invalid sort order: {order}. Must be ASC or DESC.")

    def __repr__(self):
        return f"SortKey({self.column}, {self.order})"


class SortComparator:
    """排序比较器 - 处理多列排序和NULL值"""

    def __init__(self, sort_keys: List[SortKey]):
        self.sort_keys = sort_keys

    def compare_values(self, val1: Any, val2: Any) -> int:
        """
        比较两个值
        返回: -1 if val1 < val2, 0 if val1 == val2, 1 if val1 > val2
        """
        # ★ NULL值处理：NULL < 任何非NULL值
        if val1 is None and val2 is None:
            return 0
        elif val1 is None:
            return -1
        elif val2 is None:
            return 1

        # ★ 类型统一比较
        try:
            # 尝试数值比较
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                return self._compare_numbers(val1, val2)

            # 尝试字符串比较
            elif isinstance(val1, str) and isinstance(val2, str):
                return self._compare_strings(val1, val2)

            # 混合类型：数值 < 字符串 < 其他
            else:
                return self._compare_mixed_types(val1, val2)

        except Exception:
            # 兜底：转为字符串比较
            return self._compare_strings(str(val1), str(val2))

    def _compare_numbers(self, a: Union[int, float], b: Union[int, float]) -> int:
        """数值比较"""
        if a < b:
            return -1
        elif a > b:
            return 1
        else:
            return 0

    def _compare_strings(self, a: str, b: str) -> int:
        """字符串比较"""
        if a < b:
            return -1
        elif a > b:
            return 1
        else:
            return 0

    def _compare_mixed_types(self, a: Any, b: Any) -> int:
        """混合类型比较：数值 < 字符串 < 其他"""

        def get_type_priority(val):
            if isinstance(val, (int, float)):
                return 1
            elif isinstance(val, str):
                return 2
            else:
                return 3

        priority_a = get_type_priority(a)
        priority_b = get_type_priority(b)

        if priority_a != priority_b:
            return self._compare_numbers(priority_a, priority_b)

        # 同优先级：转字符串比较
        return self._compare_strings(str(a), str(b))

    def compare_rows(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> int:
        """
        比较两行数据
        按照sort_keys的顺序逐列比较
        """
        for sort_key in self.sort_keys:
            col = sort_key.column
            val1 = row1.get(col)
            val2 = row2.get(col)

            # 比较这一列
            cmp_result = self.compare_values(val1, val2)

            if cmp_result != 0:
                # 根据排序方向调整结果
                if sort_key.order == "DESC":
                    return -cmp_result
                else:
                    return cmp_result

        # 所有列都相等：稳定排序，保持原始顺序
        return 0


class SortOperator:
    """ORDER BY排序算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        self.plan = plan
        self.catalog_mgr = catalog_mgr

        # 解析排序键
        keys_spec = plan.get('keys', [])
        if not keys_spec:
            raise ValueError("SortOperator requires at least one sort key")

        self.sort_keys = []
        for key_spec in keys_spec:
            if isinstance(key_spec, dict):
                column = key_spec.get('column')
                order = key_spec.get('order', 'ASC')
            else:
                # 简单格式：只有列名，默认ASC
                column = str(key_spec)
                order = 'ASC'

            if not column:
                raise ValueError("Sort key must specify column")

            self.sort_keys.append(SortKey(column, order))

        self.comparator = SortComparator(self.sort_keys)

    def execute(self, child_results: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """执行排序"""
        # ★ 读取所有数据到内存（内存排序）
        all_rows = list(child_results)

        if not all_rows:
            return

        # ★ 使用自定义比较器排序
        try:
            # Python的sorted是稳定排序
            from functools import cmp_to_key
            sorted_rows = sorted(all_rows, key=cmp_to_key(self.comparator.compare_rows))

            # 返回排序后的结果
            for row in sorted_rows:
                yield row

        except Exception as e:
            raise RuntimeError(f"Sort operation failed: {e}")


class LimitOperator:
    """LIMIT分页算子"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        self.plan = plan
        self.catalog_mgr = catalog_mgr

        # ★ 解析LIMIT参数：支持两种格式
        # 格式1: LIMIT count
        # 格式2: LIMIT offset, count
        # 格式3: LIMIT count OFFSET offset

        self.offset = plan.get('offset', 0)
        self.count = plan.get('count')

        if self.count is None:
            raise ValueError("LimitOperator requires count parameter")

        # 参数验证
        if self.offset < 0:
            raise ValueError(f"LIMIT offset must be non-negative: {self.offset}")
        if self.count <= 0:
            raise ValueError(f"LIMIT count must be positive: {self.count}")

    def execute(self, child_results: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """执行分页"""
        current_index = 0
        returned_count = 0

        for row in child_results:
            # ★ 跳过offset指定的行数
            if current_index < self.offset:
                current_index += 1
                continue

            # ★ 返回count指定的行数
            if returned_count < self.count:
                yield row
                returned_count += 1
                current_index += 1
            else:
                # 已返回足够的行，停止处理
                break


# ==================== 测试代码 ====================
def test_sort_comparator():
    """测试排序比较器"""
    print("=== 排序比较器测试 ===")

    # 创建测试比较器
    keys = [SortKey("salary", "DESC"), SortKey("name", "ASC")]
    comparator = SortComparator(keys)

    # 测试NULL值比较
    assert comparator.compare_values(None, 100) == -1, "NULL应该小于数值"
    assert comparator.compare_values(100, None) == 1, "数值应该大于NULL"
    assert comparator.compare_values(None, None) == 0, "NULL应该等于NULL"
    print("✓ NULL值比较测试通过")

    # 测试数值比较
    assert comparator.compare_values(100, 200) == -1, "100应该小于200"
    assert comparator.compare_values(200, 100) == 1, "200应该大于100"
    assert comparator.compare_values(100, 100) == 0, "100应该等于100"
    print("✓ 数值比较测试通过")

    # 测试字符串比较
    assert comparator.compare_values("Alice", "Bob") == -1, "Alice应该小于Bob"
    assert comparator.compare_values("Bob", "Alice") == 1, "Bob应该大于Alice"
    assert comparator.compare_values("Alice", "Alice") == 0, "Alice应该等于Alice"
    print("✓ 字符串比较测试通过")

    # 测试混合类型比较
    assert comparator.compare_values(100, "Alice") == -1, "数值应该小于字符串"
    assert comparator.compare_values("Alice", 100) == 1, "字符串应该大于数值"
    print("✓ 混合类型比较测试通过")


def test_sort_operator():
    """测试排序算子"""
    print("\n=== 排序算子测试 ===")

    # 测试数据
    test_data = [
        {"name": "Alice", "salary": 75000, "dept": "Engineering"},
        {"name": "Bob", "salary": 65000, "dept": "Sales"},
        {"name": "Charlie", "salary": 80000, "dept": "Engineering"},
        {"name": "Diana", "salary": 65000, "dept": "Sales"},  # 与Bob同薪水
        {"name": "Eve", "salary": 85000, "dept": "Engineering"},
    ]

    # 测试1: 单列排序（降序）
    print("\n1. 测试单列降序排序:")
    plan1 = {
        "keys": [{"column": "salary", "order": "DESC"}]
    }

    operator1 = SortOperator(plan1)
    results1 = list(operator1.execute(iter(test_data)))

    print("排序结果（按薪水降序）:")
    for result in results1:
        print(f"   {result['name']}: {result['salary']}")

    # 验证降序排序
    salaries = [r['salary'] for r in results1]
    assert salaries == sorted(salaries, reverse=True), f"降序排序错误: {salaries}"
    print("✓ 单列降序排序验证通过")

    # 测试2: 多列排序（复合）
    print("\n2. 测试多列排序:")
    plan2 = {
        "keys": [
            {"column": "salary", "order": "ASC"},  # 薪水升序
            {"column": "name", "order": "ASC"}  # 姓名升序（同薪水时）
        ]
    }

    operator2 = SortOperator(plan2)
    results2 = list(operator2.execute(iter(test_data)))

    print("排序结果（薪水升序，姓名升序）:")
    for result in results2:
        print(f"   {result['name']}: {result['salary']}")

    # 验证多列排序：Bob和Diana同薪水，按姓名排序Bob应该在前
    same_salary_group = [r for r in results2 if r['salary'] == 65000]
    assert len(same_salary_group) == 2, "应该有2个人薪水65000"
    assert same_salary_group[0]['name'] == "Bob", f"Bob应该排在前面: {same_salary_group}"
    assert same_salary_group[1]['name'] == "Diana", f"Diana应该排在后面: {same_salary_group}"
    print("✓ 多列排序验证通过")

    # 测试3: 含NULL值排序
    print("\n3. 测试NULL值排序:")
    test_data_with_null = test_data + [
        {"name": "Frank", "salary": None, "dept": "Marketing"}
    ]

    plan3 = {
        "keys": [{"column": "salary", "order": "ASC"}]
    }

    operator3 = SortOperator(plan3)
    results3 = list(operator3.execute(iter(test_data_with_null)))

    print("排序结果（含NULL值）:")
    for result in results3:
        print(f"   {result['name']}: {result['salary']}")

    # 验证NULL排在最前面
    assert results3[0]['salary'] is None, f"NULL应该排在最前面: {results3[0]}"
    assert results3[0]['name'] == "Frank", f"Frank应该排在最前面: {results3[0]}"
    print("✓ NULL值排序验证通过")


def test_limit_operator():
    """测试分页算子"""
    print("\n=== 分页算子测试 ===")

    # 测试数据（有序）
    test_data = [
        {"id": i, "name": f"User{i}"}
        for i in range(1, 11)  # 10条记录
    ]

    # 测试1: 简单LIMIT
    print("\n1. 测试简单LIMIT:")
    plan1 = {"offset": 0, "count": 3}

    operator1 = LimitOperator(plan1)
    results1 = list(operator1.execute(iter(test_data)))

    print(f"LIMIT 3结果: {len(results1)}条")
    for result in results1:
        print(f"   {result}")

    assert len(results1) == 3, f"应该返回3条记录: {len(results1)}"
    assert results1[0]['id'] == 1, f"第一条应该是id=1: {results1[0]}"
    assert results1[2]['id'] == 3, f"第三条应该是id=3: {results1[2]}"
    print("✓ 简单LIMIT验证通过")

    # 测试2: OFFSET + LIMIT
    print("\n2. 测试OFFSET + LIMIT:")
    plan2 = {"offset": 3, "count": 4}

    operator2 = LimitOperator(plan2)
    results2 = list(operator2.execute(iter(test_data)))

    print(f"LIMIT 3,4结果: {len(results2)}条")
    for result in results2:
        print(f"   {result}")

    assert len(results2) == 4, f"应该返回4条记录: {len(results2)}"
    assert results2[0]['id'] == 4, f"第一条应该是id=4: {results2[0]}"  # 跳过前3条
    assert results2[3]['id'] == 7, f"第四条应该是id=7: {results2[3]}"
    print("✓ OFFSET + LIMIT验证通过")

    # 测试3: 超出范围的LIMIT
    print("\n3. 测试超出范围的LIMIT:")
    plan3 = {"offset": 8, "count": 5}  # 只剩2条记录

    operator3 = LimitOperator(plan3)
    results3 = list(operator3.execute(iter(test_data)))

    print(f"超出范围LIMIT结果: {len(results3)}条")
    for result in results3:
        print(f"   {result}")

    assert len(results3) == 2, f"应该返回2条记录: {len(results3)}"  # 实际只有2条
    print("✓ 超出范围LIMIT验证通过")


def test_combined_sort_limit():
    """测试排序+分页组合"""
    print("\n=== 排序+分页组合测试 ===")

    test_data = [
        {"name": "Alice", "score": 95},
        {"name": "Bob", "score": 87},
        {"name": "Charlie", "score": 92},
        {"name": "Diana", "score": 89},
        {"name": "Eve", "score": 91},
    ]

    # 先排序再分页
    sort_plan = {
        "keys": [{"column": "score", "order": "DESC"}]
    }
    limit_plan = {
        "offset": 1,
        "count": 3
    }

    # 执行排序
    sort_op = SortOperator(sort_plan)
    sorted_results = list(sort_op.execute(iter(test_data)))

    # 执行分页
    limit_op = LimitOperator(limit_plan)
    final_results = list(limit_op.execute(iter(sorted_results)))

    print("排序+分页结果（跳过第1名，取2-4名）:")
    for result in final_results:
        print(f"   {result['name']}: {result['score']}")

    # 验证：应该是Charlie(92), Eve(91), Diana(89)
    expected_names = ["Charlie", "Eve", "Diana"]
    actual_names = [r['name'] for r in final_results]
    assert actual_names == expected_names, f"分页结果错误: {actual_names}"
    print("✓ 排序+分页组合验证通过")


if __name__ == "__main__":
    test_sort_comparator()
    test_sort_operator()
    test_limit_operator()
    test_combined_sort_limit()
    print("\n🎉 排序和分页算子测试全部通过!")