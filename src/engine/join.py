# 文件路径: MoonSQL/src/engine/join.py

"""
JOIN算子 - S8多表查询核心实现

【功能说明】
- 实现内连接（INNER JOIN）和外连接（LEFT/RIGHT JOIN）
- 使用嵌套循环连接算法（Nested Loop Join）
- 支持等值连接条件（ON A.col = B.col）
- 处理列名冲突和表别名
- 正确处理NULL值和外连接的NULL填充

【算法设计】
- 嵌套循环：外表每行与内表所有行进行比较
- 条件匹配：评估ON子句的等值条件
- 结果构造：合并两表行数据，处理列名前缀
- NULL扩展：外连接时为不匹配行填充NULL

【支持的JOIN类型】
- INNER JOIN: 只返回匹配的行
- LEFT JOIN: 返回左表所有行 + 匹配行
- RIGHT JOIN: 返回右表所有行 + 匹配行

【性能特点】
- 简单直观，适合教学和小数据量
- 时间复杂度O(M*N)，M和N为两表行数
- 后续可优化为Hash Join或Sort-Merge Join
"""

from typing import Dict, List, Any, Iterator, Optional, Tuple
from abc import ABC


class JoinOperator:
    """JOIN算子 - 实现多表连接查询"""

    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        """
        初始化JOIN算子

        Args:
            plan: 执行计划 {
                "op": "Join",
                "join_type": "INNER|LEFT|RIGHT",
                "left_table": "table1",
                "right_table": "table2",
                "on_condition": {"type": "compare", "left": "table1.id", "op": "=", "right": "table2.id"},
                "left_alias": "t1",  # 可选
                "right_alias": "t2"  # 可选
            }
            catalog_mgr: 目录管理器
        """
        self.plan = plan
        self.catalog_mgr = catalog_mgr

        # 解析连接参数
        self.join_type = plan.get("join_type", "INNER").upper()
        self.left_table = plan.get("left_table")
        self.right_table = plan.get("right_table")
        self.on_condition = plan.get("on_condition")

        # 表别名（可选）
        self.left_alias = plan.get("left_alias")
        self.right_alias = plan.get("right_alias")

        # 验证参数
        if not self.left_table or not self.right_table:
            raise ValueError("JOIN requires both left_table and right_table")

        if not self.on_condition:
            raise ValueError("JOIN requires ON condition")

        if self.join_type not in ["INNER", "LEFT", "RIGHT"]:
            raise ValueError(f"Unsupported JOIN type: {self.join_type}")

        # 解析ON条件中的列引用
        self._parse_join_condition()

    def _parse_join_condition(self):
        """解析JOIN条件，提取左右表的连接列"""
        if self.on_condition.get("type") != "compare" or self.on_condition.get("op") != "=":
            raise ValueError("Only equality joins (=) are currently supported")

        left_col_ref = self.on_condition.get("left")
        right_col_ref = self.on_condition.get("right")

        # 解析表.列格式，如 "students.id" 或 "s.id"
        self.left_join_col = self._parse_column_reference(left_col_ref)
        self.right_join_col = self._parse_column_reference(right_col_ref)

        # 验证列是否属于正确的表
        if not self._validate_column_belongs_to_table(self.left_join_col, self.left_table, self.left_alias):
            if not self._validate_column_belongs_to_table(self.left_join_col, self.right_table, self.right_alias):
                raise ValueError(f"Column {left_col_ref} not found in either table")
            # 如果左列实际属于右表，交换
            self.left_join_col, self.right_join_col = self.right_join_col, self.left_join_col

    def _parse_column_reference(self, col_ref: str) -> str:
        """解析列引用，返回实际列名"""
        if "." in col_ref:
            table_part, col_part = col_ref.split(".", 1)
            return col_part
        else:
            # 无表前缀，直接返回列名
            return col_ref

    def _validate_column_belongs_to_table(self, column: str, table: str, alias: str = None) -> bool:
        """验证列是否属于指定表"""
        if not self.catalog_mgr:
            return True  # 无catalog时跳过验证

        return self.catalog_mgr.column_exists(table, column)

    def execute(self, left_data: Iterator[Dict[str, Any]], right_data: Iterator[Dict[str, Any]]) -> Iterator[
        Dict[str, Any]]:
        """
        执行JOIN操作

        Args:
            left_data: 左表数据迭代器
            right_data: 右表数据迭代器

        Yields:
            连接结果行
        """
        # 将右表数据物化到内存（嵌套循环需要多次遍历）
        right_rows = list(right_data)

        if self.join_type == "INNER":
            yield from self._execute_inner_join(left_data, right_rows)
        elif self.join_type == "LEFT":
            yield from self._execute_left_join(left_data, right_rows)
        elif self.join_type == "RIGHT":
            yield from self._execute_right_join(left_data, right_rows)
        else:
            raise ValueError(f"Unsupported JOIN type: {self.join_type}")

    def _execute_inner_join(self, left_data: Iterator[Dict[str, Any]], right_rows: List[Dict[str, Any]]) -> Iterator[
        Dict[str, Any]]:
        """执行内连接"""
        for left_row in left_data:
            for right_row in right_rows:
                if self._match_join_condition(left_row, right_row):
                    yield self._merge_rows(left_row, right_row)

    def _execute_left_join(self, left_data: Iterator[Dict[str, Any]], right_rows: List[Dict[str, Any]]) -> Iterator[
        Dict[str, Any]]:
        """执行左外连接"""
        for left_row in left_data:
            matched = False
            for right_row in right_rows:
                if self._match_join_condition(left_row, right_row):
                    yield self._merge_rows(left_row, right_row)
                    matched = True

            # 左表行没有匹配时，右表部分填NULL
            if not matched:
                null_right_row = self._create_null_row(right_rows[0] if right_rows else {})
                yield self._merge_rows(left_row, null_right_row)

    def _execute_right_join(self, left_data: Iterator[Dict[str, Any]], right_rows: List[Dict[str, Any]]) -> Iterator[
        Dict[str, Any]]:
        """执行右外连接"""
        left_rows = list(left_data)  # 物化左表数据
        matched_right_rows = set()

        # 先处理匹配的行
        for left_row in left_rows:
            for i, right_row in enumerate(right_rows):
                if self._match_join_condition(left_row, right_row):
                    yield self._merge_rows(left_row, right_row)
                    matched_right_rows.add(i)

        # 处理右表未匹配的行
        for i, right_row in enumerate(right_rows):
            if i not in matched_right_rows:
                null_left_row = self._create_null_row(left_rows[0] if left_rows else {})
                yield self._merge_rows(null_left_row, right_row)

    def _match_join_condition(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> bool:
        """检查两行是否满足连接条件"""
        left_value = left_row.get(self.left_join_col)
        right_value = right_row.get(self.right_join_col)

        # NULL值不参与等值连接
        if left_value is None or right_value is None:
            return False

        return left_value == right_value

    def _merge_rows(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> Dict[str, Any]:
        """合并两行数据，处理列名冲突"""
        result = {}

        # 添加左表列（使用表名或别名作为前缀）
        left_prefix = self.left_alias or self.left_table
        for col, value in left_row.items():
            result[f"{left_prefix}.{col}"] = value

        # 添加右表列
        right_prefix = self.right_alias or self.right_table
        for col, value in right_row.items():
            result[f"{right_prefix}.{col}"] = value

        return result

    def _create_null_row(self, template_row: Dict[str, Any]) -> Dict[str, Any]:
        """创建NULL行（用于外连接）"""
        return {col: None for col in template_row.keys()}

    def get_join_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        return {
            "join_type": self.join_type,
            "left_table": self.left_table,
            "right_table": self.right_table,
            "left_join_column": self.left_join_col,
            "right_join_column": self.right_join_col,
            "algorithm": "Nested Loop Join"
        }


# ==================== 辅助函数 ====================

def create_join_plan(join_type: str, left_table: str, right_table: str,
                     left_col: str, right_col: str,
                     left_alias: str = None, right_alias: str = None) -> Dict[str, Any]:
    """
    创建JOIN执行计划的辅助函数

    Args:
        join_type: "INNER", "LEFT", "RIGHT"
        left_table: 左表名
        right_table: 右表名
        left_col: 左表连接列
        right_col: 右表连接列
        left_alias: 左表别名（可选）
        right_alias: 右表别名（可选）

    Returns:
        JOIN执行计划字典
    """
    return {
        "op": "Join",
        "join_type": join_type.upper(),
        "left_table": left_table,
        "right_table": right_table,
        "on_condition": {
            "type": "compare",
            "left": f"{left_alias or left_table}.{left_col}",
            "op": "=",
            "right": f"{right_alias or right_table}.{right_col}"
        },
        "left_alias": left_alias,
        "right_alias": right_alias,
        "estimated_cost": 100.0,
        "description": f"{join_type.upper()} JOIN {left_table} and {right_table}"
    }


# ==================== 测试代码 ====================

def test_join_operator():
    """测试JOIN算子功能"""
    print("=== JOIN算子功能测试 ===")

    # 模拟学生表数据
    students_data = [
        {"id": 1, "name": "Alice", "major": "CS"},
        {"id": 2, "name": "Bob", "major": "Math"},
        {"id": 3, "name": "Charlie", "major": "CS"},
        {"id": 4, "name": "Diana", "major": "Physics"}
    ]

    # 模拟成绩表数据
    scores_data = [
        {"student_id": 1, "course": "Database", "score": 95},
        {"student_id": 1, "course": "Algorithm", "score": 88},
        {"student_id": 2, "course": "Database", "score": 82},
        {"student_id": 5, "course": "Database", "score": 91},  # 学生不存在
    ]

    print("\n1. 测试INNER JOIN:")

    # 内连接计划
    inner_join_plan = create_join_plan(
        "INNER", "students", "scores", "id", "student_id", "s", "sc"
    )

    join_op = JoinOperator(inner_join_plan)
    print(f"   连接统计: {join_op.get_join_stats()}")

    inner_results = list(join_op.execute(iter(students_data), iter(scores_data)))
    print(f"   INNER JOIN结果: {len(inner_results)}行")
    for row in inner_results[:3]:  # 显示前3行
        print(f"     {row}")

    print("\n2. 测试LEFT JOIN:")

    left_join_plan = create_join_plan(
        "LEFT", "students", "scores", "id", "student_id", "s", "sc"
    )

    left_join_op = JoinOperator(left_join_plan)
    left_results = list(left_join_op.execute(iter(students_data), iter(scores_data)))
    print(f"   LEFT JOIN结果: {len(left_results)}行")

    # 查找没有成绩的学生
    no_score_students = [row for row in left_results if row.get("sc.student_id") is None]
    print(f"   没有成绩的学生: {len(no_score_students)}个")
    for row in no_score_students:
        print(f"     {row['s.name']}: 无成绩记录")

    print("\n3. 测试RIGHT JOIN:")

    right_join_plan = create_join_plan(
        "RIGHT", "students", "scores", "id", "student_id", "s", "sc"
    )

    right_join_op = JoinOperator(right_join_plan)
    right_results = list(right_join_op.execute(iter(students_data), iter(scores_data)))
    print(f"   RIGHT JOIN结果: {len(right_results)}行")

    # 查找无对应学生的成绩
    orphan_scores = [row for row in right_results if row.get("s.id") is None]
    print(f"   无对应学生的成绩: {len(orphan_scores)}个")
    for row in orphan_scores:
        print(f"     学生ID {row['sc.student_id']}: 学生不存在")

    print("\n4. 验证连接条件:")

    # 验证内连接结果的正确性
    for row in inner_results[:2]:
        s_id = row.get("s.id")
        sc_student_id = row.get("sc.student_id")
        print(f"   连接条件验证: {s_id} == {sc_student_id} : {s_id == sc_student_id}")


def test_join_error_cases():
    """测试JOIN错误处理"""
    print("\n=== JOIN错误处理测试 ===")

    error_cases = [
        # 缺少表名
        ({"op": "Join", "join_type": "INNER"}, "缺少表名"),

        # 缺少连接条件
        ({"op": "Join", "join_type": "INNER", "left_table": "t1", "right_table": "t2"}, "缺少ON条件"),

        # 不支持的连接类型
        ({"op": "Join", "join_type": "FULL", "left_table": "t1", "right_table": "t2",
          "on_condition": {"type": "compare", "left": "t1.id", "op": "=", "right": "t2.id"}},
         "不支持的连接类型"),

        # 非等值连接
        ({"op": "Join", "join_type": "INNER", "left_table": "t1", "right_table": "t2",
          "on_condition": {"type": "compare", "left": "t1.id", "op": ">", "right": "t2.id"}},
         "非等值连接"),
    ]

    for i, (plan, desc) in enumerate(error_cases, 1):
        print(f"\n[错误测试 {i}] {desc}")
        try:
            join_op = JoinOperator(plan)
            print("❌ 应该报错但创建成功了")
        except ValueError as e:
            print(f"✓ 正确捕获错误: {e}")
        except Exception as e:
            print(f"✓ 捕获其他错误: {e}")


if __name__ == "__main__":
    test_join_operator()
    test_join_error_cases()