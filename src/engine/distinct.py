# 文件路径: MoonSQL/src/engine/distinct.py

"""
DISTINCT去重算子 - S5组件

【功能说明】
- 实现SELECT DISTINCT语义
- 基于哈希的高效去重
- 支持多列组合去重
- 内存友好的流式处理

【算子接口】
继承自Operator基类，实现execute()方法
输入：子算子的结果流
输出：去重后的结果流

【去重策略】
- 计算行的哈希值进行去重
- 支持所有数据类型的组合
- NULL值参与去重计算
- 保持第一次出现的行

【使用示例】
SELECT DISTINCT id, name FROM users;
=>
DistinctOperator
└── ProjectOperator(columns=["id", "name"])
    └── SeqScanOperator(table="users")

【Plan JSON格式】
{
  "op": "Distinct",
  "child": {
    "op": "Project",
    "columns": ["id", "name"],
    "child": {...}
  }
}
"""

import hashlib
import json
from typing import Dict, List, Any, Iterator, Set
from abc import ABC, abstractmethod


class Operator(ABC):
    """算子基类（为了独立性重新定义）"""
    
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


class DistinctOperator(Operator):
    """
    DISTINCT去重算子
    
    对子算子的输出进行去重处理，保留第一次出现的记录
    """
    
    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        self.distinct_columns = plan.get('columns')  # None表示对所有列去重
        
    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行去重操作"""
        if not self.children:
            raise RuntimeError("DISTINCT算子需要子算子")
        
        # 获取子算子的结果
        child_results = self.children[0].execute(storage_engine)
        
        # 使用哈希集合进行去重
        seen_hashes: Set[str] = set()
        
        for row in child_results:
            # 计算行的哈希值
            row_hash = self._compute_row_hash(row)
            
            # 如果未见过此哈希值，输出行并记录
            if row_hash not in seen_hashes:
                seen_hashes.add(row_hash)
                yield row
    
    def _compute_row_hash(self, row: Dict[str, Any]) -> str:
        """
        计算行的哈希值
        
        Args:
            row: 数据行
            
        Returns:
            行的哈希值字符串
        """
        # 确定参与哈希计算的列
        if self.distinct_columns:
            # 仅对指定列去重
            hash_data = {}
            for col in self.distinct_columns:
                hash_data[col] = row.get(col)
        else:
            # 对所有列去重
            hash_data = row.copy()
        
        # 创建标准化的字符串表示
        normalized_str = self._normalize_for_hash(hash_data)
        
        # 计算MD5哈希
        return hashlib.md5(normalized_str.encode('utf-8')).hexdigest()
    
    def _normalize_for_hash(self, data: Dict[str, Any]) -> str:
        """
        将数据标准化为可哈希的字符串
        
        确保相同数据生成相同哈希值，不同数据生成不同哈希值
        """
        # 按键排序确保一致性
        sorted_items = sorted(data.items())
        
        # 构建标准化字符串
        parts = []
        for key, value in sorted_items:
            # 标准化值的表示
            if value is None:
                normalized_value = "NULL"
            elif isinstance(value, bool):
                normalized_value = "TRUE" if value else "FALSE"
            elif isinstance(value, (int, float)):
                # 数字标准化：处理int/float等价性
                normalized_value = str(float(value))
            elif isinstance(value, str):
                # 字符串保持原样，但去除首尾空白
                normalized_value = f"'{value.strip()}'"
            else:
                # 其他类型转为字符串
                normalized_value = f"<{type(value).__name__}:{str(value)}>"
            
            parts.append(f"{key}={normalized_value}")
        
        return "{" + ",".join(parts) + "}"


class DistinctProjectOperator(Operator):
    """
    DISTINCT + PROJECT组合算子
    
    专门用于 SELECT DISTINCT col1, col2 这种场景的优化实现
    避免先投影再去重的两次遍历
    """
    
    def __init__(self, plan: Dict[str, Any], catalog_mgr=None):
        super().__init__(plan, catalog_mgr)
        self.columns = plan.get('columns', [])
        if not self.columns:
            raise ValueError("DistinctProject算子需要指定列")
    
    def execute(self, storage_engine) -> Iterator[Dict[str, Any]]:
        """执行投影+去重操作"""
        if not self.children:
            raise RuntimeError("DistinctProject算子需要子算子")
        
        child_results = self.children[0].execute(storage_engine)
        seen_hashes: Set[str] = set()
        
        for row in child_results:
            # 先投影
            projected_row = {}
            for col in self.columns:
                if col == '*':
                    # SELECT DISTINCT * 
                    projected_row.update(row)
                else:
                    projected_row[col] = row.get(col)
            
            # 再去重
            row_hash = self._compute_projected_hash(projected_row)
            
            if row_hash not in seen_hashes:
                seen_hashes.add(row_hash)
                yield projected_row
    
    def _compute_projected_hash(self, projected_row: Dict[str, Any]) -> str:
        """计算投影后行的哈希值"""
        # 复用DistinctOperator的哈希逻辑
        distinct_op = DistinctOperator({})
        return distinct_op._normalize_for_hash(projected_row)


def test_distinct_operator():
    """测试去重算子"""
    print("=== DISTINCT算子测试 ===")
    
    # 模拟子算子
    class MockChildOperator(Operator):
        def __init__(self, test_data):
            super().__init__({})
            self.test_data = test_data
        
        def execute(self, storage_engine):
            for row in self.test_data:
                yield row
    
    # 测试数据（包含重复行）
    test_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 1, "name": "Alice", "age": 25},  # 重复行
        {"id": 3, "name": "Charlie", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},    # 重复行
        {"id": 4, "name": "Alice", "age": 22},  # 不同的Alice
        {"id": 1, "name": "Alice", "age": 25},  # 又一个重复行
    ]
    
    print("原始数据（7行）:")
    for i, row in enumerate(test_data, 1):
        print(f"  [{i}] {row}")
    
    print("\n1. 测试全行去重:")
    
    # 创建去重算子
    distinct_plan = {"op": "Distinct"}
    distinct_op = DistinctOperator(distinct_plan)
    
    # 添加模拟子算子
    mock_child = MockChildOperator(test_data)
    distinct_op.add_child(mock_child)
    
    # 执行去重
    distinct_results = list(distinct_op.execute(None))
    
    print(f"去重后结果（{len(distinct_results)}行）:")
    for i, row in enumerate(distinct_results, 1):
        print(f"  [{i}] {row}")
    
    print("\n2. 测试指定列去重（仅name列）:")
    
    # 创建指定列去重算子
    distinct_name_plan = {"op": "Distinct", "columns": ["name"]}
    distinct_name_op = DistinctOperator(distinct_name_plan)
    distinct_name_op.add_child(MockChildOperator(test_data))
    
    # 执行
    name_results = list(distinct_name_op.execute(None))
    
    print(f"按name去重结果（{len(name_results)}行）:")
    for i, row in enumerate(name_results, 1):
        print(f"  [{i}] {row}")
    
    print("\n3. 测试DISTINCT + PROJECT组合:")
    
    # 创建组合算子
    distinct_project_plan = {"op": "DistinctProject", "columns": ["name", "age"]}
    distinct_project_op = DistinctProjectOperator(distinct_project_plan)
    distinct_project_op.add_child(MockChildOperator(test_data))
    
    # 执行
    project_results = list(distinct_project_op.execute(None))
    
    print(f"DISTINCT name,age 结果（{len(project_results)}行）:")
    for i, row in enumerate(project_results, 1):
        print(f"  [{i}] {row}")
    
    print("\n4. 测试边界情况:")
    
    # 空数据
    empty_op = DistinctOperator({"op": "Distinct"})
    empty_op.add_child(MockChildOperator([]))
    empty_results = list(empty_op.execute(None))
    print(f"空数据去重: {len(empty_results)}行")
    
    # 单行数据
    single_op = DistinctOperator({"op": "Distinct"})
    single_op.add_child(MockChildOperator([{"id": 1, "name": "Test"}]))
    single_results = list(single_op.execute(None))
    print(f"单行数据去重: {len(single_results)}行")
    
    # NULL值数据
    null_data = [
        {"id": 1, "name": None},
        {"id": 2, "name": "Alice"},
        {"id": 1, "name": None},  # 重复NULL
        {"id": 3, "name": None},  # 不同id的NULL
    ]
    null_op = DistinctOperator({"op": "Distinct"})
    null_op.add_child(MockChildOperator(null_data))
    null_results = list(null_op.execute(None))
    print(f"NULL值数据去重: {len(null_results)}行")
    for row in null_results:
        print(f"  {row}")


def test_hash_consistency():
    """测试哈希一致性"""
    print("\n=== 哈希一致性测试 ===")
    
    distinct_op = DistinctOperator({})
    
    # 相同数据应该生成相同哈希
    row1 = {"id": 1, "name": "Alice", "age": 25}
    row2 = {"id": 1, "name": "Alice", "age": 25}
    row3 = {"name": "Alice", "id": 1, "age": 25}  # 不同顺序
    
    hash1 = distinct_op._compute_row_hash(row1)
    hash2 = distinct_op._compute_row_hash(row2)
    hash3 = distinct_op._compute_row_hash(row3)
    
    print(f"相同数据哈希一致性: {hash1 == hash2 == hash3}")
    print(f"  哈希值: {hash1}")
    
    # 不同数据应该生成不同哈希
    row4 = {"id": 1, "name": "Bob", "age": 25}
    hash4 = distinct_op._compute_row_hash(row4)
    
    print(f"不同数据哈希差异性: {hash1 != hash4}")
    print(f"  原哈希: {hash1}")
    print(f"  新哈希: {hash4}")
    
    # NULL值处理
    row5 = {"id": 1, "name": None, "age": 25}
    row6 = {"id": 1, "name": None, "age": 25}
    hash5 = distinct_op._compute_row_hash(row5)
    hash6 = distinct_op._compute_row_hash(row6)
    
    print(f"NULL值哈希一致性: {hash5 == hash6}")
    print(f"  NULL哈希: {hash5}")


if __name__ == "__main__":
    test_distinct_operator()
    test_hash_consistency()