"""
MiniDB核心接口定义 - Phase 0接口稳定点
【设计原则】后续只替换实现，不改接口

【接口层次】
1. Planner：SQL -> Plan(JSON)
2. StorageEngine：表操作接口
3. Executor：执行计划解释器
4. 统一错误结构
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Iterator, Optional
import json


class SqlError(Exception):
    """统一SQL错误结构"""

    def __init__(self, error_type: str, line: int, col: int, hint: str):
        self.error_type = error_type  # "LexicalError"|"SyntaxError"|"SemanticError"|"RuntimeError"
        self.line = line
        self.col = col
        self.hint = hint
        super().__init__(f"{error_type} at line {line}, col {col}: {hint}")

    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            "etype": self.error_type,
            "line": self.line,
            "col": self.col,
            "hint": self.hint
        }


class Plan:
    """执行计划封装"""

    def __init__(self, plan_dict: Dict):
        self.plan = plan_dict

    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.plan, indent=2, ensure_ascii=False)

    def to_dict(self) -> Dict:
        """获取计划字典"""
        return self.plan


class IPlanner(ABC):
    """计划生成器接口 - Phase 0接口稳定点"""

    @abstractmethod
    def plan(self, sql_text: str) -> Plan:
        """
        SQL语句转换为执行计划
        Args:
            sql_text: SQL语句文本
        Returns:
            Plan对象
        Raises:
            SqlError: 编译错误（词法/语法/语义）
        """
        pass


class IStorageEngine(ABC):
    """存储引擎接口 - Phase 0接口稳定点"""

    @abstractmethod
    def create_table(self, name: str, columns: List[Dict]) -> None:
        """
        创建表
        Args:
            name: 表名
            columns: 列定义列表 [{"name": "id", "type": "INT"}, ...]
        Raises:
            SqlError: 表已存在等错误
        """
        pass

    @abstractmethod
    def insert_row(self, table: str, row: Dict) -> None:
        """
        插入行
        Args:
            table: 表名
            row: 行数据字典 {"id": 1, "name": "Alice"}
        Raises:
            SqlError: 表不存在、类型不匹配等错误
        """
        pass

    @abstractmethod
    def seq_scan(self, table: str) -> Iterator[Dict]:
        """
        顺序扫描表
        Args:
            table: 表名
        Returns:
            行数据迭代器
        Raises:
            SqlError: 表不存在等错误
        """
        pass

    @abstractmethod
    def delete_where(self, table: str, predicate) -> int:
        """
        条件删除
        Args:
            table: 表名
            predicate: 谓词函数 row -> bool
        Returns:
            删除行数
        """
        pass


class ICursor:
    """游标接口 - 支持分页"""

    def __init__(self, iterator: Iterator):
        self.iterator = iterator
        self.finished = False

    def fetchmany(self, size: int) -> List[Dict]:
        """
        获取指定数量的行
        Args:
            size: 行数
        Returns:
            行列表
        """
        result = []
        try:
            for _ in range(size):
                result.append(next(self.iterator))
        except StopIteration:
            self.finished = True
        return result

    def fetchall(self) -> List[Dict]:
        """获取所有剩余行"""
        try:
            result = list(self.iterator)
            self.finished = True
            return result
        except StopIteration:
            self.finished = True
            return []


class IExecutor(ABC):
    """执行器接口 - Phase 0接口稳定点"""

    @abstractmethod
    def run(self, plan: Plan) -> Any:
        """
        执行计划（一次性）
        Args:
            plan: 执行计划
        Returns:
            执行结果（查询返回行列表，其他返回成功信息）
        """
        pass

    @abstractmethod
    def cursor(self, plan: Plan) -> ICursor:
        """
        创建游标（分页执行）
        Args:
            plan: 执行计划
        Returns:
            游标对象
        """
        pass


# ==================== 占位实现（Phase 0基线闭环） ====================

class MockPlanner(IPlanner):
    """占位计划生成器 - 最小可运行实现"""

    def plan(self, sql_text: str) -> Plan:
        """简单占位实现"""
        # TODO: A1-A4阶段将替换为完整实现
        sql_upper = sql_text.strip().upper()

        if sql_upper.startswith('CREATE TABLE'):
            return Plan({"op": "CreateTable", "sql": sql_text})
        elif sql_upper.startswith('INSERT'):
            return Plan({"op": "Insert", "sql": sql_text})
        elif sql_upper.startswith('SELECT'):
            return Plan({"op": "Select", "sql": sql_text})
        elif sql_upper.startswith('DELETE'):
            return Plan({"op": "Delete", "sql": sql_text})
        else:
            raise SqlError("SyntaxError", 1, 1, f"Unsupported SQL: {sql_text}")


class MockStorageEngine(IStorageEngine):
    """占位存储引擎 - 内存实现"""

    def __init__(self):
        self.tables = {}  # table_name -> {"columns": [...], "rows": [...]}

    def create_table(self, name: str, columns: List[Dict]) -> None:
        if name in self.tables:
            raise SqlError("RuntimeError", 1, 1, f"Table {name} already exists")
        self.tables[name] = {"columns": columns, "rows": []}

    def insert_row(self, table: str, row: Dict) -> None:
        if table not in self.tables:
            raise SqlError("RuntimeError", 1, 1, f"Table {table} does not exist")
        self.tables[table]["rows"].append(row)

    def seq_scan(self, table: str) -> Iterator[Dict]:
        if table not in self.tables:
            raise SqlError("RuntimeError", 1, 1, f"Table {table} does not exist")
        return iter(self.tables[table]["rows"])

    def delete_where(self, table: str, predicate) -> int:
        if table not in self.tables:
            raise SqlError("RuntimeError", 1, 1, f"Table {table} does not exist")

        rows = self.tables[table]["rows"]
        original_count = len(rows)
        self.tables[table]["rows"] = [row for row in rows if not predicate(row)]
        return original_count - len(self.tables[table]["rows"])


class MockExecutor(IExecutor):
    """占位执行器 - 最小实现"""

    def __init__(self, storage_engine: IStorageEngine):
        self.storage = storage_engine

    def run(self, plan: Plan) -> Any:
        plan_dict = plan.to_dict()
        op = plan_dict.get("op")

        if op == "CreateTable":
            return f"MockExecutor: Created table (plan: {plan.to_json()})"
        elif op == "Insert":
            return f"MockExecutor: Inserted row (plan: {plan.to_json()})"
        elif op == "Select":
            return [{"mock": "result", "plan": plan_dict}]
        elif op == "Delete":
            return f"MockExecutor: Deleted rows (plan: {plan.to_json()})"
        else:
            raise SqlError("RuntimeError", 1, 1, f"Unknown operation: {op}")

    def cursor(self, plan: Plan) -> ICursor:
        # 简单实现：先run再包装成游标
        result = self.run(plan)
        if isinstance(result, list):
            return ICursor(iter(result))
        else:
            return ICursor(iter([{"message": str(result)}]))


# ==================== 测试函数 ====================

def test_interfaces():
    """测试核心接口的占位实现"""
    print("=== Testing Core Interfaces (Phase 0) ===")

    # 测试存储引擎
    storage = MockStorageEngine()
    storage.create_table("test", [{"name": "id", "type": "INT"}])
    storage.insert_row("test", {"id": 1})

    rows = list(storage.seq_scan("test"))
    print(f"Storage test: {rows}")

    # 测试计划生成器
    planner = MockPlanner()
    plan = planner.plan("SELECT * FROM test")
    print(f"Planner test: {plan.to_json()}")

    # 测试执行器
    executor = MockExecutor(storage)
    result = executor.run(plan)
    print(f"Executor test: {result}")

    # 测试游标
    cursor = executor.cursor(plan)
    rows = cursor.fetchmany(2)
    print(f"Cursor test: {rows}")

    print("Phase 0 interfaces working!")


if __name__ == "__main__":
    test_interfaces()