"""
SQL语义分析器 - A3阶段核心实现
负责表/列存在性检查、类型一致性检查、列数匹配验证

【设计原理】
1. Catalog管理：内存中维护表结构信息
2. 语义检查：对AST进行语义验证
3. 错误定位：精确到行列的错误报告
4. 类型系统：支持INT、VARCHAR基本类型

【检查项目】
- CREATE TABLE：表重复创建检查
- INSERT：表存在性、列数匹配、类型匹配
- SELECT：表存在性、列存在性
- DELETE：表存在性、WHERE条件列检查

【Catalog结构】
tables = {
    "table_name": {
        "columns": [
            {"name": "col1", "type": "INT"},
            {"name": "col2", "type": "VARCHAR"}
        ]
    }
}

【错误格式】
SemanticError(error_type="SemanticError", line=1, col=5, hint="详细原因")
"""

import sys
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
    from sql.lexer import SqlError
else:
    from .parser import (
        Parser, ASTNode, CreateTableNode, InsertNode, SelectNode,
        DeleteNode, ColumnDefNode, ValueNode, ColumnNode,
        BinaryOpNode, WhereClauseNode, ParseError
    )
    from .lexer import SqlError


class SemanticError(SqlError):
    """语义分析错误"""

    def __init__(self, line: int, col: int, hint: str):
        super().__init__("SemanticError", line, col, hint)


class TableInfo:
    """表信息"""

    def __init__(self, name: str, columns: List[Dict[str, str]]):
        self.name = name
        self.columns = columns  # [{"name": "id", "type": "INT"}, ...]

    def get_column(self, col_name: str) -> Optional[Dict[str, str]]:
        """获取列信息"""
        for col in self.columns:
            if col["name"].lower() == col_name.lower():
                return col
        return None

    def has_column(self, col_name: str) -> bool:
        """检查列是否存在"""
        return self.get_column(col_name) is not None

    def get_column_names(self) -> List[str]:
        """获取所有列名"""
        return [col["name"] for col in self.columns]


class Catalog:
    """系统目录 - 内存实现"""

    def __init__(self):
        self.tables: Dict[str, TableInfo] = {}

    def create_table(self, name: str, columns: List[Dict[str, str]]) -> None:
        """创建表"""
        if self.table_exists(name):
            raise ValueError(f"Table '{name}' already exists")

        self.tables[name.lower()] = TableInfo(name, columns)

    def table_exists(self, name: str) -> bool:
        """检查表是否存在"""
        return name.lower() in self.tables

    def get_table(self, name: str) -> Optional[TableInfo]:
        """获取表信息"""
        return self.tables.get(name.lower())

    def drop_table(self, name: str) -> bool:
        """删除表"""
        if name.lower() in self.tables:
            del self.tables[name.lower()]
            return True
        return False

    def list_tables(self) -> List[str]:
        """列出所有表名"""
        return [table.name for table in self.tables.values()]

    def get_stats(self) -> Dict:
        """获取目录统计信息"""
        total_columns = sum(len(table.columns) for table in self.tables.values())
        return {
            "table_count": len(self.tables),
            "total_columns": total_columns,
            "tables": {name: len(table.columns) for name, table in self.tables.items()}
        }


class SemanticAnalyzer:
    """语义分析器"""

    def __init__(self, catalog: Catalog = None):
        self.catalog = catalog if catalog else Catalog()
        self.errors: List[SemanticError] = []

    def analyze(self, ast: ASTNode) -> Dict[str, Any]:
        """
        语义分析主函数
        Args:
            ast: 语法树根节点
        Returns:
            分析结果字典
        Raises:
            SemanticError: 语义错误
        """
        self.errors.clear()

        try:
            if isinstance(ast, CreateTableNode):
                return self._analyze_create_table(ast)
            elif isinstance(ast, InsertNode):
                return self._analyze_insert(ast)
            elif isinstance(ast, SelectNode):
                return self._analyze_select(ast)
            elif isinstance(ast, DeleteNode):
                return self._analyze_delete(ast)
            else:
                raise SemanticError(ast.line, ast.col, f"Unsupported statement type: {type(ast).__name__}")

        except SemanticError:
            raise
        except Exception as e:
            raise SemanticError(ast.line, ast.col, f"Semantic analysis error: {str(e)}")

    def _analyze_create_table(self, node: CreateTableNode) -> Dict[str, Any]:
        """分析CREATE TABLE语句"""
        table_name = node.table_name

        # 检查表是否已存在
        if self.catalog.table_exists(table_name):
            raise SemanticError(node.line, node.col,
                                f"Table '{table_name}' already exists")

        # 检查列定义
        columns = []
        column_names = set()

        # 在这个位置添加长度解析逻辑：
        for col_def in node.columns:
            col_name = col_def.name
            col_type = col_def.data_type

            # 检查列名重复
            if col_name.lower() in column_names:
                raise SemanticError(col_def.line, col_def.col,
                                    f"Duplicate column name '{col_name}'")

            column_names.add(col_name.lower())

            # 验证数据类型并解析长度 -- 新增代码
            col_info = {"name": col_name, "type": col_type}

            if col_type.startswith("VARCHAR("):
                # 解析VARCHAR(50) -> type="VARCHAR", max_length=50
                import re
                match = re.match(r'VARCHAR\((\d+)\)', col_type)
                if match:
                    max_length = int(match.group(1))
                    col_info = {"name": col_name, "type": "VARCHAR", "max_length": max_length}
                else:
                    raise SemanticError(col_def.line, col_def.col, f"Invalid VARCHAR format: {col_type}")
            elif not self._is_valid_type(col_type):
                raise SemanticError(col_def.line, col_def.col, f"Invalid data type '{col_type}'")

            columns.append(col_info)

        # 检查是否至少有一个列
        if not columns:
            raise SemanticError(node.line, node.col,
                                "Table must have at least one column")

        # 创建表（更新catalog）
        self.catalog.create_table(table_name, columns)

        return {
            "statement_type": "CREATE_TABLE",
            "table_name": table_name,
            "columns": columns,
            "semantic_checks": "passed"
        }

    def _analyze_insert(self, node: InsertNode) -> Dict[str, Any]:
        """分析INSERT语句"""
        table_name = node.table_name

        # 检查表是否存在
        table = self.catalog.get_table(table_name)
        if not table:
            raise SemanticError(node.line, node.col,
                                f"Table '{table_name}' does not exist")

        # 确定目标列
        if node.columns:
            # 指定了列名
            target_columns = []
            for col_name in node.columns:
                if not table.has_column(col_name):
                    raise SemanticError(node.line, node.col,
                                        f"Column '{col_name}' does not exist in table '{table_name}'")
                target_columns.append(table.get_column(col_name))
        else:
            # 未指定列名，使用所有列
            target_columns = table.columns

        # 检查值的数量
        if len(node.values) != len(target_columns):
            raise SemanticError(node.line, node.col,
                                f"Column count mismatch: expected {len(target_columns)}, got {len(node.values)}")

        # 检查类型兼容性
        for i, (value_node, col_info) in enumerate(zip(node.values, target_columns)):
            if not self._is_type_compatible(value_node, col_info["type"]):
                raise SemanticError(value_node.line, value_node.col,
                                    f"Type mismatch: cannot insert {value_node.value_type} into {col_info['type']} column '{col_info['name']}'")

        return {
            "statement_type": "INSERT",
            "table_name": table_name,
            "target_columns": [col["name"] for col in target_columns],
            "value_count": len(node.values),
            "semantic_checks": "passed"
        }

    def _analyze_select(self, node: SelectNode) -> Dict[str, Any]:
        """分析SELECT语句"""
        table_name = node.table_name

        # 检查表是否存在
        table = self.catalog.get_table(table_name)
        if not table:
            raise SemanticError(node.line, node.col,
                                f"Table '{table_name}' does not exist")

        # 检查选择的列
        selected_columns = []

        if len(node.columns) == 1 and node.columns[0] == "*":
            # SELECT *
            selected_columns = table.get_column_names()
        else:
            # 指定列名
            for col in node.columns:
                if isinstance(col, ColumnNode):
                    col_name = col.name
                    if not table.has_column(col_name):
                        raise SemanticError(col.line, col.col,
                                            f"Column '{col_name}' does not exist in table '{table_name}'")
                    selected_columns.append(col_name)
                elif isinstance(col, str):
                    # 处理字符串形式的列名
                    if not table.has_column(col):
                        raise SemanticError(node.line, node.col,
                                            f"Column '{col}' does not exist in table '{table_name}'")
                    selected_columns.append(col)

        # 检查WHERE子句
        where_info = None
        if node.where_clause:
            where_info = self._analyze_where_clause(node.where_clause, table)

        return {
            "statement_type": "SELECT",
            "table_name": table_name,
            "selected_columns": selected_columns,
            "where_clause": where_info,
            "semantic_checks": "passed"
        }

    def _analyze_delete(self, node: DeleteNode) -> Dict[str, Any]:
        """分析DELETE语句"""
        table_name = node.table_name

        # 检查表是否存在
        table = self.catalog.get_table(table_name)
        if not table:
            raise SemanticError(node.line, node.col,
                                f"Table '{table_name}' does not exist")

        # 检查WHERE子句
        where_info = None
        if node.where_clause:
            where_info = self._analyze_where_clause(node.where_clause, table)

        return {
            "statement_type": "DELETE",
            "table_name": table_name,
            "where_clause": where_info,
            "semantic_checks": "passed"
        }

    def _analyze_where_clause(self, where_node: WhereClauseNode, table: TableInfo) -> Dict[str, Any]:
        """分析WHERE子句"""
        condition = where_node.condition

        if isinstance(condition, BinaryOpNode):
            # 检查左右操作数
            left_info = self._analyze_expression(condition.left, table)
            right_info = self._analyze_expression(condition.right, table)

            # 检查操作符兼容性
            if not self._is_operator_compatible(condition.operator, left_info, right_info):
                raise SemanticError(condition.line, condition.col,
                                    f"Incompatible types for operator '{condition.operator}'")

            return {
                "type": "binary_op",
                "operator": condition.operator,
                "left": left_info,
                "right": right_info
            }
        else:
            return self._analyze_expression(condition, table)

    def _analyze_expression(self, expr: ASTNode, table: TableInfo) -> Dict[str, Any]:
        """分析表达式"""
        if isinstance(expr, ColumnNode):
            col_name = expr.name
            if not table.has_column(col_name):
                raise SemanticError(expr.line, expr.col,
                                    f"Column '{col_name}' does not exist in table '{table.name}'")

            col_info = table.get_column(col_name)
            return {
                "type": "column",
                "name": col_name,
                "data_type": col_info["type"]
            }

        elif isinstance(expr, ValueNode):
            return {
                "type": "value",
                "value": expr.value,
                "data_type": expr.value_type
            }

        else:
            raise SemanticError(expr.line, expr.col,
                                f"Unsupported expression type: {type(expr).__name__}")

    def _is_valid_type(self, type_name: str) -> bool:
        """检查数据类型是否有效"""
        valid_types = {"INT", "INTEGER", "VARCHAR", "CHAR", "TEXT"}
        return type_name.upper() in valid_types

    def _is_type_compatible(self, value_node: ValueNode, target_type: str) -> bool:
        """检查值类型与目标类型是否兼容"""
        value_type = value_node.value_type
        target_type = target_type.upper()

        if value_type == "NULL":
            return True  # NULL可以插入任何类型

        if target_type in ["INT", "INTEGER"]:
            return value_type == "NUMBER" and isinstance(value_node.value, int)

        if target_type in ["VARCHAR", "CHAR", "TEXT"]:
            return value_type == "STRING"

        return False

    def _is_operator_compatible(self, operator: str, left_info: Dict, right_info: Dict) -> bool:
        """检查操作符两边的类型是否兼容"""
        left_type = left_info.get("data_type", "").upper()
        right_type = right_info.get("data_type", "").upper()

        # 比较操作符
        if operator in ["=", "!=", "<>", "<", ">", "<=", ">="]:
            # 同类型比较
            if left_type == right_type:
                return True
            # INT和NUMBER可以比较
            if {left_type, right_type}.issubset({"INT", "INTEGER", "NUMBER"}):
                return True

        return False


def analyze_sql(sql_text: str, catalog: Catalog = None) -> Dict[str, Any]:
    """
    完整的SQL语义分析流程
    Args:
        sql_text: SQL语句
        catalog: 系统目录（可选）
    Returns:
        分析结果
    """
    # 语法分析
    parser = Parser()
    ast = parser.parse(sql_text)

    # 语义分析
    analyzer = SemanticAnalyzer(catalog)
    result = analyzer.analyze(ast)

    return result


def format_semantic_result(result: Dict[str, Any]) -> str:
    """格式化语义分析结果"""
    lines = ["=== 语义分析结果 ==="]
    lines.append(f"语句类型: {result['statement_type']}")

    if result['statement_type'] == 'CREATE_TABLE':
        lines.append(f"表名: {result['table_name']}")
        lines.append("列定义:")
        for col in result['columns']:
            lines.append(f"  - {col['name']}: {col['type']}")

    elif result['statement_type'] == 'INSERT':
        lines.append(f"目标表: {result['table_name']}")
        lines.append(f"目标列: {', '.join(result['target_columns'])}")
        lines.append(f"值数量: {result['value_count']}")

    elif result['statement_type'] == 'SELECT':
        lines.append(f"查询表: {result['table_name']}")
        lines.append(f"选择列: {', '.join(result['selected_columns'])}")
        if result['where_clause']:
            lines.append("WHERE条件: 已验证")

    elif result['statement_type'] == 'DELETE':
        lines.append(f"目标表: {result['table_name']}")
        if result['where_clause']:
            lines.append("WHERE条件: 已验证")

    lines.append(f"语义检查: {result['semantic_checks']}")

    return "\n".join(lines)


def test_semantic_analyzer():
    """测试语义分析器"""
    print("=== Testing Semantic Analyzer (A3) ===")

    catalog = Catalog()
    analyzer = SemanticAnalyzer(catalog)

    test_cases = [
        # 正确用例
        ("CREATE TABLE student(id INT, name VARCHAR);", "建表"),
        ("INSERT INTO student VALUES(1, 'Alice');", "插入数据"),
        ("SELECT id, name FROM student;", "查询指定列"),
        ("SELECT * FROM student WHERE id > 0;", "条件查询"),
        ("DELETE FROM student WHERE id = 1;", "条件删除"),

        # 错误用例
        ("CREATE TABLE student(id INT, name VARCHAR);", "重复建表"),
        ("INSERT INTO nonexistent VALUES(1);", "表不存在"),
        ("INSERT INTO student VALUES(1);", "列数不匹配"),
        ("INSERT INTO student VALUES('Alice', 1);", "类型不匹配"),
        ("SELECT nonexistent FROM student;", "列不存在"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            parser = Parser()
            ast = parser.parse(sql)
            result = analyzer.analyze(ast)
            print("✓ 语义分析通过")
            print(format_semantic_result(result))
        except (SemanticError, ParseError) as e:
            print(f"❌ {e.error_type}: {e.hint}")
            print(f"   位置: 第{e.line}行第{e.col}列")

    print(f"\n=== Catalog状态 ===")
    stats = catalog.get_stats()
    print(f"表数量: {stats['table_count']}")
    print(f"总列数: {stats['total_columns']}")
    for table_name, col_count in stats['tables'].items():
        print(f"  {table_name}: {col_count}列")


if __name__ == "__main__":
    test_semantic_analyzer()