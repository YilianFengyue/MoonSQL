"""
SQL语法分析器 - A2阶段核心实现
递归下降解析器，支持CREATE/INSERT/SELECT/DELETE四类语句
"""

import sys
from typing import List, Optional, Any, Dict, Union
from pathlib import Path

# 导入词法分析器
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from sql.lexer import Lexer, Token, TokenType, SqlError
else:
    from .lexer import Lexer, Token, TokenType, SqlError

# ==================== AST节点定义 ====================

class ASTNode:
    """AST节点基类"""
    def __init__(self, line: int = 0, col: int = 0):
        self.line = line
        self.col = col

    def to_dict(self) -> Dict:
        """转换为字典表示"""
        result = {"type": self.__class__.__name__}
        for key, value in self.__dict__.items():
            if key not in ['line', 'col']:
                if isinstance(value, ASTNode):
                    result[key] = value.to_dict()
                elif isinstance(value, list):
                    result[key] = [item.to_dict() if isinstance(item, ASTNode) else item for item in value]
                else:
                    result[key] = value
        return result

class ColumnDefNode(ASTNode):
    """列定义节点"""
    def __init__(self, name: str, data_type: str, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.name = name
        self.data_type = data_type

class CreateTableNode(ASTNode):
    """CREATE TABLE语句节点"""
    def __init__(self, table_name: str, columns: List[ColumnDefNode], line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.columns = columns

class ValueNode(ASTNode):
    """值节点"""
    def __init__(self, value: Union[str, int, float, None], value_type: str, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.value = value
        self.value_type = value_type

class ColumnNode(ASTNode):
    """列名节点"""
    def __init__(self, name: str, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.name = name

class InsertNode(ASTNode):
    """INSERT语句节点"""
    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[ValueNode], line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.columns = columns
        self.values = values

class BinaryOpNode(ASTNode):
    """二元操作符节点"""
    def __init__(self, left: ASTNode, operator: str, right: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.left = left
        self.operator = operator
        self.right = right

class WhereClauseNode(ASTNode):
    """WHERE子句节点"""
    def __init__(self, condition: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.condition = condition

class SelectNode(ASTNode):
    """SELECT语句节点"""
    def __init__(self, columns: List[Union[ColumnNode, str]], table_name: str, where_clause: Optional[WhereClauseNode] = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.columns = columns
        self.table_name = table_name
        self.where_clause = where_clause

class DeleteNode(ASTNode):
    """DELETE语句节点"""
    def __init__(self, table_name: str, where_clause: Optional[WhereClauseNode] = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.where_clause = where_clause

# ==================== 语法分析器 ====================

class ParseError(SqlError):
    """语法分析错误"""
    def __init__(self, line: int, col: int, message: str, expected: str = None):
        self.expected = expected
        hint = message
        if expected:
            hint = f"{message}, expected: {expected}"
        super().__init__("SyntaxError", line, col, hint)

class Parser:
    """SQL语法分析器"""

    def __init__(self):
        self.tokens: List[Token] = []
        self.current = 0

    def parse(self, sql_text: str) -> ASTNode:
        """解析SQL语句生成AST"""
        # 先进行词法分析
        lexer = Lexer()
        self.tokens = lexer.tokenize(sql_text)
        self.current = 0

        # 解析语句
        try:
            stmt = self._parse_statement()

            # 检查是否还有未处理的token
            if not self._is_at_end() and not self._check(TokenType.EOF):
                current_token = self._peek()
                raise ParseError(current_token.line, current_token.col,
                               f"Unexpected token '{current_token.lexeme}'")

            return stmt

        except ParseError:
            raise
        except Exception as e:
            current_token = self._peek() if not self._is_at_end() else self.tokens[-1]
            raise ParseError(current_token.line, current_token.col, f"Parse error: {str(e)}")

    def _parse_statement(self) -> ASTNode:
        """解析语句"""
        if self._match(TokenType.KEYWORD):
            keyword = self._previous().lexeme.upper()

            if keyword == "CREATE":
                return self._parse_create_table()
            elif keyword == "INSERT":
                return self._parse_insert()
            elif keyword == "SELECT":
                return self._parse_select()
            elif keyword == "DELETE":
                return self._parse_delete()
            else:
                raise ParseError(self._previous().line, self._previous().col,
                               f"Unsupported statement: {keyword}")
        else:
            current = self._peek()
            raise ParseError(current.line, current.col,
                           "Expected SQL statement", "CREATE, INSERT, SELECT, or DELETE")

    def _parse_create_table(self) -> CreateTableNode:
        """解析CREATE TABLE语句"""
        # CREATE已经匹配，现在期望TABLE
        self._consume(TokenType.KEYWORD, "TABLE", "Expected 'TABLE'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 左括号
        self._consume(TokenType.DELIMITER, "(", "Expected '(' after table name")

        # 列定义列表
        columns = []

        # 至少要有一个列定义
        columns.append(self._parse_column_def())

        # 处理更多列定义
        while True:
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号
                columns.append(self._parse_column_def())
            else:
                break

        # 右括号
        self._consume(TokenType.DELIMITER, ")", "Expected ')' after column definitions")

        # 分号
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return CreateTableNode(table_name, columns, table_token.line, table_token.col)

    # 修改为：
    def _parse_column_def(self) -> ColumnDefNode:
        # 列名
        name_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
        name = name_token.lexeme

        # 数据类型
        type_token = self._consume(TokenType.KEYWORD, None, "Expected data type")
        data_type = type_token.lexeme.upper()

        # 支持VARCHAR(n)
        if data_type == "VARCHAR":
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == "(":
                self._advance()  # 消费左括号
                size_token = self._consume(TokenType.NUMBER, None, "Expected size after VARCHAR(")
                self._consume(TokenType.DELIMITER, ")", "Expected ')' after VARCHAR size")

                # 保存完整类型信息，包含长度
                data_type = f"VARCHAR({size_token.lexeme})"

        return ColumnDefNode(name, data_type, name_token.line, name_token.col)

    def _parse_insert(self) -> InsertNode:
        """解析INSERT语句"""
        # INSERT已经匹配，期望INTO
        self._consume(TokenType.KEYWORD, "INTO", "Expected 'INTO'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 可选的列名列表
        columns = None
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == "(":
            self._advance()  # 消费左括号
            columns = []

            # 至少一个列名
            col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
            columns.append(col_token.lexeme)

            # 处理更多列名
            while True:
                if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                    self._advance()  # 消费逗号
                    col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
                    columns.append(col_token.lexeme)
                else:
                    break

            self._consume(TokenType.DELIMITER, ")", "Expected ')' after column list")

        # VALUES关键字
        self._consume(TokenType.KEYWORD, "VALUES", "Expected 'VALUES'")

        # 值列表
        self._consume(TokenType.DELIMITER, "(", "Expected '(' before values")

        values = []
        values.append(self._parse_value())

        # 处理更多值
        while True:
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号
                values.append(self._parse_value())
            else:
                break

        self._consume(TokenType.DELIMITER, ")", "Expected ')' after values")
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return InsertNode(table_name, columns, values, table_token.line, table_token.col)

    def _parse_select(self) -> SelectNode:
        """解析SELECT语句"""
        select_token = self._previous()  # SELECT已经匹配

        # 列列表
        columns = []

        if self._check(TokenType.OPERATOR) and self._peek().lexeme == "*":
            self._advance()  # 消费*
            columns.append("*")
        else:
            # 至少一个列名
            col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name or '*'")
            columns.append(ColumnNode(col_token.lexeme, col_token.line, col_token.col))

            # 处理更多列名
            while True:
                if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                    self._advance()  # 消费逗号
                    col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
                    columns.append(ColumnNode(col_token.lexeme, col_token.line, col_token.col))
                else:
                    break

        # FROM关键字
        self._consume(TokenType.KEYWORD, "FROM", "Expected 'FROM'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 可选的WHERE子句
        where_clause = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "WHERE":
            self._advance()  # 消费WHERE
            condition = self._parse_expression()
            where_clause = WhereClauseNode(condition)

        # 分号
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return SelectNode(columns, table_name, where_clause, select_token.line, select_token.col)

    def _parse_delete(self) -> DeleteNode:
        """解析DELETE语句"""
        delete_token = self._previous()  # DELETE已经匹配

        # FROM关键字
        self._consume(TokenType.KEYWORD, "FROM", "Expected 'FROM'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 可选的WHERE子句
        where_clause = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "WHERE":
            self._advance()  # 消费WHERE
            condition = self._parse_expression()
            where_clause = WhereClauseNode(condition)

        # 分号
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return DeleteNode(table_name, where_clause, delete_token.line, delete_token.col)

    def _parse_expression(self) -> ASTNode:
        """解析表达式"""
        left = self._parse_primary()

        if self._check(TokenType.OPERATOR):
            op_token = self._advance()
            operator = op_token.lexeme
            right = self._parse_primary()
            return BinaryOpNode(left, operator, right, op_token.line, op_token.col)

        return left

    def _parse_primary(self) -> ASTNode:
        """解析基本表达式"""
        if self._check(TokenType.NUMBER):
            token = self._advance()
            try:
                value = int(token.lexeme)
            except ValueError:
                value = float(token.lexeme)
            return ValueNode(value, "NUMBER", token.line, token.col)

        if self._check(TokenType.STRING):
            token = self._advance()
            return ValueNode(token.lexeme, "STRING", token.line, token.col)

        if self._check(TokenType.IDENTIFIER):
            token = self._advance()
            return ColumnNode(token.lexeme, token.line, token.col)

        current = self._peek()
        raise ParseError(current.line, current.col, "Expected expression", "number, string, or identifier")

    def _parse_value(self) -> ValueNode:
        """解析值"""
        if self._check(TokenType.NUMBER):
            token = self._advance()
            try:
                value = int(token.lexeme)
            except ValueError:
                value = float(token.lexeme)
            return ValueNode(value, "NUMBER", token.line, token.col)

        if self._check(TokenType.STRING):
            token = self._advance()
            return ValueNode(token.lexeme, "STRING", token.line, token.col)

        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "NULL":
            token = self._advance()
            return ValueNode(None, "NULL", token.line, token.col)

        current = self._peek()
        raise ParseError(current.line, current.col, "Expected value", "number, string, or NULL")

    # ==================== 辅助方法 ====================

    def _match(self, *types: TokenType) -> bool:
        """检查当前token是否匹配指定类型"""
        for token_type in types:
            if self._check(token_type):
                self._advance()
                return True
        return False

    def _check(self, token_type: TokenType) -> bool:
        """检查当前token类型"""
        if self._is_at_end():
            return False
        return self._peek().type == token_type

    def _advance(self) -> Token:
        """前进到下一个token"""
        if not self._is_at_end():
            self.current += 1
        return self._previous()

    def _is_at_end(self) -> bool:
        """是否到达token流结尾"""
        return self.current >= len(self.tokens) or self._peek().type == TokenType.EOF

    def _peek(self) -> Token:
        """获取当前token"""
        if self.current >= len(self.tokens):
            return self.tokens[-1]  # EOF token
        return self.tokens[self.current]

    def _previous(self) -> Token:
        """获取前一个token"""
        return self.tokens[self.current - 1]

    def _consume(self, token_type: TokenType, lexeme: str = None, error_message: str = "Unexpected token") -> Token:
        """消费指定类型的token"""
        if self._check(token_type):
            if lexeme is None or self._peek().lexeme.upper() == lexeme.upper():
                return self._advance()

        current = self._peek()
        expected = f"{token_type.value}"
        if lexeme:
            expected = f"'{lexeme}'"

        raise ParseError(current.line, current.col, error_message, expected)

# ==================== AST格式化输出 ====================

def format_ast(ast: ASTNode, indent: int = 0) -> str:
    """格式化AST为树形字符串"""
    prefix = "  " * indent
    result = [f"{prefix}{ast.__class__.__name__}"]

    for key, value in ast.__dict__.items():
        if key in ['line', 'col']:
            continue
        if isinstance(value, ASTNode):
            result.append(f"{prefix}├─ {key}:")
            result.append(format_ast(value, indent + 1))
        elif isinstance(value, list):
            result.append(f"{prefix}├─ {key}: [")
            for item in value:
                if isinstance(item, ASTNode):
                    result.append(format_ast(item, indent + 1))
                else:
                    result.append(f"{prefix}  {item}")
            result.append(f"{prefix}]")
        else:
            result.append(f"{prefix}├─ {key}: {value}")

    return "\n".join(result)

def test_parser():
    """测试语法分析器"""
    print("=== Testing SQL Parser (A2) ===")

    test_cases = [
        ("CREATE TABLE student(id INT, name VARCHAR);", "CREATE TABLE"),
        ("INSERT INTO student VALUES(1, 'Alice');", "INSERT"),
        ("SELECT id, name FROM student;", "SELECT"),
        ("SELECT * FROM student WHERE id > 0;", "SELECT with WHERE"),
        ("DELETE FROM student WHERE id = 1;", "DELETE"),
    ]

    parser = Parser()

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功")
            print("AST:")
            print(format_ast(ast))
        except ParseError as e:
            print(f"❌ 语法错误: {e.hint}")

if __name__ == "__main__":
    test_parser()