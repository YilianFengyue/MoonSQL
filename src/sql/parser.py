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
    """列定义节点（支持约束）"""
    def __init__(self, name: str, data_type: str, constraints: Dict[str, Any] = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.name = name
        self.data_type = data_type
        self.constraints = constraints or {}  # ★ 新增：{"primary_key": True, "not_null": True, "unique": True, "default": value}

class CreateTableNode(ASTNode):
    """CREATE TABLE语句节点"""
    def __init__(self, table_name: str, columns: List[ColumnDefNode],
                 table_constraints: List['ForeignKeyNode'] = None, line: int = 0, col: int = 0):  # ★ 新增参数
        super().__init__(line, col)
        self.table_name = table_name
        self.columns = columns
        self.table_constraints = table_constraints or []  # ★ 新增：表级约束

# ★ 新增：外键约束节点
class ForeignKeyNode(ASTNode):
    """外键约束节点"""
    def __init__(self, column_name: str, ref_table: str, ref_column: str,
                 constraint_name: str = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.column_name = column_name
        self.ref_table = ref_table
        self.ref_column = ref_column
        self.constraint_name = constraint_name

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
class UpdateNode(ASTNode):
    """UPDATE语句节点"""
    def __init__(self, table_name: str, set_clauses: List[Dict[str, Any]], where_clause: Optional[WhereClauseNode] = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.set_clauses = set_clauses  # [{"column": "name", "value": ValueNode}, ...]
        self.where_clause = where_clause

class ShowTablesNode(ASTNode):
    """SHOW TABLES 语句"""
    pass


class DescTableNode(ASTNode):
    """DESC 表结构"""
    def __init__(self, table_name: str, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name


class AlterTableNode(ASTNode):
    """
    ALTER TABLE 语句
    action: one of ['RENAME', 'ADD_COLUMN', 'DROP_COLUMN', 'MODIFY_COLUMN', 'CHANGE_COLUMN']
    payload: dict, 视 action 而定：
        RENAME: {"new_name": str}
        ADD_COLUMN: {"name": str, "type": str}
        DROP_COLUMN: {"name": str}
        MODIFY_COLUMN: {"name": str, "type": str}
        CHANGE_COLUMN: {"old_name": str, "new_name": str, "type": str}
    """
    def __init__(self, table_name: str, action: str, payload: Dict[str, Any], line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.action = action
        self.payload = payload

# ★ 新增：别名列节点
class AliasColumnNode(ASTNode):
    """带别名的列节点"""
    def __init__(self, column_name: str, alias: str, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.column_name = column_name
        self.alias = alias

# ★ 新增：复杂表达式节点
class InNode(ASTNode):
    """IN表达式节点"""
    def __init__(self, left: ASTNode, values: List[Any], is_not: bool = False, subquery: ASTNode = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.left = left
        self.values = values  # 常量列表
        self.subquery = subquery  # 子查询
        self.is_not = is_not  # NOT IN

class BetweenNode(ASTNode):
    """BETWEEN表达式节点"""
    def __init__(self, expr: ASTNode, min_val: ASTNode, max_val: ASTNode, is_not: bool = False, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.expr = expr
        self.min_val = min_val
        self.max_val = max_val
        self.is_not = is_not

class LikeNode(ASTNode):
    """LIKE表达式节点"""
    def __init__(self, left: ASTNode, pattern: ASTNode, is_not: bool = False, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.left = left
        self.pattern = pattern
        self.is_not = is_not

class IsNullNode(ASTNode):
    """IS NULL表达式节点"""
    def __init__(self, expr: ASTNode, is_not: bool = False, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.expr = expr
        self.is_not = is_not

class LogicalOpNode(ASTNode):
    """逻辑操作符节点(AND/OR)"""
    def __init__(self, left: ASTNode, operator: str, right: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.left = left
        self.operator = operator  # "AND" or "OR"
        self.right = right

class NotNode(ASTNode):
    """NOT操作符节点"""
    def __init__(self, expr: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.expr = expr

# ★ 修改：扩展SelectNode支持DISTINCT
class SelectNode(ASTNode):
    """SELECT语句节点（支持DISTINCT和别名）"""
    def __init__(self, columns: List[Union[ColumnNode, AliasColumnNode, str]], table_name: str,
                 distinct: bool = False, where_clause: Optional[WhereClauseNode] = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.columns = columns
        self.table_name = table_name
        self.distinct = distinct  # ★ 新增：是否DISTINCT
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
            elif keyword == "SHOW":
                return self._parse_show_tables()
            elif keyword == "ALTER":
                return self._parse_alter_table()
            elif keyword == "DESC":
                return self._parse_desc()
            elif keyword == "UPDATE":
                return self._parse_update()
            else:
                raise ParseError(self._previous().line, self._previous().col,
                                 f"Unsupported statement: {keyword}")
        else:
            current = self._peek()
            raise ParseError(current.line, current.col,
                             "Expected SQL statement", "CREATE, INSERT, SELECT, DELETE, SHOW, ALTER, or DESC")

    def _parse_create_table(self) -> CreateTableNode:
        """解析CREATE TABLE语句 - ★ 支持外键约束"""
        # CREATE已经匹配，现在期望TABLE
        self._consume(TokenType.KEYWORD, "TABLE", "Expected 'TABLE'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 左括号
        self._consume(TokenType.DELIMITER, "(", "Expected '(' after table name")

        # ★ 新增：解析列定义和表级约束
        columns = []
        table_constraints = []
        # 至少要有一个列定义或约束
        if self._check_foreign_key_start():
            table_constraints.append(self._parse_foreign_key_constraint())
        else:
            columns.append(self._parse_column_def())

        # 处理更多列定义和约束
        while True:
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号

                # ★ 检查下一个是外键约束还是列定义
                if self._check_foreign_key_start():
                    table_constraints.append(self._parse_foreign_key_constraint())
                else:
                    columns.append(self._parse_column_def())
            else:
                break

        # 右括号
        self._consume(TokenType.DELIMITER, ")", "Expected ')' after column definitions")

        # 分号
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return CreateTableNode(table_name, columns, table_constraints, table_token.line, table_token.col)  # ★ 传递约束

    # ★ 新增：外键检查和解析方法
    def _check_foreign_key_start(self) -> bool:
        """检查是否是外键约束的开始"""
        if self._check(TokenType.KEYWORD):
            keyword = self._peek().lexeme.upper()
            return keyword == "FOREIGN" or keyword == "CONSTRAINT"
        return False

    def _parse_foreign_key_constraint(self) -> ForeignKeyNode:
        """解析表级外键约束"""
        start_token = self._peek()
        constraint_name = None

        # 可选的 CONSTRAINT name
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "CONSTRAINT":
            self._advance()  # CONSTRAINT
            name_token = self._consume(TokenType.IDENTIFIER, None, "Expected constraint name")
            constraint_name = name_token.lexeme

        # FOREIGN KEY
        self._consume(TokenType.KEYWORD, "FOREIGN", "Expected 'FOREIGN'")
        self._consume(TokenType.KEYWORD, "KEY", "Expected 'KEY'")

        # (column_name)
        self._consume(TokenType.DELIMITER, "(", "Expected '(' after 'FOREIGN KEY'")
        column_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
        column_name = column_token.lexeme
        self._consume(TokenType.DELIMITER, ")", "Expected ')' after column name")

        # REFERENCES
        self._consume(TokenType.KEYWORD, "REFERENCES", "Expected 'REFERENCES'")

        # ref_table(ref_column)
        ref_table_token = self._consume(TokenType.IDENTIFIER, None, "Expected reference table name")
        ref_table = ref_table_token.lexeme

        self._consume(TokenType.DELIMITER, "(", "Expected '(' after reference table")
        ref_column_token = self._consume(TokenType.IDENTIFIER, None, "Expected reference column name")
        ref_column = ref_column_token.lexeme
        self._consume(TokenType.DELIMITER, ")", "Expected ')' after reference column")

        return ForeignKeyNode(
            column_name=column_name,
            ref_table=ref_table,
            ref_column=ref_column,
            constraint_name=constraint_name,
            line=start_token.line,
            col=start_token.col
        )
    # 修改为：
    def _parse_column_def(self) -> ColumnDefNode:
        # 列名
        name_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
        name = name_token.lexeme

        # 数据类型
        data_type = self._parse_type_specifier()

        # ★ 新增：解析列级约束
        constraints = {}
        while True:
            if self._check(TokenType.KEYWORD):
                keyword = self._peek().lexeme.upper()

                if keyword == "PRIMARY":
                    self._advance()  # PRIMARY
                    self._consume(TokenType.KEYWORD, "KEY", "Expected 'KEY' after 'PRIMARY'")
                    constraints["primary_key"] = True
                    constraints["not_null"] = True  # PRIMARY KEY隐含NOT NULL

                elif keyword == "NOT":
                    self._advance()  # NOT
                    self._consume(TokenType.KEYWORD, "NULL", "Expected 'NULL' after 'NOT'")
                    constraints["not_null"] = True

                elif keyword == "UNIQUE":
                    self._advance()  # UNIQUE
                    constraints["unique"] = True


                elif keyword == "DEFAULT":
                    self._advance()  # DEFAULT

                    try:
                        default_value = self._parse_value()
                        constraints["default"] = default_value.value

                    except ParseError as e:
                        # ★ 改进错误信息
                        raise ParseError(self._peek().line, self._peek().col,
                                         f"Invalid DEFAULT value: {e.hint}", "valid default value")

                else:
                    break  # 不是约束关键字，结束解析
            else:
                break

        return ColumnDefNode(name, data_type, constraints, name_token.line, name_token.col)

    def _parse_update(self) -> UpdateNode:
        """解析UPDATE语句"""
        update_token = self._previous()  # UPDATE已经匹配

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # SET关键字
        self._consume(TokenType.KEYWORD, "SET", "Expected 'SET'")

        # SET子句列表: col1=val1, col2=val2, ...
        set_clauses = []

        # 至少一个SET子句
        col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
        self._consume(TokenType.OPERATOR, "=", "Expected '=' after column name")
        value = self._parse_value()
        set_clauses.append({"column": col_token.lexeme, "value": value})

        # 处理更多SET子句
        while True:
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号
                col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
                self._consume(TokenType.OPERATOR, "=", "Expected '=' after column name")
                value = self._parse_value()
                set_clauses.append({"column": col_token.lexeme, "value": value})
            else:
                break

        # 可选的WHERE子句
        where_clause = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "WHERE":
            self._advance()  # 消费WHERE
            condition = self._parse_expression()
            where_clause = WhereClauseNode(condition)

        # 分号
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return UpdateNode(table_name, set_clauses, where_clause, update_token.line, update_token.col)
    def _parse_type_specifier(self) -> str:
        """解析类型（含 VARCHAR(n)）并返回规范化字符串，例如 'INT' 或 'VARCHAR(50)'"""
        type_tok = self._consume(TokenType.KEYWORD, None, "Expected data type")
        data_type = type_tok.lexeme.upper()
        if data_type == "VARCHAR":
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == "(":
                self._advance()
                size_tok = self._consume(TokenType.NUMBER, None, "Expected size after VARCHAR(")
                self._consume(TokenType.DELIMITER, ")", "Expected ')' after VARCHAR size")
                data_type = f"VARCHAR({size_tok.lexeme})"
        return data_type

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
        """解析SELECT语句（★ 替换完整方法）"""
        select_token = self._previous()  # SELECT已经匹配

        # ★ 新增：检查DISTINCT
        distinct = False
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "DISTINCT":
            self._advance()  # 消费DISTINCT
            distinct = True

        # ★ 修改：列列表解析，支持别名
        columns = []

        if self._check(TokenType.OPERATOR) and self._peek().lexeme == "*":
            self._advance()  # 消费*
            columns.append("*")
        else:
            # ★ 修改：解析第一列（支持别名）
            columns.append(self._parse_select_column())

            # 处理更多列名
            while True:
                if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                    self._advance()  # 消费逗号
                    columns.append(self._parse_select_column())
                else:
                    break

        # FROM关键字
        self._consume(TokenType.KEYWORD, "FROM", "Expected 'FROM'")

        # 表名
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # ★ 修改：可选的WHERE子句（支持复杂表达式）
        where_clause = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "WHERE":
            self._advance()  # 消费WHERE
            condition = self._parse_or_expression()  # ★ 改为复杂表达式解析
            where_clause = WhereClauseNode(condition)

        # 语句结束：顶层要求 ';'，子查询允许紧跟 ')'
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == ";":
            self._advance()  # 顶层 SELECT 以 ';' 结束
        elif self._check(TokenType.DELIMITER) and self._peek().lexeme == ")":
            # 子查询：不吃掉 ')', 由调用者(_parse_in_expression)去匹配
            pass
        else:
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return SelectNode(columns, table_name, distinct, where_clause, select_token.line, select_token.col)

    def _parse_select_column(self) -> Union[ColumnNode, AliasColumnNode]:
        """★ 新增：解析SELECT列（支持别名）"""
        # 列名
        col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
        column_name = col_token.lexeme

        # 检查是否有AS别名
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AS":
            self._advance()  # 消费AS
            alias_token = self._consume(TokenType.IDENTIFIER, None, "Expected alias name")
            return AliasColumnNode(column_name, alias_token.lexeme, col_token.line, col_token.col)

        # 检查隐式别名（无AS关键字）
        elif self._check(TokenType.IDENTIFIER):
            alias_token = self._advance()
            return AliasColumnNode(column_name, alias_token.lexeme, col_token.line, col_token.col)

        else:
            # 无别名
            return ColumnNode(column_name, col_token.line, col_token.col)

    # ★ 新增：复杂表达式解析（支持逻辑运算）
    def _parse_or_expression(self) -> ASTNode:
        """解析OR表达式（最低优先级）"""
        left = self._parse_and_expression()

        while self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "OR":
            op_token = self._advance()
            right = self._parse_and_expression()
            left = LogicalOpNode(left, "OR", right, op_token.line, op_token.col)

        return left

    def _parse_and_expression(self) -> ASTNode:
        """解析AND表达式"""
        left = self._parse_not_expression()

        while self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AND":
            op_token = self._advance()
            right = self._parse_not_expression()
            left = LogicalOpNode(left, "AND", right, op_token.line, op_token.col)

        return left

    def _parse_not_expression(self) -> ASTNode:
        """解析NOT表达式"""
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "NOT":
            not_token = self._advance()
            expr = self._parse_comparison_expression()
            return NotNode(expr, not_token.line, not_token.col)

        return self._parse_comparison_expression()

    def _parse_comparison_expression(self) -> ASTNode:
        """★ 替换：解析比较表达式（支持所有比较操作）"""
        left = self._parse_primary()

        # ★ 扩展：支持多种比较操作
        if self._check(TokenType.OPERATOR):
            op_token = self._advance()
            operator = op_token.lexeme
            right = self._parse_primary()
            return BinaryOpNode(left, operator, right, op_token.line, op_token.col)

        elif self._check(TokenType.KEYWORD):
            keyword = self._peek().lexeme.upper()

            if keyword == "LIKE":
                return self._parse_like_expression(left)
            elif keyword == "IN":
                return self._parse_in_expression(left)
            elif keyword == "BETWEEN":
                return self._parse_between_expression(left)
            elif keyword == "IS":
                return self._parse_is_null_expression(left)

        return left

    def _parse_like_expression(self, left: ASTNode) -> LikeNode:
        """★ 新增：解析LIKE表达式"""
        like_token = self._advance()  # 消费LIKE
        pattern = self._parse_primary()
        return LikeNode(left, pattern, False, like_token.line, like_token.col)

    def _parse_in_expression(self, left: ASTNode) -> InNode:
        """★ 新增：解析IN表达式（支持子查询）"""
        in_token = self._advance()  # 消费IN

        self._consume(TokenType.DELIMITER, "(", "Expected '(' after IN")

        # 检查是否是子查询
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "SELECT":
            self._advance()  # ★ 关键：先消费 SELECT，让 _parse_select() 的“SELECT已匹配”假设成立
            subquery = self._parse_select()
            self._consume(TokenType.DELIMITER, ")", "Expected ')' after subquery")
            return InNode(left, [], False, subquery, in_token.line, in_token.col)
        else:
            # 常量列表
            values = []
            values.append(self._parse_value())

            while True:
                if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                    self._advance()  # 消费逗号
                    values.append(self._parse_value())
                else:
                    break

            self._consume(TokenType.DELIMITER, ")", "Expected ')' after value list")
            return InNode(left, values, False, None, in_token.line, in_token.col)

    def _parse_between_expression(self, left: ASTNode) -> BetweenNode:
        """★ 新增：解析BETWEEN表达式"""
        between_token = self._advance()  # 消费BETWEEN

        min_val = self._parse_primary()
        self._consume(TokenType.KEYWORD, "AND", "Expected 'AND' in BETWEEN expression")
        max_val = self._parse_primary()

        return BetweenNode(left, min_val, max_val, False, between_token.line, between_token.col)

    def _parse_is_null_expression(self, left: ASTNode) -> IsNullNode:
        """★ 新增：解析IS NULL表达式"""
        is_token = self._advance()  # 消费IS

        # 检查NOT
        is_not = False
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "NOT":
            self._advance()  # 消费NOT
            is_not = True

        self._consume(TokenType.KEYWORD, "NULL", "Expected 'NULL' after IS [NOT]")

        return IsNullNode(left, is_not, is_token.line, is_token.col)


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

    def _parse_show_tables(self) -> ShowTablesNode:
        # SHOW 已匹配
        # 期望 TABLES
        self._consume(TokenType.KEYWORD, "TABLES", "Expected 'TABLES'")
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
        return ShowTablesNode()

    def _parse_desc(self) -> DescTableNode:
        # DESC 已匹配
        t = self._consume(TokenType.IDENTIFIER, None, "Expected table name after DESC")
        self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
        return DescTableNode(t.lexeme, t.line, t.col)

    def _parse_alter_table(self) -> AlterTableNode:
        # ALTER 已匹配，期望 TABLE
        self._consume(TokenType.KEYWORD, "TABLE", "Expected 'TABLE'")

        # 表名
        t = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = t.lexeme

        # 分派子句
        # 1) RENAME TO new_name
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "RENAME":
            self._advance()  # RENAME
            self._consume(TokenType.KEYWORD, "TO", "Expected 'TO'")
            new_tok = self._consume(TokenType.IDENTIFIER, None, "Expected new table name")
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
            return AlterTableNode(table_name, "RENAME", {"new_name": new_tok.lexeme}, t.line, t.col)

        # 2) ADD COLUMN name TYPE
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "ADD":
            self._advance()  # ADD
            self._consume(TokenType.KEYWORD, "COLUMN", "Expected 'COLUMN'")
            col_tok = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
            data_type = self._parse_type_specifier()
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
            return AlterTableNode(table_name, "ADD_COLUMN", {"name": col_tok.lexeme, "type": data_type}, t.line, t.col)

        # 3) MODIFY COLUMN name TYPE
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "MODIFY":
            self._advance()  # MODIFY
            self._consume(TokenType.KEYWORD, "COLUMN", "Expected 'COLUMN'")
            col_tok = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
            data_type = self._parse_type_specifier()
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
            return AlterTableNode(table_name, "MODIFY_COLUMN", {"name": col_tok.lexeme, "type": data_type}, t.line,
                                  t.col)

        # 4) CHANGE old_name new_name TYPE
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "CHANGE":
            self._advance()  # CHANGE
            old_tok = self._consume(TokenType.IDENTIFIER, None, "Expected old column name")
            new_tok = self._consume(TokenType.IDENTIFIER, None, "Expected new column name")
            data_type = self._parse_type_specifier()
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
            return AlterTableNode(
                table_name, "CHANGE_COLUMN",
                {"old_name": old_tok.lexeme, "new_name": new_tok.lexeme, "type": data_type},
                t.line, t.col
            )

        # 5) DROP COLUMN name
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "DROP":
            self._advance()  # DROP
            self._consume(TokenType.KEYWORD, "COLUMN", "Expected 'COLUMN'")
            col_tok = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")
            return AlterTableNode(table_name, "DROP_COLUMN", {"name": col_tok.lexeme}, t.line, t.col)

        raise ParseError(self._peek().line, self._peek().col,
                         "Unsupported ALTER TABLE sub-clause",
                         "RENAME TO / ADD COLUMN / MODIFY COLUMN / CHANGE / DROP COLUMN")

    def _parse_expression(self) -> ASTNode:
        """解析表达式（向后兼容，重定向到新的解析器）"""
        return self._parse_or_expression()

    def _parse_primary(self) -> ASTNode:
        """解析基本表达式（补充：支持( ... )）"""
        # 括号表达式
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == "(":
            lpar = self._advance()  # '('
            expr = self._parse_or_expression()  # 按当前优先级体系解析内部
            self._consume(TokenType.DELIMITER, ")", "Expected ')' after parenthesized expression")
            return expr

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


def test_s5_parser_extensions():
    """测试S5语法扩展"""
    print("=== S5 Parser扩展测试 ===")

    parser = Parser()

    test_cases = [
        # DISTINCT
        ("SELECT DISTINCT name FROM users;", "DISTINCT查询"),
        ("SELECT DISTINCT id, name FROM users;", "DISTINCT多列"),

        # 别名
        ("SELECT id AS user_id FROM users;", "AS别名"),
        ("SELECT name username FROM users;", "隐式别名"),
        ("SELECT id AS user_id, name AS username FROM users;", "多列别名"),

        # 复杂WHERE
        ("SELECT * FROM users WHERE age > 18 AND name LIKE 'A%';", "AND + LIKE"),
        ("SELECT * FROM users WHERE age IN (18, 19, 20);", "IN常量列表"),
        ("SELECT * FROM users WHERE age BETWEEN 18 AND 25;", "BETWEEN"),
        ("SELECT * FROM users WHERE email IS NULL;", "IS NULL"),
        ("SELECT * FROM users WHERE status IS NOT NULL;", "IS NOT NULL"),
        ("SELECT * FROM users WHERE age > 18 OR name = 'Admin';", "OR逻辑"),
        ("SELECT * FROM users WHERE NOT (age < 18);", "NOT逻辑"),

        # 组合查询
        ("SELECT DISTINCT name AS username FROM users WHERE age > 18;", "DISTINCT+别名+WHERE"),

        # 子查询（基础）
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);", "IN子查询"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功")

            # 显示关键特性
            if isinstance(ast, SelectNode):
                if ast.distinct:
                    print("   特性: DISTINCT")
                for col in ast.columns:
                    if isinstance(col, AliasColumnNode):
                        print(f"   特性: 别名 {col.column_name} AS {col.alias}")
                if ast.where_clause:
                    condition_type = type(ast.where_clause.condition).__name__
                    print(f"   特性: WHERE ({condition_type})")

        except ParseError as e:
            print(f"❌ 语法错误: {e.hint}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")
if __name__ == "__main__":
    test_s5_parser_extensions()