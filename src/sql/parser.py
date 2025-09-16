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

# class SelectNode(ASTNode):
#     """SELECT语句节点"""
#     def __init__(self, columns: List[Union[ColumnNode, str]], table_name: str, where_clause: Optional[WhereClauseNode] = None, line: int = 0, col: int = 0):
#         super().__init__(line, col)
#         self.columns = columns
#         self.table_name = table_name
#         self.where_clause = where_clause

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


# ★ 新增：聚合和排序相关AST节点（添加到现有AST节点定义后）

class AggregateFuncNode(ASTNode):
    """聚合函数节点：COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)"""

    def __init__(self, func_name: str, column: str, alias: str = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.func_name = func_name.upper()  # COUNT, SUM, AVG, MIN, MAX
        self.column = column  # "*" for COUNT(*), 实际列名 for others
        self.alias = alias  # AS别名


class GroupByNode(ASTNode):
    """GROUP BY子句节点"""

    def __init__(self, columns: List[str], line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.columns = columns  # 分组列名列表


class HavingNode(ASTNode):
    """HAVING子句节点"""

    def __init__(self, condition: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.condition = condition  # HAVING条件表达式


class OrderByNode(ASTNode):
    """ORDER BY子句节点"""

    def __init__(self, sort_keys: List[Dict[str, str]], line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.sort_keys = sort_keys  # [{"column": "name", "order": "ASC"}, ...]


class LimitNode(ASTNode):
    """LIMIT子句节点"""

    def __init__(self, count: int, offset: int = 0, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.count = count  # 返回行数
        self.offset = offset  # 跳过行数


# ★ 新增：JOIN相关AST节点（添加到现有AST节点定义后）

class TableRefNode(ASTNode):
    """表引用节点（支持别名）"""
    def __init__(self, table_name: str, alias: str = None, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.table_name = table_name
        self.alias = alias

class JoinNode(ASTNode):
    """JOIN子句节点"""
    def __init__(self, join_type: str, right_table: TableRefNode, on_condition: ASTNode, line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.join_type = join_type.upper()  # "INNER", "LEFT", "RIGHT"
        self.right_table = right_table
        self.on_condition = on_condition


# ★ 修改：SelectNode支持多表查询
class SelectNode(ASTNode):
    """SELECT语句节点（支持JOIN和完整管线）"""

    def __init__(self, columns: List[Union[ColumnNode, AliasColumnNode, AggregateFuncNode, str]],
                 from_clause: Union[TableRefNode, List],  # ★ 修改：支持单表或表+JOIN列表
                 distinct: bool = False,
                 where_clause: Optional[WhereClauseNode] = None,
                 group_by: Optional[GroupByNode] = None,
                 having: Optional[HavingNode] = None,
                 order_by: Optional[OrderByNode] = None,
                 limit: Optional[LimitNode] = None,
                 line: int = 0, col: int = 0):
        super().__init__(line, col)
        self.columns = columns
        self.from_clause = from_clause  # ★ 新设计：统一的FROM子句
        self.distinct = distinct
        self.where_clause = where_clause
        self.group_by = group_by
        self.having = having
        self.order_by = order_by
        self.limit = limit

        # ★ 向后兼容：提供table_name属性
        if isinstance(from_clause, TableRefNode):
            self.table_name = from_clause.table_name
        elif isinstance(from_clause, list) and len(from_clause) > 0:
            self.table_name = from_clause[0].table_name if isinstance(from_clause[0], TableRefNode) else "multi_table"
        else:
            self.table_name = "unknown"



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
        """★ 完整替换：解析支持JOIN的SELECT语句"""
        select_token = self._previous()  # SELECT已经匹配

        # DISTINCT
        distinct = False
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "DISTINCT":
            self._advance()
            distinct = True

        # 列列表解析（复用现有逻辑）
        columns = []
        if self._check(TokenType.OPERATOR) and self._peek().lexeme == "*":
            self._advance()
            columns.append("*")
        else:
            columns.append(self._parse_select_column_or_aggregate())

            while True:
                if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                    self._advance()  # 消费逗号
                    columns.append(self._parse_select_column_or_aggregate())
                else:
                    break

        # FROM关键字
        self._consume(TokenType.KEYWORD, "FROM", "Expected 'FROM'")

        # ★ 新增：解析FROM子句（支持JOIN）
        from_clause = self._parse_from_clause()

        # 其余子句保持不变
        where_clause = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "WHERE":
            self._advance()
            condition = self._parse_or_expression()
            where_clause = WhereClauseNode(condition)

        group_by = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "GROUP":
            group_by = self._parse_group_by()

        having = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "HAVING":
            if group_by is None:
                raise ParseError(self._peek().line, self._peek().col,
                                 "HAVING clause requires GROUP BY clause")
            having = self._parse_having()

        order_by = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "ORDER":
            order_by = self._parse_order_by()

        limit = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "LIMIT":
            limit = self._parse_limit()

        # 语句结束
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == ";":
            self._advance()
        elif self._check(TokenType.DELIMITER) and self._peek().lexeme == ")":
            pass  # 子查询：不消费')'
        else:
            self._consume(TokenType.DELIMITER, ";", "Expected ';' at end of statement")

        return SelectNode(columns, from_clause, distinct, where_clause,
                          group_by, having, order_by, limit,
                          select_token.line, select_token.col)

    def _parse_from_clause(self) -> Union[TableRefNode, List]:
        """★ 修复：解析FROM子句（支持隐式连接检测）"""
        # 解析第一个表
        main_table = self._parse_table_reference()

        # ★ 新增：检查隐式连接（逗号分隔的表）
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
            # 隐式连接：FROM A, B, C...
            tables = [main_table]
            while self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号
                table = self._parse_table_reference()
                tables.append(table)

            # ★ 简化处理：暂时不支持隐式连接，给出明确错误
            raise ParseError(main_table.line, main_table.col,
                             "Implicit joins (FROM A, B) are not yet supported. Use explicit JOIN syntax.")

        # 检查是否有显式JOIN
        joins = []
        while self._check_join_keyword():
            join_clause = self._parse_join_clause()
            joins.append(join_clause)


        # 如果没有JOIN，返回单表；否则返回[主表, JOIN1, JOIN2, ...]
        if not joins:
            result = main_table
        else:
            result = [main_table] + joins

        # ★ 新增：校验 ON 子句中的前缀是否都是已声明的表名/别名
        known = self._gather_known_table_names(result)
        if isinstance(result, list):
            for j in result[1:]:
                # 只检查 JOIN 节点
                if isinstance(j, JoinNode):
                    for prefix, l, c in self._iter_qualified_prefixes(j.on_condition):
                        if prefix not in known:
                            raise ParseError(l, c, f"Unknown table or alias '{prefix}' in JOIN condition")

        return result

    def _parse_table_reference(self) -> TableRefNode:
        """★ 新增：解析表引用（支持别名）"""
        table_token = self._consume(TokenType.IDENTIFIER, None, "Expected table name")
        table_name = table_token.lexeme

        # 检查别名
        alias = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AS":
            self._advance()
            alias_token = self._consume(TokenType.IDENTIFIER, None, "Expected table alias")
            alias = alias_token.lexeme
        elif self._check(TokenType.IDENTIFIER):
            # 隐式别名（无 AS），但不得是保留词（JOIN 相关、子句关键字）
            reserved = {"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
                        "ON", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT"}
            cand = self._peek().lexeme.upper()
            if cand in reserved:
                # 不把这些词当作别名使用，留给后续解析（例如 FULL JOIN）
                pass
            else:
                alias_token = self._advance()
                alias = alias_token.lexeme

        return TableRefNode(table_name, alias, table_token.line, table_token.col)

    def _check_join_keyword(self) -> bool:
        """★ 修复：把 FULL/LEFT/RIGHT/INNER/JOIN 视为 JOIN 关键字，即便被当成 IDENTIFIER"""
        if self._is_at_end():
            return False
        tok = self._peek()
        if tok.type not in (TokenType.KEYWORD, TokenType.IDENTIFIER):
            return False
        return tok.lexeme.upper() in {"JOIN", "INNER", "LEFT", "RIGHT", "FULL"}

    def _parse_join_clause(self) -> JoinNode:
        """★ 修复：解析JOIN子句（修复OUTER关键字处理）"""
        join_start_token = self._peek()

        # 解析JOIN类型
        join_type = "INNER"  # 默认内连接

        if (self._check(TokenType.KEYWORD)
                or (self._check(TokenType.IDENTIFIER) and self._peek().lexeme.upper() in {"JOIN", "INNER", "LEFT",
                                                                                          "RIGHT", "FULL"})):
            first_keyword = self._peek().lexeme.upper()

            if first_keyword == "INNER":
                self._advance()  # 消费INNER
                self._consume(TokenType.KEYWORD, "JOIN", "Expected 'JOIN' after 'INNER'")
                join_type = "INNER"

            elif first_keyword == "LEFT":
                self._advance()  # 消费LEFT
                # ★ 修复：可选的OUTER
                if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "OUTER":
                    self._advance()  # 消费OUTER
                self._consume(TokenType.KEYWORD, "JOIN", "Expected 'JOIN' after 'LEFT [OUTER]'")
                join_type = "LEFT"

            elif first_keyword == "RIGHT":
                self._advance()  # 消费RIGHT
                # ★ 修复：可选的OUTER
                if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "OUTER":
                    self._advance()  # 消费OUTER
                self._consume(TokenType.KEYWORD, "JOIN", "Expected 'JOIN' after 'RIGHT [OUTER]'")
                join_type = "RIGHT"

            elif first_keyword == "JOIN":
                self._advance()  # 消费JOIN
                join_type = "INNER"  # 默认内连接

            elif first_keyword == "FULL":
                # ★ 新增：FULL JOIN报错
                raise ParseError(join_start_token.line, join_start_token.col,
                                 "FULL OUTER JOIN is not supported")

            else:
                raise ParseError(join_start_token.line, join_start_token.col,
                                 f"Unexpected keyword in JOIN: {first_keyword}")

        # 解析右表
        right_table = self._parse_table_reference()

        # ON关键字
        self._consume(TokenType.KEYWORD, "ON", "Expected 'ON' after JOIN table")

        # ON条件（复用表达式解析）
        on_condition = self._parse_or_expression()

        return JoinNode(join_type, right_table, on_condition,
                        join_start_token.line, join_start_token.col)

    # ★ 新增：支持表.列格式的列引用解析
    def _parse_qualified_column(self) -> str:
        """★ 新增：解析带表前缀的列名（table.column 或 alias.column）"""
        first_token = self._consume(TokenType.IDENTIFIER, None, "Expected table or column name")

        # 检查是否有点号
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == ".":
            self._advance()  # 消费点号
            second_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name after '.'")
            return f"{first_token.lexeme}.{second_token.lexeme}"
        else:
            # 回退：这只是普通列名
            self.current -= 1  # 回退一个token
            return first_token.lexeme



    def _parse_select_column_or_aggregate(self) -> Union[ColumnNode, AliasColumnNode, AggregateFuncNode]:
        """★ 新增：解析SELECT列或聚合函数"""

        # 检查是否是聚合函数
        if self._check(TokenType.IDENTIFIER):
            potential_func = self._peek().lexeme.upper()

            # 聚合函数列表
            if potential_func in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                return self._parse_aggregate_function()

        # 普通列名（复用S5的逻辑）
        return self._parse_select_column()

    def _parse_aggregate_function(self) -> AggregateFuncNode:
        """★ 新增：解析聚合函数（支持表.列）"""
        func_token = self._consume(TokenType.IDENTIFIER, None, "Expected aggregate function name")
        func_name = func_token.lexeme.upper()
        if func_name not in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
            raise ParseError(func_token.line, func_token.col, f"Unknown aggregate function: {func_name}")

        self._consume(TokenType.DELIMITER, "(", f"Expected '(' after {func_name}")

        # COUNT(*) 特例；其他允许 table.column 或 column
        if func_name == "COUNT" and self._check(TokenType.OPERATOR) and self._peek().lexeme == "*":
            self._advance()
            column = "*"
        else:
            # 允许限定列
            column = self._parse_qualified_column()

        self._consume(TokenType.DELIMITER, ")", f"Expected ')' after {func_name} argument")

        # 可选别名
        alias = None
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AS":
            self._advance()
            alias_token = self._consume(TokenType.IDENTIFIER, None, "Expected alias name")
            alias = alias_token.lexeme
        elif self._check(TokenType.IDENTIFIER):
            alias_token = self._advance()
            alias = alias_token.lexeme

        return AggregateFuncNode(func_name, column, alias, func_token.line, func_token.col)

    def _parse_group_by(self) -> GroupByNode:
        group_token = self._advance()  # 消费 GROUP
        self._consume(TokenType.KEYWORD, "BY", "Expected 'BY' after 'GROUP'")

        columns = []
        columns.append(self._parse_qualified_column())  # 允许限定列

        while self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
            self._advance()  # 消费逗号
            columns.append(self._parse_qualified_column())  # 允许限定列

        return GroupByNode(columns, group_token.line, group_token.col)

    def _parse_having(self) -> HavingNode:
        """★ 新增：解析HAVING子句"""
        having_token = self._advance()  # 消费HAVING

        # HAVING条件（复用WHERE的表达式解析）
        condition = self._parse_or_expression()

        return HavingNode(condition, having_token.line, having_token.col)

    def _parse_order_by(self) -> OrderByNode:
        """★ 新增：解析ORDER BY子句"""
        order_token = self._advance()  # 消费ORDER
        self._consume(TokenType.KEYWORD, "BY", "Expected 'BY' after 'ORDER'")

        # 排序键列表
        sort_keys = []

        # 第一个排序键
        sort_keys.append(self._parse_sort_key())

        # 更多排序键
        while True:
            if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
                self._advance()  # 消费逗号
                sort_keys.append(self._parse_sort_key())
            else:
                break

        return OrderByNode(sort_keys, order_token.line, order_token.col)

    def _parse_sort_key(self) -> Dict[str, str]:
        if self._check(TokenType.NUMBER):
            num_token = self._advance()
            column = f"__pos_{num_token.lexeme}"
        else:
            # 允许限定列或普通标识符
            if self._check(TokenType.IDENTIFIER):
                nxt = self.tokens[self.current + 1] if (self.current + 1) < len(self.tokens) else None
                if nxt and nxt.type == TokenType.DELIMITER and nxt.lexeme == ".":
                    column = self._parse_qualified_column()
                else:
                    col_token = self._advance()
                    column = col_token.lexeme
            else:
                col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name or position")
                column = col_token.lexeme

        order = "ASC"
        if self._check(TokenType.KEYWORD):
            next_kw = self._peek().lexeme.upper()
            if next_kw in ["ASC", "DESC"]:
                self._advance()
                order = next_kw

        return {"column": column, "order": order}

    def _parse_limit(self) -> LimitNode:
        """★ 新增：解析LIMIT子句"""
        limit_token = self._advance()  # 消费LIMIT

        # ★ 支持两种格式：
        # 格式1: LIMIT count
        # 格式2: LIMIT offset, count
        # 格式3: LIMIT count OFFSET offset

        first_number = self._consume(TokenType.NUMBER, None, "Expected number after LIMIT")
        first_value = int(first_number.lexeme)

        offset = 0
        count = first_value

        # 检查后续格式
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == ",":
            # 格式2: LIMIT offset, count
            self._advance()  # 消费逗号
            second_number = self._consume(TokenType.NUMBER, None, "Expected count after comma")
            offset = first_value
            count = int(second_number.lexeme)

        elif self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "OFFSET":
            # 格式3: LIMIT count OFFSET offset
            self._advance()  # 消费OFFSET
            offset_number = self._consume(TokenType.NUMBER, None, "Expected number after OFFSET")
            offset = int(offset_number.lexeme)
            # count保持first_value

        # 参数验证
        if count <= 0:
            raise ParseError(limit_token.line, limit_token.col,
                             f"LIMIT count must be positive: {count}")
        if offset < 0:
            raise ParseError(limit_token.line, limit_token.col,
                             f"LIMIT offset must be non-negative: {offset}")

        return LimitNode(count, offset, limit_token.line, limit_token.col)

    # ★ 新增：表达式环境的聚合函数调用解析（无别名）
    def _parse_agg_call_in_expr(self) -> AggregateFuncNode:
        """解析表达式中的聚合函数调用（支持表.列）"""
        func_tok = self._consume(TokenType.IDENTIFIER, None, "Expected function name")
        func = func_tok.lexeme.upper()
        if func not in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
            raise ParseError(func_tok.line, func_tok.col, f"Unsupported function in expression: {func}")

        self._consume(TokenType.DELIMITER, "(", f"Expected '(' after {func}")

        if func == "COUNT" and self._check(TokenType.OPERATOR) and self._peek().lexeme == "*":
            self._advance()
            col = "*"
        else:
            col = self._parse_qualified_column()  # ★ 允许 table.column

        self._consume(TokenType.DELIMITER, ")", f"Expected ')' to close {func}(")
        return AggregateFuncNode(func, col, alias=None, line=func_tok.line, col=func_tok.col)

    def _parse_select_column(self) -> Union[ColumnNode, AliasColumnNode]:
        """★ 修复：解析SELECT列（支持表.列引用和别名）"""
        # ★ 修复：检查是否为表.列格式
        if self._check(TokenType.IDENTIFIER):
            first_token = self._peek()

            # 检查下一个token是否为点号
            next_token = self.tokens[self.current + 1] if (self.current + 1) < len(self.tokens) else None

            if next_token and next_token.type == TokenType.DELIMITER and next_token.lexeme == ".":
                # 表.列格式：table.column
                table_token = self._advance()  # 消费表名
                self._advance()  # 消费点号
                col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name after '.'")
                column_name = f"{table_token.lexeme}.{col_token.lexeme}"

                # 检查别名
                alias = None
                if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AS":
                    self._advance()  # 消费AS
                    alias_token = self._consume(TokenType.IDENTIFIER, None, "Expected alias name")
                    alias = alias_token.lexeme
                elif self._check(TokenType.IDENTIFIER):
                    # 隐式别名
                    alias_token = self._advance()
                    alias = alias_token.lexeme

                if alias:
                    return AliasColumnNode(column_name, alias, table_token.line, table_token.col)
                else:
                    return ColumnNode(column_name, table_token.line, table_token.col)
            else:
                # 普通列名
                col_token = self._advance()
                column_name = col_token.lexeme

                # 检查别名
                alias = None
                if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "AS":
                    self._advance()  # 消费AS
                    alias_token = self._consume(TokenType.IDENTIFIER, None, "Expected alias name")
                    alias = alias_token.lexeme
                elif self._check(TokenType.IDENTIFIER):
                    # 隐式别名
                    alias_token = self._advance()
                    alias = alias_token.lexeme

                if alias:
                    return AliasColumnNode(column_name, alias, col_token.line, col_token.col)
                else:
                    return ColumnNode(column_name, col_token.line, col_token.col)
        else:
            # 兜底：期望标识符
            col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name")
            return ColumnNode(col_token.lexeme, col_token.line, col_token.col)

    def _gather_known_table_names(self, from_clause) -> set:
        names = set()
        if isinstance(from_clause, TableRefNode):
            names.add(from_clause.alias or from_clause.table_name)
        elif isinstance(from_clause, list) and from_clause:
            main = from_clause[0]
            if isinstance(main, TableRefNode):
                names.add(main.alias or main.table_name)
            for item in from_clause[1:]:
                if isinstance(item, JoinNode):
                    t = item.right_table
                    names.add(t.alias or t.table_name)
        return names

    def _iter_qualified_prefixes(self, expr) -> List[tuple]:
        out = []

        def walk(node):
            if node is None:
                return
            if isinstance(node, ColumnNode):
                if isinstance(node.name, str) and "." in node.name:
                    p = node.name.split(".", 1)[0]
                    out.append((p, node.line, node.col))
            elif isinstance(node, AliasColumnNode):
                if "." in node.column_name:
                    p = node.column_name.split(".", 1)[0]
                    out.append((p, node.line, node.col))
            elif isinstance(node, (BinaryOpNode, LogicalOpNode)):
                walk(node.left);
                walk(node.right)
            elif isinstance(node, NotNode):
                walk(node.expr)
            elif isinstance(node, InNode):
                walk(node.left)
            elif isinstance(node, BetweenNode):
                walk(node.expr);
                walk(node.min_val);
                walk(node.max_val)
            elif isinstance(node, (LikeNode, IsNullNode)):
                walk(node.left if hasattr(node, "left") else node.expr)
            elif isinstance(node, AggregateFuncNode):
                pass
            elif isinstance(node, WhereClauseNode):
                walk(node.condition)

        walk(expr)
        return out


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
        """★ 修改：解析基本表达式（支持表.列引用和聚合函数）"""
        # 括号表达式
        if self._check(TokenType.DELIMITER) and self._peek().lexeme == "(":
            lpar = self._advance()
            expr = self._parse_or_expression()
            self._consume(TokenType.DELIMITER, ")", "Expected ')' after parenthesized expression")
            return expr

        # 数字常量
        if self._check(TokenType.NUMBER):
            token = self._advance()
            try:
                value = int(token.lexeme)
            except ValueError:
                value = float(token.lexeme)
            return ValueNode(value, "NUMBER", token.line, token.col)

        # 字符串常量
        if self._check(TokenType.STRING):
            token = self._advance()
            return ValueNode(token.lexeme, "STRING", token.line, token.col)

        # NULL常量
        if self._check(TokenType.KEYWORD) and self._peek().lexeme.upper() == "NULL":
            token = self._advance()
            return ValueNode(None, "NULL", token.line, token.col)

        # ★ 修改：IDENTIFIER 处理（支持表.列和聚合函数）
        if self._check(TokenType.IDENTIFIER):
            cur = self._peek()  # 当前标识符

            # 先检查下一个token是否为左括号（函数调用）
            nxt = self.tokens[self.current + 1] if (self.current + 1) < len(self.tokens) else None

            if nxt is not None and nxt.type == TokenType.DELIMITER and nxt.lexeme == "(":
                # 函数调用：检查是否为聚合函数
                func_name = cur.lexeme.upper()
                if func_name in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                    return self._parse_agg_call_in_expr()
                else:
                    raise ParseError(cur.line, cur.col, f"Unsupported function: {func_name}")

            # 检查是否为表.列格式
            elif nxt is not None and nxt.type == TokenType.DELIMITER and nxt.lexeme == ".":
                # 表.列引用
                table_token = self._advance()  # 消费表名
                self._advance()  # 消费点号
                col_token = self._consume(TokenType.IDENTIFIER, None, "Expected column name after '.'")
                qualified_name = f"{table_token.lexeme}.{col_token.lexeme}"
                return ColumnNode(qualified_name, table_token.line, table_token.col)

            else:
                # 普通列名
                token = self._advance()
                return ColumnNode(token.lexeme, token.line, token.col)

        current = self._peek()
        raise ParseError(current.line, current.col, "Expected expression",
                         "number, string, identifier, table.column or '(' ... ')'")

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


def test_s6s7_parser_extensions():
    """测试S6+S7语法扩展"""
    print("=== S6+S7 Parser扩展测试 ===")

    parser = Parser()

    test_cases = [
        # S6聚合函数测试
        ("SELECT COUNT(*) FROM users;", "COUNT(*)聚合"),
        ("SELECT COUNT(id) FROM users;", "COUNT(column)聚合"),
        ("SELECT AVG(salary), SUM(salary) FROM employees;", "多个聚合函数"),
        ("SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept;", "分组聚合"),
        ("SELECT dept, AVG(sal) FROM emp GROUP BY dept HAVING AVG(sal) > 5000;", "HAVING过滤"),

        # S7排序和分页测试
        ("SELECT * FROM users ORDER BY name;", "单列排序"),
        ("SELECT * FROM users ORDER BY salary DESC, name ASC;", "多列排序"),
        ("SELECT * FROM users ORDER BY 1, 2 DESC;", "序号排序"),
        ("SELECT * FROM users LIMIT 10;", "简单分页"),
        ("SELECT * FROM users LIMIT 5, 10;", "偏移分页"),
        ("SELECT * FROM users LIMIT 10 OFFSET 5;", "OFFSET语法"),

        # 完整管线测试
        ("SELECT dept, AVG(salary) as avg_sal FROM employees WHERE age > 25 GROUP BY dept HAVING AVG(salary) > 60000 ORDER BY avg_sal DESC LIMIT 3;",
         "完整管线"),

        # 别名测试
        ("SELECT COUNT(*) as total, AVG(age) as avg_age FROM users;", "聚合函数别名"),
        ("SELECT name username, salary FROM emp ORDER BY username;", "ORDER BY别名"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功")

            # 显示关键特性
            if isinstance(ast, SelectNode):
                # 聚合函数检查
                agg_funcs = [col for col in ast.columns if isinstance(col, AggregateFuncNode)]
                if agg_funcs:
                    for agg in agg_funcs:
                        print(f"   聚合函数: {agg.func_name}({agg.column})" +
                              (f" AS {agg.alias}" if agg.alias else ""))

                # GROUP BY检查
                if ast.group_by:
                    print(f"   分组列: {', '.join(ast.group_by.columns)}")

                # HAVING检查
                if ast.having:
                    print(f"   HAVING条件: {type(ast.having.condition).__name__}")

                # ORDER BY检查
                if ast.order_by:
                    keys_desc = []
                    for key in ast.order_by.sort_keys:
                        keys_desc.append(f"{key['column']} {key['order']}")
                    print(f"   排序键: {', '.join(keys_desc)}")

                # LIMIT检查
                if ast.limit:
                    if ast.limit.offset > 0:
                        print(f"   分页: LIMIT {ast.limit.offset}, {ast.limit.count}")
                    else:
                        print(f"   分页: LIMIT {ast.limit.count}")

        except ParseError as e:
            print(f"❌ 语法错误: {e.hint}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")


def test_parser_error_cases():
    """测试语法错误检测"""
    print("\n=== 语法错误检测测试 ===")

    parser = Parser()

    error_cases = [
        ("SELECT COUNT() FROM users;", "聚合函数缺少参数"),
        ("SELECT * FROM users HAVING id > 1;", "HAVING without GROUP BY"),
        ("SELECT UNKNOWN(id) FROM users;", "未知聚合函数"),
        ("SELECT * FROM users LIMIT -1;", "负数LIMIT"),
        ("SELECT * FROM users LIMIT 0;", "零LIMIT"),
        ("SELECT * FROM users ORDER BY;", "ORDER BY缺少列名"),
        ("SELECT COUNT(*) GROUP BY dept;", "缺少FROM子句"),
    ]

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n[错误测试 {i}] {expected_error}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print(f"❌ 应该报错但解析成功了")
        except ParseError as e:
            print(f"✓ 正确捕获错误: {e.hint}")
        except Exception as e:
            print(f"✓ 捕获其他错误: {e}")


def test_having_bug_fix():
    """测试HAVING聚合函数bug修复"""
    print("=== HAVING聚合函数Bug修复测试 ===")

    parser = Parser()

    # 修复前会失败的用例
    test_cases = [
        ("SELECT dept, AVG(salary) FROM emp GROUP BY dept HAVING AVG(salary) > 5000;", "HAVING AVG"),
        ("SELECT age, COUNT(*) FROM emp GROUP BY age HAVING COUNT(*) >= 2;", "HAVING COUNT"),
        ("SELECT dept, SUM(sal) FROM emp GROUP BY dept HAVING SUM(sal) > 100000;", "HAVING SUM"),
        ("SELECT * FROM emp WHERE dept = 'IT' AND id IN (SELECT MAX(id) FROM emp);", "子查询中的聚合"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[修复测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功 - HAVING聚合函数已可正确解析")

            # 检查HAVING子句
            if isinstance(ast, SelectNode) and ast.having:
                condition_type = type(ast.having.condition).__name__
                print(f"   HAVING条件类型: {condition_type}")

        except ParseError as e:
            print(f"❌ 仍有语法错误: {e.hint}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")


def test_s8_join_parser():
    """测试S8 JOIN语法解析"""
    print("=== S8 JOIN Parser扩展测试 ===")

    parser = Parser()

    test_cases = [
        # 内连接测试
        ("SELECT * FROM students s JOIN scores sc ON s.id = sc.student_id;", "隐式内连接"),
        ("SELECT * FROM students s INNER JOIN scores sc ON s.id = sc.student_id;", "显式内连接"),

        # 外连接测试
        ("SELECT * FROM students s LEFT JOIN scores sc ON s.id = sc.student_id;", "左外连接"),
        ("SELECT s.name, sc.score FROM students s LEFT OUTER JOIN scores sc ON s.id = sc.student_id;",
         "左外连接(OUTER)"),
        ("SELECT * FROM students s RIGHT JOIN scores sc ON s.id = sc.student_id;", "右外连接"),

        # 表别名测试
        ("SELECT s.name, sc.score FROM students s JOIN scores sc ON s.id = sc.student_id;", "表别名"),
        ("SELECT students.name, scores.score FROM students JOIN scores ON students.id = scores.student_id;", "无别名"),

        # 多表连接测试
        ("SELECT s.name, c.title, sc.score FROM students s JOIN scores sc ON s.id = sc.student_id JOIN courses c ON sc.course_id = c.id;",
         "三表连接"),

        # 复杂查询测试
        ("SELECT s.name, AVG(sc.score) as avg_score FROM students s LEFT JOIN scores sc ON s.id = sc.student_id WHERE s.major = 'CS' GROUP BY s.id, s.name HAVING AVG(sc.score) > 80 ORDER BY avg_score DESC LIMIT 5;",
         "JOIN+聚合+完整管线"),

        # 隐式连接（FROM A,B WHERE...）
        ("SELECT s.name, sc.score FROM students s, scores sc WHERE s.id = sc.student_id;", "隐式连接"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功")

            # 显示关键特性
            if isinstance(ast, SelectNode):
                # 分析FROM子句
                if isinstance(ast.from_clause, TableRefNode):
                    table = ast.from_clause
                    print(f"   单表: {table.table_name}" + (f" AS {table.alias}" if table.alias else ""))
                elif isinstance(ast.from_clause, list):
                    print(f"   多表查询: {len(ast.from_clause)}个表/JOIN")
                    for j, item in enumerate(ast.from_clause):
                        if isinstance(item, TableRefNode):
                            print(f"     表{j}: {item.table_name}" + (f" AS {item.alias}" if item.alias else ""))
                        elif isinstance(item, JoinNode):
                            right_table = item.right_table
                            print(f"     {item.join_type} JOIN: {right_table.table_name}" +
                                  (f" AS {right_table.alias}" if right_table.alias else ""))

                # 检查列引用
                qualified_cols = []
                for col in ast.columns:
                    if isinstance(col, ColumnNode) and "." in col.name:
                        qualified_cols.append(col.name)
                    elif isinstance(col, AliasColumnNode) and "." in col.column_name:
                        qualified_cols.append(col.column_name)

                if qualified_cols:
                    print(f"   限定列引用: {qualified_cols[:3]}" + ("..." if len(qualified_cols) > 3 else ""))

        except ParseError as e:
            print(f"❌ 语法错误: {e.hint}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")


def test_join_error_cases():
    """测试JOIN语法错误检测"""
    print("\n=== JOIN语法错误检测测试 ===")

    parser = Parser()

    error_cases = [
        ("SELECT * FROM students JOIN;", "JOIN缺少表名"),
        ("SELECT * FROM students JOIN scores;", "JOIN缺少ON条件"),
        ("SELECT * FROM students FULL JOIN scores ON s.id = sc.id;", "不支持的JOIN类型"),
        ("SELECT * FROM students LEFT scores ON s.id = sc.id;", "LEFT缺少JOIN关键字"),
        ("SELECT * FROM students JOIN scores ON;", "ON条件为空"),
        ("SELECT * FROM students s JOIN scores ON s.id = sc.student_id;", "右表缺少别名但引用了别名"),
    ]

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n[错误测试 {i}] {expected_error}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print(f"❌ 应该报错但解析成功了")
        except ParseError as e:
            print(f"✓ 正确捕获错误: {e.hint}")
        except Exception as e:
            print(f"✓ 捕获其他错误: {e}")


def test_join_bugfixes():
    """测试JOIN Bug修复效果"""
    print("=== JOIN Bug修复测试 ===")

    parser = Parser()

    # 之前失败的测试用例
    test_cases = [
        # 表.列引用测试
        ("SELECT s.name, sc.score FROM students s JOIN scores sc ON s.id = sc.student_id;", "表.列引用"),
        ("SELECT students.name, scores.score FROM students JOIN scores ON students.id = scores.student_id;",
         "无别名表.列"),

        # OUTER JOIN测试
        ("SELECT s.name, sc.score FROM students s LEFT OUTER JOIN scores sc ON s.id = sc.student_id;",
         "LEFT OUTER JOIN"),
        ("SELECT * FROM students s RIGHT OUTER JOIN scores sc ON s.id = sc.student_id;", "RIGHT OUTER JOIN"),

        # 复杂查询测试
        ("SELECT s.name, AVG(sc.score) as avg_score FROM students s LEFT JOIN scores sc ON s.id = sc.student_id WHERE s.major = 'CS' GROUP BY s.id, s.name;",
         "复杂JOIN查询"),

        # 三表连接测试
        ("SELECT s.name, c.title, sc.score FROM students s JOIN scores sc ON s.id = sc.student_id JOIN courses c ON sc.course_id = c.id;",
         "三表连接"),
    ]

    for i, (sql, desc) in enumerate(test_cases, 1):
        print(f"\n[修复测试 {i}] {desc}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print("✓ 解析成功")

            # 显示关键特性
            if isinstance(ast, SelectNode):
                # 检查列引用
                qualified_cols = []
                for col in ast.columns:
                    if isinstance(col, ColumnNode) and "." in col.name:
                        qualified_cols.append(col.name)
                    elif isinstance(col, AliasColumnNode) and "." in col.column_name:
                        qualified_cols.append(col.column_name)

                if qualified_cols:
                    print(f"   表.列引用: {qualified_cols}")

                # 分析FROM子句
                if isinstance(ast.from_clause, list) and len(ast.from_clause) > 1:
                    join_types = []
                    for item in ast.from_clause[1:]:
                        if isinstance(item, JoinNode):
                            join_types.append(item.join_type)
                    print(f"   JOIN类型: {join_types}")

        except ParseError as e:
            print(f"❌ 语法错误: {e.hint}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")


def test_join_error_fixes():
    """测试JOIN错误处理修复"""
    print("\n=== JOIN错误处理修复测试 ===")

    parser = Parser()

    error_cases = [
        # 应该报错的用例
        ("SELECT * FROM students FULL JOIN scores ON s.id = sc.id;", "FULL JOIN应该报错"),
        ("SELECT * FROM students FULL OUTER JOIN scores ON s.id = sc.id;", "FULL OUTER JOIN应该报错"),
        ("SELECT s.name, sc.score FROM students s, scores sc WHERE s.id = sc.student_id;", "隐式连接应该报错"),
    ]

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n[错误修复测试 {i}] {expected_error}")
        print(f"SQL: {sql}")
        try:
            ast = parser.parse(sql)
            print(f"❌ 应该报错但解析成功了")
        except ParseError as e:
            print(f"✓ 正确捕获错误: {e.hint}")
        except Exception as e:
            print(f"✓ 捕获其他错误: {e}")

if __name__ == "__main__":
    # test_s6s7_parser_extensions()
    # test_parser_error_cases()
    # test_having_bug_fix()
    test_s8_join_parser()
    test_join_error_cases()
    test_join_bugfixes()
    test_join_error_fixes()
