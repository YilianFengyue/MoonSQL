"""
SQL词法分析器
实现将SQL语句切分为Token四元组的功能
"""

import re
from enum import Enum
from typing import List, Tuple, NamedTuple

class TokenType(Enum):
    """Token种别码 - 符合任务书要求"""
    KEYWORD = "KEYWORD"          # SQL关键字：SELECT, FROM, WHERE等
    IDENTIFIER = "IDENTIFIER"    # 标识符：表名、列名
    CONST = "CONST"             # 常量（兼容，实际使用NUMBER/STRING）
    OPERATOR = "OPERATOR"        # 操作符：=, !=, <, >等
    DELIMITER = "DELIMITER"      # 分隔符：( ) , ; .等
    STRING = "STRING"           # 字符串常量：'Alice', "test"
    NUMBER = "NUMBER"           # 数字常量：123, 45.67
    EOF = "EOF"                 # 文件结束标记
    ERROR = "ERROR"             # 错误标记

class Token(NamedTuple):
    """Token四元组：[种别码, 词素值, 行号, 列号] - 任务书标准格式"""
    type: TokenType    # 种别码
    lexeme: str       # 词素值（源代码中的具体字符串）
    line: int         # 行号（从1开始）
    col: int          # 列号（从1开始）

    def __str__(self):
        """格式化输出"""
        return f"[{self.type.value}, {self.lexeme}, {self.line}, {self.col}]"

class SqlError(Exception):
    """SQL错误异常"""
    def __init__(self, error_type: str, line: int, col: int, hint: str):
        self.error_type = error_type
        self.line = line
        self.col = col
        self.hint = hint
        super().__init__(f"{error_type} at line {line}, col {col}: {hint}")

class Lexer:
    """SQL词法分析器"""

    # SQL关键字
    KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES',
        'DELETE', 'UPDATE', 'SET', 'AND', 'OR', 'NOT', 'NULL', 'INT', 'VARCHAR',
        'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'INDEX', 'DROP',
        'ALTER', 'ADD', 'COLUMN', 'ORDER', 'BY', 'GROUP', 'HAVING', 'DISTINCT',
        'AS', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'UNION', 'ALL',
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'EXISTS', 'BETWEEN', 'LIKE',
        'IN', 'IS', 'ASC', 'DESC', 'LIMIT', 'OFFSET','SHOW', 'TABLES','RENAME',
        'TO','MODIFY','CHAR'
    }

    # 操作符
    OPERATORS = {
        '=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', '||'
    }

    # 分隔符
    DELIMITERS = {
        '(', ')', ',', ';', '.', '[', ']', '{', '}'
    }

    def __init__(self):
        self.text = ""
        self.pos = 0
        self.line = 1
        self.col = 1
        self.tokens = []

    def tokenize(self, sql_text: str) -> List[Token]:
        """
        词法分析主函数
        Args:
            sql_text: SQL语句文本
        Returns:
            Token列表
        Raises:
            SqlError: 词法错误
        """
        self.text = sql_text
        self.pos = 0
        self.line = 1
        self.col = 1
        self.tokens = []

        while self.pos < len(self.text):
            self._skip_whitespace()

            if self.pos >= len(self.text):
                break

            if self._match_comment():
                continue

            start_line = self.line
            start_col = self.col

            # 尝试匹配各种Token
            if self._match_string():
                continue
            elif self._match_number():
                continue
            elif self._match_identifier_or_keyword():
                continue
            elif self._match_operator():
                continue
            elif self._match_delimiter():
                continue
            else:
                # 非法字符
                char = self.text[self.pos]
                raise SqlError("LexicalError", start_line, start_col,
                             f"Unexpected character '{char}'")

        # 添加EOF标记
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.col))
        return self.tokens

    def _current_char(self) -> str:
        """获取当前字符"""
        if self.pos >= len(self.text):
            return '\0'
        return self.text[self.pos]

    def _peek_char(self, offset: int = 1) -> str:
        """预读字符"""
        peek_pos = self.pos + offset
        if peek_pos >= len(self.text):
            return '\0'
        return self.text[peek_pos]

    def _advance(self) -> str:
        """前进一个字符"""
        if self.pos < len(self.text):
            char = self.text[self.pos]
            self.pos += 1
            if char == '\n':
                self.line += 1
                self.col = 1
            else:
                self.col += 1
            return char
        return '\0'

    def _skip_whitespace(self):
        """跳过空白字符"""
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self._advance()

    def _match_comment(self) -> bool:
        """匹配注释"""
        if self._current_char() == '-' and self._peek_char() == '-':
            # 单行注释
            while self.pos < len(self.text) and self._current_char() != '\n':
                self._advance()
            return True

        if self._current_char() == '/' and self._peek_char() == '*':
            # 多行注释
            self._advance()  # /
            self._advance()  # *
            while self.pos < len(self.text) - 1:
                if self._current_char() == '*' and self._peek_char() == '/':
                    self._advance()  # *
                    self._advance()  # /
                    return True
                self._advance()
            raise SqlError("LexicalError", self.line, self.col,
                         "Unterminated comment")

        return False

    def _match_string(self) -> bool:
        """匹配字符串常量"""
        if self._current_char() not in ["'", '"']:
            return False

        start_line = self.line
        start_col = self.col
        quote = self._advance()  # 开始引号
        value = ""

        while self.pos < len(self.text):
            char = self._current_char()
            if char == quote:
                self._advance()  # 结束引号
                self.tokens.append(Token(TokenType.STRING, value, start_line, start_col))
                return True
            elif char == '\\':
                # 转义字符
                self._advance()
                if self.pos < len(self.text):
                    escaped = self._advance()
                    if escaped == 'n':
                        value += '\n'
                    elif escaped == 't':
                        value += '\t'
                    elif escaped == 'r':
                        value += '\r'
                    elif escaped == '\\':
                        value += '\\'
                    elif escaped == quote:
                        value += quote
                    else:
                        value += escaped
            else:
                value += self._advance()

        raise SqlError("LexicalError", start_line, start_col,
                     f"Unterminated string literal")

    def _match_number(self) -> bool:
        """匹配数字常量"""
        if not self._current_char().isdigit():
            return False

        start_line = self.line
        start_col = self.col
        value = ""

        # 整数部分
        while self.pos < len(self.text) and self._current_char().isdigit():
            value += self._advance()

        # 小数部分
        if self._current_char() == '.' and self._peek_char().isdigit():
            value += self._advance()  # .
            while self.pos < len(self.text) and self._current_char().isdigit():
                value += self._advance()

        self.tokens.append(Token(TokenType.NUMBER, value, start_line, start_col))
        return True

    def _match_identifier_or_keyword(self) -> bool:
        """匹配标识符或关键字"""
        if not (self._current_char().isalpha() or self._current_char() == '_'):
            return False

        start_line = self.line
        start_col = self.col
        value = ""

        while (self.pos < len(self.text) and
               (self._current_char().isalnum() or self._current_char() == '_')):
            value += self._advance()

        # 判断是关键字还是标识符
        if value.upper() in self.KEYWORDS:
            token_type = TokenType.KEYWORD
            value = value.upper()  # 关键字统一大写
        else:
            token_type = TokenType.IDENTIFIER

        self.tokens.append(Token(token_type, value, start_line, start_col))
        return True

    def _match_operator(self) -> bool:
        """匹配操作符"""
        start_line = self.line
        start_col = self.col

        # 尝试双字符操作符
        two_char = self._current_char() + self._peek_char()
        if two_char in self.OPERATORS:
            self._advance()
            self._advance()
            self.tokens.append(Token(TokenType.OPERATOR, two_char, start_line, start_col))
            return True

        # 尝试单字符操作符
        one_char = self._current_char()
        if one_char in self.OPERATORS:
            self._advance()
            self.tokens.append(Token(TokenType.OPERATOR, one_char, start_line, start_col))
            return True

        return False

    def _match_delimiter(self) -> bool:
        """匹配分隔符"""
        char = self._current_char()
        if char in self.DELIMITERS:
            start_line = self.line
            start_col = self.col
            self._advance()
            self.tokens.append(Token(TokenType.DELIMITER, char, start_line, start_col))
            return True
        return False

def format_tokens(tokens: List[Token]) -> str:
    """格式化Token输出"""
    result = []
    result.append("=== Token Stream ===")
    result.append(f"{'Type':<12} {'Lexeme':<15} {'Line':<4} {'Col':<4}")
    result.append("-" * 40)

    for token in tokens:
        if token.type == TokenType.EOF:
            break
        result.append(f"{token.type.value:<12} {token.lexeme:<15} {token.line:<4} {token.col:<4}")

    return "\n".join(result)

# 测试函数
def test_lexer():
    """测试词法分析器"""
    test_sql = """
    CREATE TABLE student(
        id INT,
        name VARCHAR(50),
        age INT
    );
    INSERT INTO student VALUES(1, 'Alice', 20);
    SELECT id, name FROM student WHERE age > 18;
    """

    lexer = Lexer()
    try:
        tokens = lexer.tokenize(test_sql)
        print(format_tokens(tokens))
    except SqlError as e:
        print(f"Lexical Error: {e}")

if __name__ == "__main__":
    test_lexer()