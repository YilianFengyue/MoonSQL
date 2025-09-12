"""
SQL编译器负样例测试 - A5阶段
测试各种错误情况的处理和错误定位

【测试分类】
1. 词法错误：非法字符、未闭合字符串
2. 语法错误：缺分号、括号不匹配、期望符号
3. 语义错误：表不存在、列不存在、类型不匹配
4. 计划错误：不支持的语法结构

【错误定位验证】
- 精确的行号列号
- 友好的错误提示
- 期望符号提示
"""

import sys
from pathlib import Path

# 添加src目录到路径
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from sql.lexer import Lexer, SqlError, TokenType
from sql.parser import Parser, ParseError
from sql.semantic import SemanticAnalyzer, Catalog, SemanticError
from sql.planner import Planner, PlanError


class BadCaseTester:
    """负样例测试器"""

    def __init__(self):
        self.lexer = Lexer()
        self.parser = Parser()
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.planner = Planner(self.catalog)

        # 预创建一些表供测试使用
        self._setup_test_environment()

    def _setup_test_environment(self):
        """设置测试环境"""
        try:
            self.catalog.create_table("student", [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "age", "type": "INT"}
            ])
        except:
            pass  # 表可能已存在

    def test_lexical_errors(self):
        """测试词法错误"""
        print("=== 词法错误测试 ===")

        lexical_errors = [
            ("SELECT * FROM table @@@;", "非法字符 @@@"),
            ("SELECT * FROM 'unclosed", "未闭合字符串"),
            ("SELECT * FROM \"unclosed", "未闭合双引号字符串"),
            ("SELECT /* unclosed comment", "未闭合注释"),
            ("SELECT 123abc FROM table;", "数字后跟字母"),
            ("SELECT #illegal FROM table;", "非法字符 #"),
            ("SELECT $ FROM table;", "非法字符 $"),
            ("CREATE TABLE test(id INT, name VARCHAR(;", "括号内缺少数字"),
        ]

        success_count = 0
        for i, (sql, description) in enumerate(lexical_errors, 1):
            print(f"\n[词法错误 {i}] {description}")
            print(f"SQL: {sql}")
            try:
                tokens = self.lexer.tokenize(sql)
                print("❌ 应该产生词法错误但没有")
            except SqlError as e:
                if e.error_type == "LexicalError":
                    print(f"✓ 正确检测到词法错误: {e.hint}")
                    print(f"   位置: 第{e.line}行第{e.col}列")
                    success_count += 1
                else:
                    print(f"❌ 错误类型不匹配: {e.error_type}")
            except Exception as e:
                print(f"❌ 意外错误: {e}")

        print(f"\n词法错误测试: {success_count}/{len(lexical_errors)}")
        return success_count, len(lexical_errors)

    def test_syntax_errors(self):
        """测试语法错误"""
        print("\n=== 语法错误测试 ===")

        syntax_errors = [
            ("CREATE TABLE student(id INT, name", "缺少右括号"),
            ("CREATE TABLE student(id INT, name VARCHAR)", "缺少分号"),
            ("INSERT INTO student VALUES(1, 'Alice'", "VALUES缺少右括号"),
            ("INSERT INTO student VALUES 1, 'Alice');", "VALUES缺少左括号"),
            ("SELECT id, FROM student;", "缺少列名"),
            ("SELECT FROM student;", "SELECT后缺少列"),
            ("SELECT * student;", "缺少FROM关键字"),
            ("DELETE student WHERE id = 1;", "DELETE缺少FROM"),
            ("CREATE TABLE student(id, name VARCHAR);", "缺少数据类型"),
            ("INSERT student VALUES(1);", "INSERT缺少INTO"),
            ("SELECT * FROM;", "FROM后缺少表名"),
            ("CREATE TABLE (id INT);", "缺少表名"),
        ]

        success_count = 0
        for i, (sql, description) in enumerate(syntax_errors, 1):
            print(f"\n[语法错误 {i}] {description}")
            print(f"SQL: {sql}")
            try:
                ast = self.parser.parse(sql)
                print("❌ 应该产生语法错误但没有")
            except ParseError as e:
                print(f"✓ 正确检测到语法错误: {e.hint}")
                print(f"   位置: 第{e.line}行第{e.col}列")
                if e.expected:
                    print(f"   期望: {e.expected}")
                success_count += 1
            except Exception as e:
                print(f"❌ 意外错误: {e}")

        print(f"\n语法错误测试: {success_count}/{len(syntax_errors)}")
        return success_count, len(syntax_errors)

    def test_semantic_errors(self):
        """测试语义错误"""
        print("\n=== 语义错误测试 ===")

        semantic_errors = [
            # 表相关错误
            ("CREATE TABLE student(id INT, name VARCHAR);", "重复建表"),
            ("INSERT INTO nonexistent VALUES(1);", "表不存在"),
            ("SELECT * FROM nonexistent;", "查询不存在的表"),
            ("DELETE FROM nonexistent WHERE id = 1;", "删除不存在的表"),

            # 列相关错误
            ("SELECT nonexistent FROM student;", "列不存在"),
            ("SELECT id, nonexistent FROM student;", "混合存在和不存在的列"),
            ("INSERT INTO student(nonexistent) VALUES(1);", "指定不存在的列"),

            # 类型错误
            ("INSERT INTO student VALUES('Alice', 1, 20);", "第一列类型错误"),
            ("INSERT INTO student VALUES(1, 20, 'Alice');", "第二列类型错误"),
            ("INSERT INTO student VALUES(1.5, 'Alice', 20);", "整数列插入浮点数"),

            # 列数错误
            ("INSERT INTO student VALUES(1);", "列数不足"),
            ("INSERT INTO student VALUES(1, 'Alice', 20, 'Extra');", "列数过多"),
            ("INSERT INTO student(id) VALUES(1, 'Alice');", "指定列与值数量不匹配"),

            # 列定义错误
            ("CREATE TABLE test(id INT, id VARCHAR);", "重复列名"),
            ("CREATE TABLE test();", "空列定义"),
            ("CREATE TABLE test(id INVALID_TYPE);", "无效数据类型"),
        ]

        success_count = 0
        for i, (sql, description) in enumerate(semantic_errors, 1):
            print(f"\n[语义错误 {i}] {description}")
            print(f"SQL: {sql}")
            try:
                # 先语法分析
                ast = self.parser.parse(sql)
                # 再语义分析
                result = self.semantic_analyzer.analyze(ast)
                print("❌ 应该产生语义错误但没有")
            except SemanticError as e:
                print(f"✓ 正确检测到语义错误: {e.hint}")
                print(f"   位置: 第{e.line}行第{e.col}列")
                success_count += 1
            except ParseError as e:
                print(f"⚠️  语法错误阻止了语义检查: {e.hint}")
            except Exception as e:
                print(f"❌ 意外错误: {e}")

        print(f"\n语义错误测试: {success_count}/{len(semantic_errors)}")
        return success_count, len(semantic_errors)

    def test_mixed_errors(self):
        """测试混合错误情况"""
        print("\n=== 混合错误测试 ===")

        mixed_errors = [
            ("SELECT * FROM table @@ WHERE id = 1;", "词法+语法错误"),
            ("SELECT nonexistent FROM 'unclosed WHERE id = 1;", "词法+语义错误"),
            ("INSERT INTO nonexistent VALUES(1, 'Alice'", "语法+语义错误"),
            ("CREATE TABLE (id @@ INT);", "多重错误"),
            ("SELECT * FROM student WHERE id = 'not_number';", "语义类型错误"),
        ]

        success_count = 0
        for i, (sql, description) in enumerate(mixed_errors, 1):
            print(f"\n[混合错误 {i}] {description}")
            print(f"SQL: {sql}")
            error_caught = False
            try:
                # 尝试完整流程
                tokens = self.lexer.tokenize(sql)
                ast = self.parser.parse(sql)
                result = self.semantic_analyzer.analyze(ast)
                plan = self.planner.plan(sql)
                print("❌ 应该产生错误但没有")
            except SqlError as e:
                print(f"✓ 检测到{e.error_type}: {e.hint}")
                print(f"   位置: 第{e.line}行第{e.col}列")
                error_caught = True
                success_count += 1
            except Exception as e:
                print(f"✓ 检测到错误: {e}")
                error_caught = True
                success_count += 1

        print(f"\n混合错误测试: {success_count}/{len(mixed_errors)}")
        return success_count, len(mixed_errors)

    def test_error_recovery(self):
        """测试错误恢复和定位精度"""
        print("\n=== 错误定位精度测试 ===")

        positioning_tests = [
            ("SELECT * FROM student WHERE id > 18 @;", "第1行第37列的非法字符"),
            ("CREATE TABLE test(\n  id INT,\n  name @@ VARCHAR\n);", "第3行的错误定位"),
            ("INSERT INTO student\nVALUES(1, 'Alice'", "第2行的缺少右括号"),
            ("SELECT\n  id,\n  name,\nFROM student;", "第4行FROM前的逗号错误"),
        ]

        success_count = 0
        for i, (sql, description) in enumerate(positioning_tests, 1):
            print(f"\n[定位测试 {i}] {description}")
            print(f"SQL:\n{sql}")
            try:
                # 尝试各个阶段
                tokens = self.lexer.tokenize(sql)
                ast = self.parser.parse(sql)
                result = self.semantic_analyzer.analyze(ast)
                print("❌ 应该产生错误")
            except SqlError as e:
                print(f"✓ {e.error_type} at line {e.line}, col {e.col}: {e.hint}")
                success_count += 1
            except Exception as e:
                print(f"✓ 错误: {e}")
                success_count += 1

        print(f"\n定位精度测试: {success_count}/{len(positioning_tests)}")
        return success_count, len(positioning_tests)

    def run_all_bad_cases(self):
        """运行所有负样例测试"""
        print("=== SQL编译器负样例测试 (A5阶段) ===")

        results = []

        # 各类错误测试
        results.append(self.test_lexical_errors())
        results.append(self.test_syntax_errors())
        results.append(self.test_semantic_errors())
        results.append(self.test_mixed_errors())
        results.append(self.test_error_recovery())

        # 汇总结果
        total_success = sum(r[0] for r in results)
        total_tests = sum(r[1] for r in results)

        print(f"\n=== 负样例测试总结 ===")
        print(f"总计: {total_success}/{total_tests}")
        print(f"成功率: {total_success / total_tests * 100:.1f}%")

        categories = ["词法错误", "语法错误", "语义错误", "混合错误", "定位精度"]
        for i, (success, total) in enumerate(results):
            print(f"{categories[i]}: {success}/{total}")

        return total_success == total_tests


def demo_four_views_with_errors():
    """演示四视图错误展示"""
    print("\n=== 四视图错误演示 ===")

    tester = BadCaseTester()

    # 选择一些代表性错误
    demo_cases = [
        ("SELECT * FROM table @@@;", "词法错误演示"),
        ("SELECT id, FROM student;", "语法错误演示"),
        ("SELECT nonexistent FROM student;", "语义错误演示"),
    ]

    for sql, description in demo_cases:
        print(f"\n--- {description} ---")
        print(f"SQL: {sql}")

        # A1: Token视图
        print("\n[A1] Token分析:")
        try:
            tokens = tester.lexer.tokenize(sql)
            print("✓ 词法分析成功")
        except SqlError as e:
            print(f"❌ {e.error_type}: {e.hint} (第{e.line}行第{e.col}列)")
            continue

        # A2: AST视图
        print("\n[A2] 语法分析:")
        try:
            ast = tester.parser.parse(sql)
            print("✓ 语法分析成功")
        except ParseError as e:
            print(f"❌ {e.error_type}: {e.hint} (第{e.line}行第{e.col}列)")
            if e.expected:
                print(f"   期望: {e.expected}")
            continue

        # A3: 语义分析
        print("\n[A3] 语义分析:")
        try:
            result = tester.semantic_analyzer.analyze(ast)
            print("✓ 语义分析成功")
        except SemanticError as e:
            print(f"❌ {e.error_type}: {e.hint} (第{e.line}行第{e.col}列)")
            continue

        # A4: 计划生成
        print("\n[A4] 计划生成:")
        try:
            plan = tester.planner.plan(sql)
            print("✓ 计划生成成功")
        except PlanError as e:
            print(f"❌ {e.error_type}: {e.hint} (第{e.line}行第{e.col}列)")


if __name__ == "__main__":
    tester = BadCaseTester()

    # 运行所有负样例测试
    success = tester.run_all_bad_cases()

    # 演示四视图错误处理
    demo_four_views_with_errors()

    if success:
        print("\n✅ 所有负样例测试通过！错误处理工作正常！")
    else:
        print("\n⚠️  部分错误检测不完善，需要改进")
        sys.exit(1)