"""
SQL编译器综合测试 - A5阶段
测试Token→AST→Semantic→Plan完整流程

【测试范围】
1. 四个阶段的正确性验证
2. 任务书要求的SQL语句全覆盖
3. 四视图一键展示功能
4. 端到端流程测试

【测试用例】
- CREATE TABLE student(id INT, name VARCHAR, age INT);
- INSERT INTO student VALUES(1, 'Alice', 20);
- SELECT id, name FROM student WHERE age > 18;
- DELETE FROM student WHERE id = 1;
"""

import sys
import unittest
from pathlib import Path

# 添加src目录到路径
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from sql.lexer import Lexer, format_tokens, SqlError, TokenType
from sql.parser import Parser, format_ast, ParseError
from sql.semantic import SemanticAnalyzer, Catalog, SemanticError, format_semantic_result
from sql.planner import Planner, format_execution_plan, PlanError


class TestSQLCompiler(unittest.TestCase):
    """SQL编译器综合测试类"""

    def setUp(self):
        """测试前准备"""
        self.lexer = Lexer()
        self.parser = Parser()
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.planner = Planner(self.catalog)

    def test_full_pipeline_create_table(self):
        """测试CREATE TABLE完整流程"""
        sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"

        # A1: 词法分析
        tokens = self.lexer.tokenize(sql)
        self.assertGreater(len(tokens), 0)
        self.assertEqual(tokens[0].type, TokenType.KEYWORD)
        self.assertEqual(tokens[0].lexeme, "CREATE")

        # A2: 语法分析
        ast = self.parser.parse(sql)
        self.assertEqual(ast.__class__.__name__, "CreateTableNode")
        self.assertEqual(ast.table_name, "student")
        self.assertEqual(len(ast.columns), 3)

        # A3: 语义分析
        semantic_result = self.semantic_analyzer.analyze(ast)
        self.assertEqual(semantic_result["statement_type"], "CREATE_TABLE")
        self.assertEqual(semantic_result["table_name"], "student")
        self.assertEqual(len(semantic_result["columns"]), 3)

        # A4: 执行计划生成
        plan = self.planner.plan(sql)
        self.assertEqual(plan.get_operator(), "CreateTable")
        plan_dict = plan.to_dict()
        self.assertEqual(plan_dict["table"], "student")
        self.assertEqual(len(plan_dict["columns"]), 3)

    def test_full_pipeline_insert(self):
        """测试INSERT完整流程"""
        # 先创建表
        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
        create_ast = self.parser.parse(create_sql)
        self.semantic_analyzer.analyze(create_ast)

        # 测试INSERT
        sql = "INSERT INTO student VALUES(1, 'Alice', 20);"

        # A1: 词法分析
        tokens = self.lexer.tokenize(sql)
        self.assertEqual(tokens[0].lexeme, "INSERT")

        # A2: 语法分析
        ast = self.parser.parse(sql)
        self.assertEqual(ast.__class__.__name__, "InsertNode")
        self.assertEqual(ast.table_name, "student")
        self.assertEqual(len(ast.values), 3)

        # A3: 语义分析
        semantic_result = self.semantic_analyzer.analyze(ast)
        self.assertEqual(semantic_result["statement_type"], "INSERT")
        self.assertEqual(len(semantic_result["target_columns"]), 3)

        # A4: 执行计划生成
        plan = self.planner.plan(sql)
        self.assertEqual(plan.get_operator(), "Insert")
        plan_dict = plan.to_dict()
        self.assertEqual(plan_dict["table"], "student")

    def test_full_pipeline_select_simple(self):
        """测试简单SELECT完整流程"""
        # 先创建表
        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
        create_ast = self.parser.parse(create_sql)
        self.semantic_analyzer.analyze(create_ast)

        # 测试SELECT *
        sql = "SELECT * FROM student;"

        # A1: 词法分析
        tokens = self.lexer.tokenize(sql)
        self.assertEqual(tokens[0].lexeme, "SELECT")

        # A2: 语法分析
        ast = self.parser.parse(sql)
        self.assertEqual(ast.__class__.__name__, "SelectNode")
        self.assertEqual(ast.table_name, "student")
        self.assertEqual(ast.columns[0], "*")

        # A3: 语义分析
        semantic_result = self.semantic_analyzer.analyze(ast)
        self.assertEqual(semantic_result["statement_type"], "SELECT")
        self.assertEqual(len(semantic_result["selected_columns"]), 3)  # id, name, age

        # A4: 执行计划生成
        plan = self.planner.plan(sql)
        self.assertEqual(plan.get_operator(), "SeqScan")  # SELECT * 只需要SeqScan

    def test_full_pipeline_select_complex(self):
        """测试复杂SELECT完整流程"""
        # 先创建表
        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
        create_ast = self.parser.parse(create_sql)
        self.semantic_analyzer.analyze(create_ast)

        # 测试复杂SELECT
        sql = "SELECT id, name FROM student WHERE age > 18;"

        # A1: 词法分析
        tokens = self.lexer.tokenize(sql)
        token_types = [t.type for t in tokens if t.type != TokenType.EOF]
        self.assertIn(TokenType.KEYWORD, token_types)
        self.assertIn(TokenType.IDENTIFIER, token_types)
        self.assertIn(TokenType.OPERATOR, token_types)

        # A2: 语法分析
        ast = self.parser.parse(sql)
        self.assertEqual(ast.__class__.__name__, "SelectNode")
        self.assertEqual(len(ast.columns), 2)  # id, name
        self.assertIsNotNone(ast.where_clause)

        # A3: 语义分析
        semantic_result = self.semantic_analyzer.analyze(ast)
        self.assertEqual(semantic_result["statement_type"], "SELECT")
        self.assertEqual(semantic_result["selected_columns"], ["id", "name"])
        self.assertIsNotNone(semantic_result["where_clause"])

        # A4: 执行计划生成 - 应该是三层结构
        plan = self.planner.plan(sql)
        self.assertEqual(plan.get_operator(), "Project")  # 顶层应该是Project

        plan_dict = plan.to_dict()
        self.assertIn("child", plan_dict)

        # 检查Filter层
        filter_layer = plan_dict["child"]
        self.assertEqual(filter_layer["op"], "Filter")
        self.assertIn("child", filter_layer)

        # 检查SeqScan层
        seqscan_layer = filter_layer["child"]
        self.assertEqual(seqscan_layer["op"], "SeqScan")
        self.assertEqual(seqscan_layer["table"], "student")

    def test_full_pipeline_delete(self):
        """测试DELETE完整流程"""
        # 先创建表
        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
        create_ast = self.parser.parse(create_sql)
        self.semantic_analyzer.analyze(create_ast)

        # 测试DELETE
        sql = "DELETE FROM student WHERE id = 1;"

        # A1: 词法分析
        tokens = self.lexer.tokenize(sql)
        self.assertEqual(tokens[0].lexeme, "DELETE")

        # A2: 语法分析
        ast = self.parser.parse(sql)
        self.assertEqual(ast.__class__.__name__, "DeleteNode")
        self.assertEqual(ast.table_name, "student")
        self.assertIsNotNone(ast.where_clause)

        # A3: 语义分析
        semantic_result = self.semantic_analyzer.analyze(ast)
        self.assertEqual(semantic_result["statement_type"], "DELETE")
        self.assertEqual(semantic_result["table_name"], "student")

        # A4: 执行计划生成
        plan = self.planner.plan(sql)
        self.assertEqual(plan.get_operator(), "Delete")

    def test_four_views_integration(self):
        """测试四视图集成功能"""
        sql = "SELECT id, name FROM student WHERE age > 18;"

        # 准备环境
        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
        create_ast = self.parser.parse(create_sql)
        self.semantic_analyzer.analyze(create_ast)

        # 四视图测试
        views = {}

        # View 1: Token
        tokens = self.lexer.tokenize(sql)
        views["tokens"] = format_tokens(tokens)

        # View 2: AST
        ast = self.parser.parse(sql)
        views["ast"] = format_ast(ast)

        # View 3: Semantic
        semantic_result = self.semantic_analyzer.analyze(ast)
        views["semantic"] = format_semantic_result(semantic_result)

        # View 4: Plan
        plan = self.planner.plan(sql)
        views["plan"] = format_execution_plan(plan)

        # 验证所有视图都有内容
        for view_name, content in views.items():
            self.assertIsNotNone(content)
            self.assertGreater(len(content), 0)
            print(f"\n=== {view_name.upper()} VIEW ===")
            print(content)

    def test_task_required_statements(self):
        """测试任务书要求的所有语句"""
        statements = [
            "CREATE TABLE student(id INT, name VARCHAR, age INT);",
            "INSERT INTO student VALUES(1, 'Alice', 20);",
            "SELECT id, name FROM student WHERE age > 18;",
            "DELETE FROM student WHERE id = 1;",
            "SELECT * FROM student;",
            "INSERT INTO student(id, name, age) VALUES(2, 'Bob', 22);"
        ]

        results = []

        for i, sql in enumerate(statements):
            try:
                # 完整流程测试
                tokens = self.lexer.tokenize(sql)
                ast = self.parser.parse(sql)

                # 语义分析和计划生成需要表存在
                if not sql.startswith("CREATE") and "student" in sql:
                    if not self.catalog.table_exists("student"):
                        # 先创建表
                        create_sql = "CREATE TABLE student(id INT, name VARCHAR, age INT);"
                        create_ast = self.parser.parse(create_sql)
                        self.semantic_analyzer.analyze(create_ast)

                semantic_result = self.semantic_analyzer.analyze(ast)
                plan = self.planner.plan(sql)

                results.append({
                    "sql": sql,
                    "tokens": len(tokens) - 1,  # 减去EOF
                    "ast": ast.__class__.__name__,
                    "semantic": semantic_result["statement_type"],
                    "plan": plan.get_operator(),
                    "status": "SUCCESS"
                })

            except Exception as e:
                results.append({
                    "sql": sql,
                    "status": "FAILED",
                    "error": str(e)
                })

        # 验证所有语句都成功
        success_count = sum(1 for r in results if r["status"] == "SUCCESS")
        print(f"\n=== 任务书语句测试结果 ===")
        for result in results:
            if result["status"] == "SUCCESS":
                print(f"✓ {result['sql'][:50]}... -> {result['plan']}")
            else:
                print(f"❌ {result['sql'][:50]}... -> {result['error']}")

        print(f"成功率: {success_count}/{len(statements)}")
        self.assertEqual(success_count, len(statements))


def run_comprehensive_tests():
    """运行综合测试"""
    print("=== SQL编译器综合测试 (A5阶段) ===")

    # 创建测试套件
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSQLCompiler)

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # 输出总结
    print(f"\n=== 测试总结 ===")
    print(f"运行测试: {result.testsRun}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")

    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")

    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_comprehensive_tests()
    if success:
        print("\n🎉 所有测试通过！SQL编译器四个阶段工作正常！")
    else:
        print("\n❌ 部分测试失败，需要修复")
        sys.exit(1)