"""
MiniDB命令行接口
支持四视图显示：--show=token|ast|sem|plan
支持交互式和批处理模式
"""

import argparse
import sys
import os
from pathlib import Path

# 添加src目录到路径，方便导入
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from sql.lexer import Lexer, format_tokens, SqlError
from storage.file_manager import FileManager
from storage.page import SlottedPage
from sql.parser import Parser, format_ast, ParseError
from sql.semantic import SemanticAnalyzer, Catalog, SemanticError, format_semantic_result
# 在已有导入后添加：
from sql.planner import Planner, ExecutionPlan, PlanError, format_execution_plan
# 在已有导入后添加：
import os
import subprocess
from pathlib import Path

# 尝试导入测试模块（如果存在）
try:
    from tests.test_sql import TestSQLCompiler, run_comprehensive_tests
    from tests.bad_cases import BadCaseTester, demo_four_views_with_errors

    TESTS_AVAILABLE = True
except ImportError:
    TESTS_AVAILABLE = False

class MiniDBCLI:
    """MiniDB命令行接口"""


    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.lexer = Lexer()
        self.file_manager = FileManager(data_dir)
        self.show_mode = None
        # A3阶段：语义分析器和Catalog
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        # A4阶段：执行计划生成器
        self.planner = Planner(self.catalog)
    def run_interactive(self):
        # 修改交互模式开头的提示：
        print("=== MiniDB Interactive CLI (A1-A5完整版) ===")
        print("当前阶段：四阶段编译器完成")
        print()
        print("命令:")
        print("  .help    - 显示帮助")
        print("  .exit    - 退出")
        print("  .stats   - 显示系统统计")
        print("  .tables  - 列出所有表")
        print("  .all     - 四视图演示")
        print("  \\show <mode> - 设置显示模式")
        print()
        print("测试命令:")
        print("  .test1   - A1阶段测试（词法分析器）")
        print("  .test2   - A2阶段测试（语法分析器）")
        print("  .test3   - A3阶段测试（语义分析器）")
        print("  .test4   - A4阶段测试（执行计划生成器）")
        print("  .test5   - A5阶段测试（编译器联测）")
        print("  .badcases - 负样例测试")
        print("  .fulltest - 运行完整测试套件")
        print()
        print("显示模式:")
        print("  token - 显示词法分析结果 (A1阶段)")
        print("  ast   - 显示语法分析树 (A2阶段)")
        print("  sem   - 显示语义分析结果 (A3阶段)")
        print("  plan  - 显示执行计划 (A4阶段)")


        while True:
            try:
                line = input("minidb> ").strip()

                if not line:
                    continue

                if line == '.exit':
                    break
                elif line == '.help':
                    self._show_help()
                elif line == '.stats':
                    self._show_stats()
                elif line == '.tables':
                    self._show_tables()
                # 在run_interactive方法的命令处理中添加：
                elif line == '.test2':
                    self._run_a2_tests()
                elif line == '.test3':
                    self._run_a3_tests()
                elif line == '.test4':
                    self._run_a4_tests()
                # 在elif line == '.test4':之后添加：
                elif line == '.test5':
                    self._run_a5_tests()
                elif line == '.all':
                    self._show_four_views_demo()
                elif line == '.badcases':
                    self._run_bad_cases()
                elif line == '.fulltest':
                    self._run_full_test_suite()
                elif line.startswith('\\show'):
                    self._handle_show_command(line)
                else:
                    self._process_sql(line)

            except KeyboardInterrupt:
                print("\nUse .exit to quit")
            except EOFError:
                break

        print("Goodbye!")
        self._cleanup()

    def run_single(self, sql: str, show_mode: str = None):
        """单次执行模式"""
        self.show_mode = show_mode
        self._process_sql(sql)
        self._cleanup()

    def _process_sql(self, sql: str):
        """处理SQL语句"""
        try:
            if self.show_mode == 'token' or self.show_mode is None:
                # 词法分析
                tokens = self.lexer.tokenize(sql)

                if self.show_mode == 'token':
                    print(format_tokens(tokens))
                else:
                    # 默认模式：简化输出
                    print("✓ Lexical analysis completed")
                    print(f"  Tokens: {len(tokens) - 1}")  # 减去EOF


            elif self.show_mode == 'ast':

                # A2阶段：语法分析

                print("=== 语法分析 ===")

                try:

                    parser = Parser()

                    ast = parser.parse(sql)

                    print("✓ 语法分析成功")

                    print("\n=== AST结构 ===")

                    print(format_ast(ast))

                    print(f"\n=== AST字典表示 ===")

                    import json

                    print(json.dumps(ast.to_dict(), indent=2, ensure_ascii=False))

                except ParseError as e:

                    print(f"❌ 语法错误：{e.hint}")

                    print(f"   位置：第{e.line}行，第{e.col}列")

                    if e.expected:
                        print(f"   期望：{e.expected}")


            elif self.show_mode == 'sem':

                # A3阶段：语义分析

                print("=== 语义分析 ===")

                try:

                    # 先进行语法分析
                    parser = Parser()
                    ast = parser.parse(sql)
                    print("✓ 语法分析成功")
                    # 再进行语义分析
                    result = self.semantic_analyzer.analyze(ast)
                    print("✓ 语义分析成功")
                    print("\n" + format_semantic_result(result))

                    # 显示当前Catalog状态
                    stats = self.catalog.get_stats()
                    print(f"\n=== 当前Catalog状态 ===")
                    print(f"表数量: {stats['table_count']}")
                    if stats['tables']:
                        print("已创建的表:")
                        for table_name, col_count in stats['tables'].items():
                            print(f"  - {table_name}: {col_count}列")


                except ParseError as e:
                    print(f"❌ 语法错误：{e.hint}")
                    print(f"   位置：第{e.line}行，第{e.col}列")

                except SemanticError as e:
                    print(f"❌ 语义错误：{e.hint}")
                    print(f"   位置：第{e.line}行，第{e.col}列")


            elif self.show_mode == 'plan':

                # A4阶段：执行计划生成
                print("=== 执行计划生成 ===")

                try:

                    # 生成执行计划
                    plan = self.planner.plan(sql)

                    print("✓ 执行计划生成成功")
                    print("\n=== 计划树结构 ===")
                    print(format_execution_plan(plan))
                    print(f"\n=== JSON格式 ===")
                    print(plan.to_json())

                    # 显示计划摘要
                    print(f"\n=== 计划摘要 ===")
                    print(f"根算子: {plan.get_operator()}")
                    plan_dict = plan.to_dict()
                    if "estimated_cost" in plan_dict:
                        print(f"预估代价: {plan_dict['estimated_cost']}")
                    if "estimated_rows" in plan_dict:
                        print(f"预估行数: {plan_dict['estimated_rows']}")

                except (PlanError, ParseError, SemanticError) as e:
                    print(f"❌ 计划生成错误：{e.hint}")
                    print(f"   位置：第{e.line}行，第{e.col}列")

        except SqlError as e:
            print(f"❌ {e}")
        except Exception as e:
            print(f"❌ Internal error: {e}")

    def _handle_show_command(self, command: str):
        """处理show命令"""
        parts = command.split()
        if len(parts) != 2:
            print("Usage: \\show <mode>")
            print("Modes: token, ast, sem, plan")
            return

        mode = parts[1].lower()
        if mode in ['token', 'ast', 'sem', 'plan']:
            self.show_mode = mode
            print(f"Show mode set to: {mode}")
        else:
            print("Invalid mode. Use: token, ast, sem, plan")

    def _show_help(self):
        """显示帮助"""
        print("""
=== MiniDB帮助 (A1-A5完整版) ===

当前实现: SQL编译器四个阶段完成

可用命令:
  .help     - 显示此帮助信息
  .exit     - 退出CLI
  .stats    - 显示系统统计
  .tables   - 列出所有表
  .all      - 四视图演示（输入SQL显示四个阶段）

单阶段测试:
  .test1    - A1阶段测试（词法分析器）
  .test2    - A2阶段测试（语法分析器）
  .test3    - A3阶段测试（语义分析器）
  .test4    - A4阶段测试（执行计划生成器）
  .test5    - A5阶段测试（编译器联测）

综合测试:
  .badcases - 负样例测试（错误处理）
  .fulltest - 完整测试套件（A1-A5）

显示模式 (\\show <mode>):
  token - 显示词法分析结果 (A1阶段)
  ast   - 显示语法分析树 (A2阶段) 
  sem   - 显示语义分析结果 (A3阶段)
  plan  - 显示执行计划 (A4阶段)

支持的SQL示例:
  CREATE TABLE student(id INT, name VARCHAR, age INT);
  INSERT INTO student VALUES(1, 'Alice', 20);
  SELECT id, name FROM student WHERE age > 18;
  DELETE FROM student WHERE id = 1;

四视图演示用法:
  1. 输入 .all
  2. 然后输入任意SQL语句
  3. 系统将显示四个编译阶段的完整输出
""")

        print("  .test4   - 运行A4阶段测试（执行计划生成器）")
        print("  .test3   - 运行A3阶段测试（语义分析器）")
    def _show_stats(self):
        """显示系统统计"""
        stats = self.file_manager.get_stats()
        print("=== System Statistics ===")
        print(f"Data Directory: {stats['data_directory']}")
        print(f"Open Files: {stats['open_files']}")
        print(f"Total Pages: {stats['total_pages']}")
        print(f"Dirty Pages: {stats['dirty_pages']}")
        print(f"Disk Usage: {stats['disk_usage_mb']:.2f} MB")

    def _show_tables(self):
        """显示所有表"""
        tables = self.file_manager.list_tables()
        print("=== Tables ===")
        if tables:
            for i, table in enumerate(tables, 1):
                page_count = self.file_manager.get_table_page_count(table)
                print(f"{i:2d}. {table} ({page_count} pages)")
        else:
            print("No tables found")

    def _cleanup(self):
        """清理资源"""
        self.file_manager.close_all()

    # 在MiniDBCLI类中添加新方法：
    def _run_a2_tests(self):
        """运行A2阶段测试：语法分析器"""
        print("=== A2阶段测试：语法分析器 ===")

        test_cases = [
            # 正确用例
            ("CREATE TABLE student(id INT, name VARCHAR);", "建表语句"),
            ("INSERT INTO student VALUES(1, 'Alice');", "插入语句（VALUES）"),
            ("INSERT INTO student(id, name) VALUES(1, 'Bob');", "插入语句（指定列）"),
            ("SELECT id, name FROM student;", "查询语句（指定列）"),
            ("SELECT * FROM student;", "查询语句（全部列）"),
            ("SELECT * FROM student WHERE id > 0;", "查询语句（带WHERE）"),
            ("DELETE FROM student WHERE id = 1;", "删除语句"),

            # 错误用例 - 验证"期望符号"提示
            ("CREATE TABLE student(id INT, name", "缺少右括号"),
            ("INSERT INTO student VALUES(1, 'Alice'", "缺少右括号"),
            ("SELECT id, FROM student;", "缺少列名"),
            ("DELETE student WHERE id = 1;", "缺少FROM关键字"),
            ("CREATE TABLE student(id, name VARCHAR);", "缺少数据类型"),
        ]

        parser = Parser()
        success_count = 0

        for i, (sql, desc) in enumerate(test_cases, 1):
            print(f"\n[测试 {i}] {desc}")
            print(f"SQL: {sql}")
            try:
                ast = parser.parse(sql)
                print(f"✓ 成功 - {ast.__class__.__name__}")
                success_count += 1
            except ParseError as e:
                print(f"❌ {e.error_type} - {e.hint}")
                if e.expected:
                    print(f"   期望: {e.expected}")

        print(f"\n=== 测试总结 ===")
        print(f"成功: {success_count}/{len(test_cases)}")

    def _run_a3_tests(self):
        """运行A3阶段测试：语义分析器"""
        print("=== A3阶段测试：语义分析器 ===")

        # 重置catalog确保干净的测试环境
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)

        test_cases = [
            # 正确用例序列（有依赖关系）
            ("CREATE TABLE student(id INT, name VARCHAR);", "建表语句"),
            ("INSERT INTO student VALUES(1, 'Alice');", "插入数据（全列）"),
            ("INSERT INTO student(id, name) VALUES(2, 'Bob');", "插入数据（指定列）"),
            ("SELECT id, name FROM student;", "查询指定列"),
            ("SELECT * FROM student;", "查询所有列"),
            ("SELECT * FROM student WHERE id > 0;", "条件查询"),
            ("DELETE FROM student WHERE id = 1;", "条件删除"),

            # 错误用例
            ("CREATE TABLE student(id INT, name VARCHAR);", "重复建表"),
            ("INSERT INTO nonexistent VALUES(1);", "表不存在"),
            ("INSERT INTO student VALUES(1);", "列数不匹配"),
            ("INSERT INTO student VALUES('Alice', 1);", "类型不匹配"),
            ("SELECT nonexistent FROM student;", "列不存在"),
            ("SELECT * FROM nonexistent;", "查询不存在的表"),
        ]

        success_count = 0

        for i, (sql, desc) in enumerate(test_cases, 1):
            print(f"\n[测试 {i}] {desc}")
            print(f"SQL: {sql}")
            try:
                parser = Parser()
                ast = parser.parse(sql)
                result = self.semantic_analyzer.analyze(ast)
                print(f"✓ 成功 - {result['statement_type']}")
                success_count += 1
            except (SemanticError, ParseError) as e:
                print(f"❌ {e.error_type} - {e.hint}")

        print(f"\n=== 测试总结 ===")
        print(f"成功: {success_count}/{len(test_cases)}")

        # 显示最终catalog状态
        stats = self.catalog.get_stats()
        print(f"\n=== 最终Catalog状态 ===")
        print(f"表数量: {stats['table_count']}")
        for table_name, col_count in stats['tables'].items():
            print(f"  - {table_name}: {col_count}列")

    def _run_a4_tests(self):
        """运行A4阶段测试：执行计划生成器"""
        print("=== A4阶段测试：执行计划生成器 ===")

        # 重置并准备测试环境
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.planner = Planner(self.catalog)

        # 预先创建表供测试使用
        try:
            self.catalog.create_table("student", [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "age", "type": "INT"}
            ])
            print("✓ 测试环境准备完成")
        except Exception as e:
            print(f"❌ 环境准备失败: {e}")
            return

        test_cases = [
            # 五个基础算子测试
            ("CREATE TABLE test(id INT, name VARCHAR);", "CreateTable算子"),
            ("INSERT INTO student VALUES(1, 'Alice', 20);", "Insert算子"),
            ("SELECT * FROM student;", "SeqScan算子"),
            ("SELECT id, name FROM student;", "Project算子"),
            ("SELECT * FROM student WHERE age > 18;", "Filter算子"),

            # 复合计划测试
            ("SELECT id, name FROM student WHERE age > 18;", "Project + Filter + SeqScan"),
            ("DELETE FROM student WHERE id = 1;", "Delete + Filter + SeqScan"),

            # 边界情况
            ("SELECT * FROM student WHERE id = 1 AND age > 18;", "复杂条件"),
        ]

        success_count = 0

        for i, (sql, desc) in enumerate(test_cases, 1):
            print(f"\n[测试 {i}] {desc}")
            print(f"SQL: {sql}")
            try:
                plan = self.planner.plan(sql)
                print(f"✓ 成功 - {plan.get_operator()}")

                # 显示简化的计划信息
                plan_dict = plan.to_dict()
                if "description" in plan_dict:
                    print(f"   描述: {plan_dict['description']}")

                success_count += 1
            except (PlanError, ParseError, SemanticError) as e:
                print(f"❌ {e.error_type} - {e.hint}")

        print(f"\n=== 测试总结 ===")
        print(f"成功: {success_count}/{len(test_cases)}")

        # 演示完整计划树
        print(f"\n=== 计划树演示 ===")
        demo_sql = "SELECT id, name FROM student WHERE age > 18;"
        try:
            demo_plan = self.planner.plan(demo_sql)
            print(f"演示SQL: {demo_sql}")
            print("计划树:")
            print(format_execution_plan(demo_plan))
        except Exception as e:
            print(f"演示失败: {e}")

    def _show_four_views_demo(self):
        """演示四视图功能"""
        print("=== 四视图演示 (A5阶段) ===")
        print("请输入SQL语句，将显示四个编译阶段的输出：")

        sql = input("SQL> ").strip()
        if not sql:
            print("未输入SQL语句")
            return

        print(f"\n分析SQL: {sql}")
        print("=" * 60)

        # 确保有分号
        if not sql.endswith(';'):
            sql += ';'

        # A1: Token视图
        print("\n【A1 词法分析 - Token视图】")
        try:
            tokens = self.lexer.tokenize(sql)
            print(format_tokens(tokens))
        except SqlError as e:
            print(f"❌ 词法错误: {e.hint} (第{e.line}行第{e.col}列)")
            return

        # A2: AST视图
        print("\n【A2 语法分析 - AST视图】")
        try:
            parser = Parser()
            ast = parser.parse(sql)
            print(format_ast(ast))
        except ParseError as e:
            print(f"❌ 语法错误: {e.hint} (第{e.line}行第{e.col}列)")
            if e.expected:
                print(f"   期望: {e.expected}")
            return

        # A3: 语义分析视图
        print("\n【A3 语义分析 - Semantic视图】")
        try:
            result = self.semantic_analyzer.analyze(ast)
            print(format_semantic_result(result))
        except SemanticError as e:
            print(f"❌ 语义错误: {e.hint} (第{e.line}行第{e.col}列)")
            return

        # A4: 执行计划视图
        print("\n【A4 执行计划 - Plan视图】")
        try:
            plan = self.planner.plan(sql)
            print(format_execution_plan(plan))
            print(f"\nJSON格式:")
            print(plan.to_json())
        except PlanError as e:
            print(f"❌ 计划错误: {e.hint} (第{e.line}行第{e.col}列)")

        print("\n" + "=" * 60)
        print("四视图展示完成！")

    def _run_a5_tests(self):
        """运行A5阶段综合测试"""
        print("=== A5阶段测试：编译器联测 ===")

        if not TESTS_AVAILABLE:
            print("❌ 测试模块不可用，请确保tests目录存在")
            return

        try:
            print("运行正面用例测试...")
            success = run_comprehensive_tests()

            if success:
                print("\n✅ A5阶段测试全部通过！")
            else:
                print("\n⚠️  部分A5测试失败")

        except Exception as e:
            print(f"❌ 测试运行失败: {e}")

    def _run_bad_cases(self):
        """运行负样例测试"""
        print("=== 负样例测试 ===")

        if not TESTS_AVAILABLE:
            print("❌ 测试模块不可用")
            return

        try:
            tester = BadCaseTester()
            success = tester.run_all_bad_cases()

            if success:
                print("\n✅ 负样例测试全部通过！")
            else:
                print("\n⚠️  部分负样例测试需要改进")

            # 演示错误处理
            print("\n--- 错误处理演示 ---")
            demo_four_views_with_errors()

        except Exception as e:
            print(f"❌ 负样例测试失败: {e}")

    def _run_full_test_suite(self):
        """运行完整测试套件"""
        print("=== 完整测试套件 (A1-A5) ===")

        print("\n[1/5] 词法分析器测试 (A1)")
        self._run_a1_tests()

        print("\n[2/5] 语法分析器测试 (A2)")
        self._run_a2_tests()

        print("\n[3/5] 语义分析器测试 (A3)")
        self._run_a3_tests()

        print("\n[4/5] 执行计划生成器测试 (A4)")
        self._run_a4_tests()

        print("\n[5/5] 编译器联测 (A5)")
        if TESTS_AVAILABLE:
            self._run_a5_tests()
            self._run_bad_cases()
        else:
            print("❌ A5测试模块不可用")

        print("\n=== 完整测试套件结束 ===")

# 在帮助信息中添加：
print("  .test2   - 运行A2阶段测试（语法分析器）")
def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="MiniDB - A simple SQL database system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python minidb_cli.py --interactive
  python minidb_cli.py --show=token "SELECT * FROM student;"
  python minidb_cli.py -f script.sql
        """
    )

    # 模式选择
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--interactive', '-i', action='store_true',
                           help='启动交互模式')
    mode_group.add_argument('--file', '-f', type=str,
                           help='从文件执行SQL')
    mode_group.add_argument('sql', nargs='?',
                           help='要执行的SQL语句')

    # 显示选项 - A1阶段专注token
    parser.add_argument('--show', choices=['token', 'ast', 'sem', 'plan'],
                       default='token',
                       help='编译阶段显示模式 (当前支持: token)')

    # 数据目录
    parser.add_argument('--data-dir', '-d', default='data',
                       help='数据目录 (默认: data)')

    # 其他选项
    parser.add_argument('--version', action='version', version='MiniDB 1.0 (A1阶段)')

    args = parser.parse_args()

    # 创建CLI实例
    cli = MiniDBCLI(args.data_dir)

    try:
        if args.interactive:
            # 交互模式
            cli.run_interactive()
        elif args.file:
            # 文件模式
            try:
                with open(args.file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # 处理多条SQL语句
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                for i, stmt in enumerate(statements, 1):
                    print(f"\n=== 语句 {i} ===")
                    print(f"SQL: {stmt};")
                    cli.run_single(stmt + ';', args.show)

            except FileNotFoundError:
                print(f"错误: 文件 '{args.file}' 不存在")
                sys.exit(1)
            except Exception as e:
                print(f"读取文件错误: {e}")
                sys.exit(1)
        elif args.sql:
            # 单语句模式
            cli.run_single(args.sql, args.show)
        else:
            # 默认进入交互模式
            print("=== MiniDB A1阶段 ===")
            print("专注于词法分析器实现")
            print("使用 --help 查看选项")
            print()
            cli.run_interactive()

    except KeyboardInterrupt:
        print("\n程序中断")
        sys.exit(1)

if __name__ == "__main__":
    main()