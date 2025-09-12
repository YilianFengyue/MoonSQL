# 文件路径: MoonSQL/src/cli/minidb_cli.py

"""
MiniDB完整集成CLI
集成A阶段SQL编译器 + B阶段存储引擎 + C阶段执行器
支持真正的SQL执行和数据持久化
"""

import argparse
import sys
import os
import json
import time
from pathlib import Path
from tabulate import tabulate
import traceback  # ★ 新增：打印完整堆栈

# 添加src目录到路径
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

# A阶段：SQL编译器
try:
    from sql.lexer import Lexer, format_tokens, SqlError
    from sql.parser import Parser, format_ast, ParseError
    from sql.semantic import SemanticAnalyzer, Catalog, SemanticError, format_semantic_result
    from sql.planner import Planner, ExecutionPlan, PlanError, format_execution_plan

    SQL_COMPILER_AVAILABLE = True
except ImportError as e:
    print(f"A阶段SQL编译器导入失败: {e}")
    SQL_COMPILER_AVAILABLE = False

# B+C阶段：存储引擎和执行器
try:
    from storage.storage_engine import StorageEngine
    from engine.executor import Executor
    from engine.catalog_mgr import CatalogManager

    STORAGE_ENGINE_AVAILABLE = True
except ImportError as e:
    print(f"B+C阶段存储引擎导入失败: {e}")
    traceback.print_exc()  # ★ 新增：输出完整堆栈，精确到文件与行号
    STORAGE_ENGINE_AVAILABLE = False

# 测试模块
try:
    from tests.test_sql import TestSQLCompiler, run_comprehensive_tests
    from tests.bad_cases import BadCaseTester, demo_four_views_with_errors

    TESTS_AVAILABLE = True
except ImportError:
    TESTS_AVAILABLE = False


class IntegratedMiniDBCLI:
    """完整集成的MiniDB CLI"""

    def __init__(self, data_dir: str = "minidb_data"):
        self.data_dir = data_dir
        self.show_mode = "result"  # 默认显示执行结果

        # 初始化A阶段组件（如果可用）
        if SQL_COMPILER_AVAILABLE:
            self.lexer = Lexer()
            self.a_stage_catalog = Catalog()  # A阶段的内存catalog
            self.semantic_analyzer = SemanticAnalyzer(self.a_stage_catalog)
            self.a_stage_planner = Planner(self.a_stage_catalog)
            print("✓ A阶段SQL编译器已加载")
        else:
            print("⚠ A阶段SQL编译器不可用，将使用简化解析")

        # 初始化B+C阶段组件（如果可用）
        if STORAGE_ENGINE_AVAILABLE:
            print("正在初始化存储引擎...")
            self.storage_engine = StorageEngine(data_dir, buffer_capacity=32, buffer_policy="LRU")
            self.catalog_manager = CatalogManager(self.storage_engine)
            self.executor = Executor(self.storage_engine)
            print("✓ B+C阶段存储引擎已加载")
        else:
            print("❌ B+C阶段存储引擎不可用")


        # 检查集成状态
        self.fully_integrated = SQL_COMPILER_AVAILABLE and STORAGE_ENGINE_AVAILABLE

        if self.fully_integrated:
            print("🎉 MiniDB完整集成成功！")
        else:
            print("⚠ 部分组件不可用，功能受限")

    def run_interactive(self):
        """启动交互模式"""
        self._show_banner()

        while True:
            try:
                line = input("minidb> ").strip()

                if not line:
                    continue

                if line.startswith('.'):
                    self._handle_system_command(line)
                else:
                    self._process_sql_statement(line)

            except KeyboardInterrupt:
                print("\n使用 .exit 退出")
            except EOFError:
                break

        print("再见!")
        self._cleanup()

    def _show_banner(self):
        """显示启动横幅"""
        print("\n" + "=" * 80)
        print("   MiniDB - 完整集成版 SQL 数据库系统")
        print("=" * 80)

        if self.fully_integrated:
            print("🎯 功能状态: 完整集成 (A+B+C阶段)")
            print("   ✓ SQL编译器 (词法/语法/语义/计划)")
            print("   ✓ 存储引擎 (页面/文件/缓冲/持久化)")
            print("   ✓ 执行引擎 (五大算子/系统目录)")
            print("   ✓ 真正的SQL执行和数据存储")
        else:
            print("⚠ 功能状态: 部分可用")

        print("\n📋 系统命令:")
        print("   .help     - 显示完整帮助")
        print("   .exit     - 退出系统")
        print("   .tables   - 列出所有表")
        print("   .schema <table> - 显示表结构")
        print("   .stats    - 显示系统统计")

        if self.fully_integrated:
            print("\n🔧 调试命令:")
            print("   .show <mode> - 设置显示模式")
            print("   .fourview    - 四视图演示")
            print("   .demo        - 演示完整SQL功能")
            print("\n📊 显示模式:")
            print("   result   - 显示执行结果 (默认)")
            print("   token    - 显示词法分析")
            print("   ast      - 显示语法分析")
            print("   semantic - 显示语义分析")
            print("   plan     - 显示执行计划")
            print("   all      - 显示所有阶段")

        print("\n📚 SQL示例:")
        print("   CREATE TABLE users(id INT, name VARCHAR(50), age INT);")
        print("   INSERT INTO users VALUES(1, 'Alice', 25);")
        print("   SELECT id,name FROM users WHERE age > 20;")
        print("   DELETE FROM users WHERE id = 1;")
        print()

    def _handle_system_command(self, command: str):
        """处理系统命令"""
        cmd = command.lower().split()

        if cmd[0] == '.exit':
            print("再见!")
            self._cleanup()
            sys.exit(0)
        elif cmd[0] == '.help':
            self._show_detailed_help()
        elif cmd[0] == '.tables':
            self._show_tables()
        elif cmd[0] == '.schema':
            if len(cmd) > 1:
                self._show_schema(cmd[1])
            else:
                print("用法: .schema <table_name>")
        elif cmd[0] == '.stats':
            self._show_stats()
        elif cmd[0] == '.show':
            if len(cmd) > 1:
                self._set_show_mode(cmd[1])
            else:
                print(f"当前显示模式: {self.show_mode}")
        elif cmd[0] == '.fourview':
            self._demo_four_views()
        elif cmd[0] == '.demo':
            self._run_demo()
        else:
            print(f"未知命令: {command}")
            print("输入 .help 查看所有命令")

    def _process_sql_statement(self, sql: str):
        """处理SQL语句 - 完整集成版本"""
        if not sql.endswith(';'):
            sql += ';'

        print(f"\n执行SQL: {sql}")
        print("=" * 60)

        start_time = time.time()

        try:
            if self.fully_integrated:
                self._process_with_full_integration(sql)
            else:
                self._process_with_partial_integration(sql)

        except Exception as e:
            print(f"❌ 执行失败: {e}")

        end_time = time.time()
        print(f"\n⏱ 总耗时: {(end_time - start_time) * 1000:.2f}ms")
        print("=" * 60)

    def _process_with_full_integration(self, sql: str):
        """完整集成处理：A阶段编译 + B+C阶段执行"""

        # 阶段1: 词法分析
        if self.show_mode in ['token', 'all']:
            print("\n【阶段1: 词法分析】")
            try:
                tokens = self.lexer.tokenize(sql)
                print(format_tokens(tokens))
            except SqlError as e:
                print(f"❌ 词法错误: {e}")
                return

        # 阶段2: 语法分析
        if self.show_mode in ['ast', 'all']:
            print("\n【阶段2: 语法分析】")
            try:
                parser = Parser()
                ast = parser.parse(sql)
                print("✓ 语法分析成功")
                print(format_ast(ast))
            except ParseError as e:
                print(f"❌ 语法错误: {e}")
                return

        # 阶段3: 语义分析（使用A阶段的语义分析器做检查）
        if self.show_mode in ['semantic', 'all']:
            print("\n【阶段3: 语义分析】")
            try:
                # 同步B+C阶段的表信息到A阶段catalog
                self._sync_catalog_to_a_stage()

                parser = Parser()
                ast = parser.parse(sql)
                result = self.semantic_analyzer.analyze(ast)
                print("✓ 语义分析成功")
                print(format_semantic_result(result))
            except (ParseError, SemanticError) as e:
                print(f"❌ 语义错误: {e}")
                return

        # 阶段4: 计划生成（使用A阶段的计划生成器）
        if self.show_mode in ['plan', 'all']:
            print("\n【阶段4: 计划生成】")
            try:
                self._sync_catalog_to_a_stage()
                plan = self.a_stage_planner.plan(sql)
                print("✓ 计划生成成功")
                print(format_execution_plan(plan))
                print(f"\nJSON格式:\n{plan.to_json()}")
            except (PlanError, ParseError, SemanticError) as e:
                print(f"❌ 计划生成错误: {e}")
                return

        # 阶段5: 真正执行（使用B+C阶段的执行器）
        if self.show_mode in ['result', 'all']:
            print("\n【阶段5: 执行结果】")
            try:
                # 生成执行计划
                self._sync_catalog_to_a_stage()
                execution_plan = self.a_stage_planner.plan(sql)

                # 转换为执行器可理解的格式
                plan_dict = self._convert_plan_to_executor_format(execution_plan)

                # 执行
                results = list(self.executor.execute(plan_dict))

                self._display_execution_results(results)

                # 更新B+C阶段的系统目录
                self._update_catalog_after_execution(sql, results)

            except Exception as e:
                print(f"❌ 执行失败: {e}")

    def _process_with_partial_integration(self, sql: str):
        """部分集成处理：仅使用可用组件"""
        if STORAGE_ENGINE_AVAILABLE:
            print("使用简化SQL解析 + 存储引擎执行")
            # 简化的SQL解析和执行逻辑
            self._simple_sql_execution(sql)
        else:
            print("仅展示编译过程（无法真正执行）")
            if SQL_COMPILER_AVAILABLE:
                # 仅展示A阶段编译过程
                self._show_compilation_only(sql)

    def _sync_catalog_to_a_stage(self):
        """同步B+C阶段的表信息到A阶段catalog"""
        if not (SQL_COMPILER_AVAILABLE and STORAGE_ENGINE_AVAILABLE):
            return

        # 获取B+C阶段的所有表
        tables = self.catalog_manager.list_all_tables()

        # 清空A阶段catalog并重新同步
        self.a_stage_catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.a_stage_catalog)
        self.a_stage_planner = Planner(self.a_stage_catalog)

        for table_name in tables:
            columns = self.catalog_manager.get_table_columns(table_name)
            col_defs = []
            for col in columns:
                col_def = {"name": col.column_name, "type": col.column_type}
                if col.max_length:
                    col_def["max_length"] = col.max_length
                col_defs.append(col_def)

            try:
                self.a_stage_catalog.create_table(table_name, col_defs)
            except:
                pass  # 忽略重复创建错误

    def _convert_plan_to_executor_format(self, execution_plan: 'ExecutionPlan') -> dict:
        """将A阶段的ExecutionPlan转换为C阶段Executor可理解的格式"""
        plan_dict = execution_plan.to_dict()

        # 递归转换计划树结构
        def convert_node(node):
            if isinstance(node, dict):
                # 已经是字典格式
                converted = {}
                for key, value in node.items():
                    if key == 'child' and value:
                        converted[key] = convert_node(value)
                    else:
                        converted[key] = value
                return converted
            else:
                # 可能是ExecutionPlan对象
                return node.to_dict() if hasattr(node, 'to_dict') else node

        return convert_node(plan_dict)

    def _display_execution_results(self, results: list):
        """显示执行结果"""
        if not results:
            print("✓ 执行成功，无返回结果")
            return

        print(f"✓ 执行成功，返回 {len(results)} 条结果")

        # 区分状态消息和数据结果
        status_results = [r for r in results if isinstance(r, dict) and 'status' in r]
        data_results = [r for r in results if r not in status_results]

        # 显示状态消息
        for status in status_results:
            if status.get('status') == 'success':
                print(f"   ✓ {status.get('message', '操作成功')}")
                if 'affected_rows' in status:
                    print(f"     影响行数: {status['affected_rows']}")

        # 显示数据结果
        if data_results:
            if len(data_results) <= 20 and all(isinstance(r, dict) for r in data_results):
                # 表格显示
                if data_results:
                    headers = list(data_results[0].keys())
                    table_data = [[row.get(h, '') for h in headers] for row in data_results]
                    print("\n📊 查询结果:")
                    print(tabulate(table_data, headers=headers, tablefmt='grid'))
            else:
                # 列表显示
                print("\n📋 数据结果:")
                for i, result in enumerate(data_results[:10]):
                    print(f"   [{i + 1}] {result}")
                if len(data_results) > 10:
                    print(f"   ... 还有 {len(data_results) - 10} 条结果")

    def _update_catalog_after_execution(self, sql: str, results: list):
        """执行后更新系统目录统计"""
        # ★ 如果是 CREATE TABLE，尝试从 SQL 里取表名并注册到系统目录
        sql_upper = sql.strip().upper()
        if sql_upper.startswith('CREATE TABLE'):
            try:
                # 粗略取表名：CREATE TABLE <name> (
                import re
                m = re.match(r'CREATE\s+TABLE\s+([A-Za-z_]\w*)', sql, re.IGNORECASE)
                if m:
                    tbl = m.group(1)
                    # 从存储引擎拿列定义回填系统目录
                    info = self.storage_engine.get_table_info(tbl)
                    if info is not None:
                        cols = [{"name": c.name, "type": c.type, "max_length": c.max_length} for c in info.schema.columns]
                        self.catalog_manager.register_table(tbl, cols)
            except Exception:
                pass  # 失败不影响主流程

    def _simple_sql_execution(self, sql: str):
        """简化的SQL执行（当A阶段不可用时）"""
        # 基于关键字的简单SQL识别和执行
        sql_upper = sql.upper().strip()

        try:
            if sql_upper.startswith('CREATE TABLE'):
                self._execute_simple_create(sql)
            elif sql_upper.startswith('INSERT INTO'):
                self._execute_simple_insert(sql)
            elif sql_upper.startswith('SELECT'):
                self._execute_simple_select(sql)
            elif sql_upper.startswith('DELETE'):
                self._execute_simple_delete(sql)
            else:
                print(f"不支持的SQL类型: {sql}")
        except Exception as e:
            print(f"执行失败: {e}")

    def _execute_simple_create(self, sql: str):
        """简化的CREATE TABLE执行"""
        # 简单解析CREATE TABLE语句
        import re
        match = re.match(r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)', sql, re.IGNORECASE)
        if not match:
            raise ValueError("CREATE TABLE语法错误")

        table_name = match.group(1)
        columns_str = match.group(2)

        # 解析列定义
        columns = []
        for col_def in columns_str.split(','):
            parts = col_def.strip().split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1]

                col_info = {"name": col_name, "type": col_type}
                if col_type.upper().startswith('VARCHAR'):
                    # 解析VARCHAR长度
                    if '(' in col_type:
                        length_str = col_type.split('(')[1].split(')')[0]
                        col_info["max_length"] = int(length_str)
                    else:
                        col_info["max_length"] = 255

                columns.append(col_info)

        # 执行创建
        self.storage_engine.create_table(table_name, columns)
        self.catalog_manager.register_table(table_name, columns)
        print(f"✓ 表 {table_name} 创建成功")

    def _execute_simple_insert(self, sql: str):
        """简化的INSERT执行"""
        import re
        match = re.match(r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)', sql, re.IGNORECASE)
        if not match:
            raise ValueError("INSERT语法错误")

        table_name = match.group(1)
        values_str = match.group(2)

        # 解析值
        values = []
        for value in values_str.split(','):
            value = value.strip()
            if value.startswith("'") and value.endswith("'"):
                values.append(value[1:-1])  # 字符串
            else:
                try:
                    values.append(int(value))  # 整数
                except ValueError:
                    values.append(value)  # 其他

        # 获取表结构
        columns = self.catalog_manager.get_table_columns(table_name)
        if not columns:
            raise ValueError(f"表不存在: {table_name}")

        # 构造行数据
        row_data = {}
        for i, col in enumerate(columns):
            if i < len(values):
                row_data[col.column_name] = values[i]

        # 执行插入
        success = self.storage_engine.insert_row(table_name, row_data)
        if success:
            self.catalog_manager.update_table_row_count(table_name, 1)
            print(f"✓ 插入成功，影响行数: 1")
        else:
            raise ValueError("插入失败")

    def _execute_simple_select(self, sql: str):
        """简化的SELECT执行"""
        import re

        # 基本SELECT解析
        match = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError("SELECT语法错误")

        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause = match.group(3)

        # 检查表是否存在
        if not self.catalog_manager.table_exists(table_name):
            raise ValueError(f"表不存在: {table_name}")

        # 获取数据
        results = []
        for row in self.storage_engine.seq_scan(table_name):
            # 简化的WHERE处理
            if where_clause:
                if not self._simple_where_eval(row, where_clause):
                    continue

            # 列投影
            if columns_str == '*':
                results.append(row)
            else:
                projected = {}
                for col in columns_str.split(','):
                    col = col.strip()
                    if col in row:
                        projected[col] = row[col]
                results.append(projected)

        # 显示结果
        self._display_execution_results(results)

    def _simple_where_eval(self, row: dict, where_clause: str) -> bool:
        """简化的WHERE条件评估"""
        # 非常简化的实现，仅支持基本比较
        import re

        # 支持格式: column op value
        match = re.match(r'(\w+)\s*([><=!]+)\s*(.+)', where_clause.strip(), re.IGNORECASE)
        if not match:
            return True  # 无法解析就返回True

        column = match.group(1)
        operator = match.group(2)
        value_str = match.group(3).strip()

        if column not in row:
            return False

        row_value = row[column]

        # 解析比较值
        if value_str.startswith("'") and value_str.endswith("'"):
            compare_value = value_str[1:-1]
        else:
            try:
                compare_value = int(value_str)
            except ValueError:
                compare_value = value_str

        # 执行比较
        try:
            if operator == '=':
                return row_value == compare_value
            elif operator == '>':
                return row_value > compare_value
            elif operator == '<':
                return row_value < compare_value
            elif operator == '>=':
                return row_value >= compare_value
            elif operator == '<=':
                return row_value <= compare_value
            elif operator in ['!=', '<>']:
                return row_value != compare_value
        except TypeError:
            return False

        return False

    def _execute_simple_delete(self, sql: str):
        """简化的DELETE执行"""
        import re

        match = re.match(r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?', sql, re.IGNORECASE)
        if not match:
            raise ValueError("DELETE语法错误")

        table_name = match.group(1)
        where_clause = match.group(2)

        # 检查表是否存在
        if not self.catalog_manager.table_exists(table_name):
            raise ValueError(f"表不存在: {table_name}")

        # 构造删除条件
        if where_clause:
            predicate = lambda row: self._simple_where_eval(row, where_clause)
        else:
            predicate = lambda row: True

        # 执行删除
        deleted_count = self.storage_engine.delete_where(table_name, predicate)

        if deleted_count > 0:
            self.catalog_manager.update_table_row_count(table_name, -deleted_count)

        print(f"✓ 删除成功，影响行数: {deleted_count}")

    def _show_compilation_only(self, sql: str):
        """仅显示编译过程（当存储引擎不可用时）"""
        print("⚠ 仅显示编译过程，无法真正执行")

        try:
            # 词法分析
            print("\n【词法分析】")
            tokens = self.lexer.tokenize(sql)
            print(format_tokens(tokens))

            # 语法分析
            print("\n【语法分析】")
            parser = Parser()
            ast = parser.parse(sql)
            print(format_ast(ast))

            # 语义分析
            print("\n【语义分析】")
            result = self.semantic_analyzer.analyze(ast)
            print(format_semantic_result(result))

            # 计划生成
            print("\n【计划生成】")
            plan = self.a_stage_planner.plan(sql)
            print(format_execution_plan(plan))

        except Exception as e:
            print(f"编译失败: {e}")

    def _set_show_mode(self, mode: str):
        """设置显示模式"""
        valid_modes = ['result', 'token', 'ast', 'semantic', 'plan', 'all']
        if mode in valid_modes:
            self.show_mode = mode
            print(f"显示模式已设置为: {mode}")
        else:
            print(f"无效模式: {mode}")
            print(f"可用模式: {', '.join(valid_modes)}")

    def _demo_four_views(self):
        """四视图演示"""
        if not self.fully_integrated:
            print("四视图演示需要完整集成")
            return

        print("=== 四视图演示 ===")
        print("请输入SQL语句:")

        sql = input("SQL> ").strip()
        if not sql:
            print("未输入SQL语句")
            return

        # 临时切换到all模式
        old_mode = self.show_mode
        self.show_mode = 'all'

        self._process_sql_statement(sql)

        # 恢复原模式
        self.show_mode = old_mode

    def _run_demo(self):
        """运行完整演示"""
        if not STORAGE_ENGINE_AVAILABLE:
            print("演示需要存储引擎支持")
            return

        print("=== MiniDB 完整功能演示 ===")

        demo_sqls = [
            "CREATE TABLE users(id INT, name VARCHAR(50), age INT);",
            "INSERT INTO users VALUES(1, 'Alice', 25);",
            "INSERT INTO users VALUES(2, 'Bob', 30);",
            "INSERT INTO users VALUES(3, 'Charlie', 22);",
            "SELECT * FROM users;",
            "SELECT name, age FROM users WHERE age > 23;",
            "DELETE FROM users WHERE id = 2;",
            "SELECT * FROM users;"
        ]

        old_mode = self.show_mode
        self.show_mode = 'result'

        for i, sql in enumerate(demo_sqls, 1):
            print(f"\n[演示 {i}/{len(demo_sqls)}] {sql}")
            input("按回车继续...")
            self._process_sql_statement(sql)

        self.show_mode = old_mode
        print("\n🎉 演示完成!")

    def _show_tables(self):
        """显示所有表"""
        if STORAGE_ENGINE_AVAILABLE:
            tables = self.catalog_manager.list_all_tables()

            if not tables:
                print("数据库中暂无用户表")
                return

            print(f"\n📋 用户表列表 ({len(tables)}个):")

            table_info = []
            for table_name in tables:
                meta = self.catalog_manager.get_table_metadata(table_name)
                cols = self.catalog_manager.get_table_columns(table_name)
                idxs = self.catalog_manager.get_table_indexes(table_name)

                table_info.append([
                    table_name,
                    len(cols),
                    meta.row_count if meta else 0,
                    len(idxs),
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(meta.created_time)) if meta else "N/A"
                ])

            headers = ['表名', '列数', '行数', '索引数', '创建时间']
            print(tabulate(table_info, headers=headers, tablefmt='grid'))
        else:
            print("存储引擎不可用，无法显示表信息")

    def _show_schema(self, table_name: str):
        """显示表结构"""
        if not STORAGE_ENGINE_AVAILABLE:
            print("存储引擎不可用，无法显示表结构")
            return

        schema = self.catalog_manager.get_schema_info(table_name)

        if not schema:
            print(f"表不存在: {table_name}")
            return

        print(f"\n📊 表结构: {table_name}")
        print(f"表ID: {schema['table_id']}")
        print(f"行数: {schema['row_count']}")
        print(f"创建时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(schema['created_time']))}")

        # 显示列信息
        print(f"\n📝 列信息 ({len(schema['columns'])}列):")
        col_data = []
        for col in schema['columns']:
            col_type = col['type']
            if col['max_length']:
                col_type += f"({col['max_length']})"

            col_data.append([
                col['position'],
                col['name'],
                col_type
            ])

        print(tabulate(col_data, headers=['位置', '列名', '类型'], tablefmt='grid'))

        # 显示索引信息
        if schema['indexes']:
            print(f"\n🔍 索引信息 ({len(schema['indexes'])}个):")
            idx_data = [[idx['name'], idx['column'], idx['type']] for idx in schema['indexes']]
            print(tabulate(idx_data, headers=['索引名', '列名', '类型'], tablefmt='grid'))

    def _show_stats(self):
        """显示系统统计"""
        print("\n📈 系统统计信息:")

        if STORAGE_ENGINE_AVAILABLE:
            # 数据库统计
            db_stats = self.catalog_manager.get_database_stats()
            storage_stats = self.storage_engine.get_stats()

            print(f"\n📊 数据库统计:")
            print(f"   用户表数: {db_stats['total_tables']}")
            print(f"   总行数: {db_stats['total_rows']}")
            print(f"   总索引数: {db_stats['total_indexes']}")
            print(f"   系统表数: {db_stats['system_tables']}")

            print(f"\n💾 存储引擎统计:")
            print(f"   数据目录: {storage_stats['data_directory']}")

            buffer_stats = storage_stats['buffer_pool']
            print(f"\n🔧 缓冲池统计:")
            print(f"   策略: {buffer_stats['policy']}")
            print(f"   容量: {buffer_stats['capacity']} 页")
            print(f"   已缓存: {buffer_stats['cached_pages']} 页")
            print(f"   脏页数: {buffer_stats['dirty_pages']} 页")
            print(f"   命中率: {buffer_stats['hit_ratio_pct']}%")
            print(f"   总请求: {buffer_stats['total_requests']} 次")
            print(f"   淘汰次数: {buffer_stats['evictions']} 次")
        else:
            print("   存储引擎不可用")

        # 组件状态
        print(f"\n🔧 组件状态:")
        print(f"   A阶段SQL编译器: {'✓' if SQL_COMPILER_AVAILABLE else '❌'}")
        print(f"   B+C阶段存储引擎: {'✓' if STORAGE_ENGINE_AVAILABLE else '❌'}")
        print(f"   完整集成: {'✓' if self.fully_integrated else '❌'}")
        print(f"   当前显示模式: {self.show_mode}")

    def _show_detailed_help(self):
        """显示详细帮助"""
        print("""
=== MiniDB 完整集成版帮助 ===

   系统概述:
   这是一个完整的SQL数据库系统，集成了：
   - A阶段: SQL编译器 (词法/语法/语义/计划)
   - B阶段: 存储引擎 (页面/文件/缓冲/持久化)  
   - C阶段: 执行引擎 (算子/目录)

   系统命令:
   .help              - 显示此帮助
   .exit              - 退出系统
   .tables            - 列出所有表
   .schema <table>    - 显示表结构
   .stats             - 显示系统统计

    调试命令:
   .show <mode>       - 设置显示模式
   .fourview          - 四视图演示
   .demo              - 完整功能演示

    显示模式:
   result   - 显示执行结果 (默认)
   token    - 显示词法分析
   ast      - 显示语法分析
   semantic - 显示语义分析  
   plan     - 显示执行计划
   all      - 显示所有阶段

    支持的SQL:
   CREATE TABLE table_name(col1 INT, col2 VARCHAR(n));
   INSERT INTO table_name VALUES(val1, val2);
   SELECT col1,col2 FROM table_name WHERE condition;
   DELETE FROM table_name WHERE condition;

    使用建议:
   1. 先用 .demo 查看完整功能演示
   2. 用 .show all 切换到四视图模式
   3. 输入SQL查看完整编译和执行过程
   4. 用 .tables 和 .schema 查看数据库状态
""")

    def _cleanup(self):
        """清理资源"""
        if STORAGE_ENGINE_AVAILABLE and hasattr(self, 'storage_engine'):
            print("正在保存数据...")
            self.storage_engine.close()
            print("数据已保存")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="MiniDB - 完整集成版 SQL 数据库系统",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--data-dir', '-d', default='minidb_data',
                        help='数据目录 (默认: minidb_data)')
    parser.add_argument('--show', choices=['result', 'token', 'ast', 'semantic', 'plan', 'all'],
                        default='result', help='显示模式')
    parser.add_argument('--version', action='version',
                        version='MiniDB 完整集成版 v1.0 (A+B+C阶段)')

    args = parser.parse_args()

    try:
        cli = IntegratedMiniDBCLI(args.data_dir)
        cli.show_mode = args.show
        cli.run_interactive()

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"启动失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()