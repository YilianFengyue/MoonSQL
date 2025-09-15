# 文件路径: MoonSQL/src/tests/test_s6s7_integration.py

"""
S6+S7完整集成测试
测试聚合函数+分组+排序+分页的端到端功能
"""

import sys
import os
from pathlib import Path

# 添加src目录到路径
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from sql.lexer import Lexer
from sql.parser import Parser
from sql.planner import Planner
from sql.semantic import Catalog
from engine.executor import Executor
from storage.storage_engine import StorageEngine
from engine.catalog_mgr import CatalogManager


class S6S7IntegrationTester:
    """S6+S7集成测试器"""

    def __init__(self, data_dir: str = "test_s6s7_integration"):
        self.data_dir = data_dir
        self.setup_environment()

    def setup_environment(self):
        """设置测试环境"""
        print("=== S6+S7集成测试环境初始化 ===")

        # 初始化存储和执行引擎
        self.storage_engine = StorageEngine(self.data_dir, buffer_capacity=16)
        self.catalog_manager = CatalogManager(self.storage_engine)
        self.executor = Executor(self.storage_engine, self.catalog_manager)

        # 初始化编译器组件
        self.lexer = Lexer()
        self.parser = Parser()

        # 创建语义catalog（与存储catalog同步）
        self.semantic_catalog = Catalog()
        self.planner = Planner(self.semantic_catalog)

        print("✓ 测试环境初始化完成")

    def create_test_data(self):
        """创建测试数据"""
        print("\n=== 创建测试数据 ===")

        # 1. 创建employees表
        create_sql = """
                     CREATE TABLE employees \
                     ( \
                         id     INT, \
                         name   VARCHAR(50), \
                         dept   VARCHAR(20), \
                         salary INT, \
                         age    INT
                     ); \
                     """

        self._execute_sql(create_sql)
        print("✓ 创建employees表")

        # 2. 插入测试数据
        test_employees = [
            (1, 'Alice', 'Engineering', 75000, 25),
            (2, 'Bob', 'Sales', 65000, 30),
            (3, 'Charlie', 'Engineering', 80000, 28),
            (4, 'Diana', 'Sales', 70000, 26),
            (5, 'Eve', 'Engineering', 85000, 30),
            (6, 'Frank', 'Marketing', 60000, 24),
            (7, 'Grace', 'Engineering', 90000, 32),
            (8, 'Henry', 'Sales', 68000, 29),
            (9, 'Ivy', 'Marketing', 62000, 27),
            (10, 'Jack', 'Engineering', 82000, 31)
        ]

        for emp in test_employees:
            insert_sql = f"INSERT INTO employees VALUES({emp[0]}, '{emp[1]}', '{emp[2]}', {emp[3]}, {emp[4]});"
            self._execute_sql(insert_sql)

        print(f"✓ 插入{len(test_employees)}条员工数据")

        # 同步到语义catalog
        self._sync_semantic_catalog()
        print("✓ 同步语义catalog")

    def _sync_semantic_catalog(self):
        """同步存储catalog到语义catalog"""
        tables = self.catalog_manager.list_all_tables()

        for table_name in tables:
            columns = self.catalog_manager.get_table_columns(table_name)
            col_defs = []
            for col in columns:
                col_def = {"name": col.column_name, "type": col.column_type}
                if col.max_length:
                    col_def["max_length"] = col.max_length
                col_defs.append(col_def)

            try:
                self.semantic_catalog.create_table(table_name, col_defs)
            except:
                pass  # 忽略重复创建

    def _execute_sql(self, sql: str, show_result: bool = False):
        """执行SQL并返回结果"""
        try:
            # 编译
            tokens = self.lexer.tokenize(sql)
            ast = self.parser.parse(sql)
            plan = self.planner.plan(sql)

            # 执行
            results = list(self.executor.execute(plan.to_dict()))

            if show_result:
                print(f"执行SQL: {sql.strip()}")
                for result in results:
                    print(f"   {result}")
                print()

            return results

        except Exception as e:
            print(f"❌ SQL执行失败: {sql.strip()}")
            print(f"   错误: {e}")
            raise

    def test_s6_aggregation(self):
        """测试S6聚合功能"""
        print("\n=== S6聚合功能测试 ===")

        test_cases = [
            # 基础聚合
            ("SELECT COUNT(*) as total FROM employees;", "全局COUNT"),
            ("SELECT AVG(salary) as avg_sal FROM employees;", "全局AVG"),
            ("SELECT MIN(age) as min_age, MAX(age) as max_age FROM employees;", "MIN/MAX"),
            ("SELECT SUM(salary) as total_sal FROM employees;", "SUM"),

            # 分组聚合
            ("SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept;", "部门计数"),
            ("SELECT dept, AVG(salary) as avg_sal FROM employees GROUP BY dept;", "部门平均薪水"),
            ("SELECT age, COUNT(*) as cnt FROM employees GROUP BY age;", "年龄分布"),

            # HAVING过滤
            ("SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept HAVING COUNT(*) >= 3;", "HAVING计数过滤"),
            ("SELECT dept, AVG(salary) as avg_sal FROM employees GROUP BY dept HAVING AVG(salary) > 70000;",
             "HAVING平均值过滤"),
        ]

        for i, (sql, desc) in enumerate(test_cases, 1):
            print(f"\n[S6-{i}] {desc}")
            try:
                results = self._execute_sql(sql, show_result=True)
                print("✓ 测试通过")
            except Exception as e:
                print(f"❌ 测试失败: {e}")

    def test_s7_sorting_paging(self):
        """测试S7排序分页功能"""
        print("\n=== S7排序分页功能测试 ===")

        test_cases = [
            # 排序测试
            ("SELECT name, salary FROM employees ORDER BY salary DESC;", "薪水降序"),
            ("SELECT name, dept, age FROM employees ORDER BY dept ASC, age DESC;", "多列排序"),
            ("SELECT * FROM employees ORDER BY name ASC;", "姓名升序"),

            # 分页测试
            ("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3;", "前3高薪"),
            ("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 2, 3;", "第3-5高薪"),
            ("SELECT * FROM employees ORDER BY age ASC LIMIT 5 OFFSET 2;", "跳过2人取5人"),

            # 组合测试
            ("SELECT name, age FROM employees WHERE age > 26 ORDER BY age DESC LIMIT 4;", "条件+排序+分页"),
        ]

        for i, (sql, desc) in enumerate(test_cases, 1):
            print(f"\n[S7-{i}] {desc}")
            try:
                results = self._execute_sql(sql, show_result=True)
                print("✓ 测试通过")
            except Exception as e:
                print(f"❌ 测试失败: {e}")

    def test_complete_pipeline(self):
        """测试完整SQL管线"""
        print("\n=== 完整SQL管线测试 ===")

        complex_cases = [
            # 完整管线1
            ("""
             SELECT dept, AVG(salary) as avg_sal, COUNT(*) as cnt
             FROM employees
             WHERE age > 25
             GROUP BY dept
             HAVING COUNT(*) >= 2
             ORDER BY avg_sal DESC LIMIT 2;
             """, "完整管线：WHERE+GROUP BY+HAVING+ORDER BY+LIMIT"),

            # 完整管线2
            ("""
             SELECT dept, MIN(age) as min_age, MAX(salary) as max_sal
             FROM employees
             WHERE salary > 60000
             GROUP BY dept
             HAVING MAX(salary) > 75000
             ORDER BY min_age ASC;
             """, "多聚合函数+条件过滤"),

            # 带DISTINCT
            ("""
             SELECT DISTINCT dept
             FROM employees
             WHERE age < 30
             ORDER BY dept ASC;
             """, "DISTINCT+条件+排序"),
        ]

        for i, (sql, desc) in enumerate(complex_cases, 1):
            print(f"\n[完整-{i}] {desc}")
            try:
                # 去除多余空白
                clean_sql = ' '.join(sql.split())
                results = self._execute_sql(clean_sql, show_result=True)
                print("✓ 完整管线测试通过")
            except Exception as e:
                print(f"❌ 完整管线测试失败: {e}")

    def test_edge_cases(self):
        """测试边界情况"""
        print("\n=== 边界情况测试 ===")

        edge_cases = [
            # NULL值处理
            ("SELECT COUNT(name), COUNT(*) FROM employees;", "COUNT与COUNT(*)差异"),

            # 空结果集
            ("SELECT dept, COUNT(*) FROM employees WHERE age > 100 GROUP BY dept;", "空结果集聚合"),

            # 单行结果
            ("SELECT MAX(salary) as highest FROM employees;", "单行聚合结果"),

            # 大LIMIT
            ("SELECT * FROM employees ORDER BY id LIMIT 100;", "超大LIMIT"),

            # 零OFFSET
            ("SELECT name FROM employees ORDER BY name LIMIT 3 OFFSET 0;", "零偏移"),
        ]

        for i, (sql, desc) in enumerate(edge_cases, 1):
            print(f"\n[边界-{i}] {desc}")
            try:
                results = self._execute_sql(sql, show_result=True)
                print("✓ 边界测试通过")
            except Exception as e:
                print(f"❌ 边界测试失败: {e}")

    def run_all_tests(self):
        """运行所有测试"""
        try:
            self.create_test_data()
            self.test_s6_aggregation()
            self.test_s7_sorting_paging()
            self.test_complete_pipeline()
            self.test_edge_cases()

            print("\n" + "=" * 60)
            print("🎉 S6+S7集成测试全部完成！")

            # 显示统计信息
            stats = self.storage_engine.get_stats()
            print(f"\n📊 执行统计:")
            print(f"   缓冲池命中率: {stats['buffer_pool']['hit_ratio_pct']}%")
            print(f"   总请求数: {stats['buffer_pool']['total_requests']}")

        except Exception as e:
            print(f"\n❌ 测试过程中发生错误: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """清理测试环境"""
        if hasattr(self, 'storage_engine'):
            self.storage_engine.close()
        print("✓ 测试环境已清理")


def main():
    """主函数"""
    tester = S6S7IntegrationTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()