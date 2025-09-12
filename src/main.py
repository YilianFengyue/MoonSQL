# 文件路径: MoonSQL/src/main.py
"""
MiniDB 主启动文件
提供项目的统一入口点
"""

import sys
import os
from pathlib import Path

# 确保可以导入项目模块
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


def main():
    """主入口函数"""
    print("=== MiniDB Database System ===")
    print("Version: 2.0.0 - 完整集成版")
    print("Project: Mini-SQL Implementation (A+B+C阶段)")
    print()

    # 检查命令行参数
    if len(sys.argv) > 1:
        # 有参数，传递给完整集成CLI处理
        from cli.minidb_cli import main as cli_main
        cli_main()
    else:
        # 无参数，启动完整集成交互模式
        print("启动完整集成交互模式...")
        print("支持真正的SQL执行和数据持久化")
        print()

        # 启动完整集成CLI
        from cli.minidb_cli import IntegratedMiniDBCLI
        cli = IntegratedMiniDBCLI()
        cli.run_interactive()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nGoodbye!")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)