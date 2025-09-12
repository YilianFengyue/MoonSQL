# 文件路径: MoonSQL/main.py
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
    print("Version: 1.0.0")
    print("Project: Mini-SQL Implementation")
    print()

    # 检查命令行参数
    if len(sys.argv) > 1:
        # 有参数，传递给CLI处理
        from cli.minidb_cli import main as cli_main
        cli_main()
    else:
        # 无参数，显示帮助并启动交互模式
        print("Usage:")
        print(f"  python {sys.argv[0]} --interactive         # Interactive mode")
        print(f"  python {sys.argv[0]} --show=token 'SQL'    # Show tokens")
        print(f"  python {sys.argv[0]} --file script.sql     # Execute file")
        print()
        print("Starting interactive mode...")
        print()

        # 启动交互模式
        from cli.minidb_cli import MiniDBCLI
        cli = MiniDBCLI()
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