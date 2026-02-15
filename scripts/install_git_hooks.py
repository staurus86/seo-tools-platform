"""Configure repository git hooks to enforce encoding checks before commit."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    hook_file = root / ".githooks" / "pre-commit.cmd"
    if not hook_file.exists():
        print(f"[hooks] missing hook file: {hook_file}")
        return 1

    cmd = ["git", "config", "core.hooksPath", ".githooks"]
    result = subprocess.run(cmd, cwd=str(root))
    if result.returncode != 0:
        return result.returncode

    print("[hooks] core.hooksPath configured to .githooks")
    print("[hooks] pre-commit encoding guard is now active (.githooks/pre-commit.cmd)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
