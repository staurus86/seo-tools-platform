"""Preflight checks for site_audit_pro changes."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run_step(cmd: list[str], cwd: Path) -> int:
    print(f"[preflight] running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        print(f"[preflight] failed: {' '.join(cmd)}")
    return result.returncode


def main() -> int:
    root = Path(__file__).resolve().parents[1]

    commands = [
        [
            sys.executable,
            "-m",
            "py_compile",
            "app/tools/site_pro/adapter.py",
            "app/tools/site_pro/schema.py",
            "app/reports/xlsx_generator.py",
            "app/reports/docx_generator.py",
            "tests/test_site_pro_adapter.py",
            "tests/test_site_pro_baseline_diff.py",
            "tests/test_site_pro_xlsx_layout.py",
            "tests/test_encoding_guard.py",
        ],
        [
            sys.executable,
            "-m",
            "unittest",
            "tests/test_site_pro_adapter.py",
            "tests/test_site_pro_baseline_diff.py",
            "tests/test_site_pro_xlsx_layout.py",
            "tests/test_encoding_guard.py",
        ],
        [
            sys.executable,
            "scripts/encoding_guard.py",
            "check",
            "--root",
            "app",
            "--ext",
            ".py",
            ".html",
            ".js",
        ],
    ]

    for cmd in commands:
        code = run_step(cmd, root)
        if code != 0:
            return code

    print("[preflight] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
