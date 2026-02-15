import subprocess
import sys
import unittest
from pathlib import Path


class EncodingGuardTests(unittest.TestCase):
    def test_no_mojibake_tokens_in_platform(self):
        root = Path(__file__).resolve().parents[1]
        scan_roots = [
            "app",
            "scripts",
            "tests",
            "Py scripts",
        ]
        exts = [".py", ".html", ".js", ".md", ".txt", ".json", ".yml", ".yaml"]

        failures = []
        for rel_root in scan_roots:
            cmd = [
                sys.executable,
                str(root / "scripts" / "encoding_guard.py"),
                "check",
                "--root",
                str(root / rel_root),
                "--ext",
                *exts,
            ]
            run = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root))
            if run.returncode != 0:
                failures.append(
                    f"[{rel_root}] encoding_guard failed\nstdout:\n{run.stdout}\nstderr:\n{run.stderr}"
                )

        self.assertFalse(failures, msg="\n\n".join(failures))


if __name__ == "__main__":
    unittest.main()
