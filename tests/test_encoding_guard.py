import subprocess
import sys
import unittest
from pathlib import Path


class EncodingGuardTests(unittest.TestCase):
    def test_no_mojibake_tokens_in_app(self):
        root = Path(__file__).resolve().parents[1]
        cmd = [
            sys.executable,
            str(root / "scripts" / "encoding_guard.py"),
            "check",
            "--root",
            str(root / "app"),
            "--ext",
            ".py",
            ".html",
            ".js",
        ]
        run = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root))
        self.assertEqual(
            run.returncode,
            0,
            msg=f"encoding_guard failed\nstdout:\n{run.stdout}\nstderr:\n{run.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
