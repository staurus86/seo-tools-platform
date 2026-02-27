import subprocess
import sys
import unittest
from pathlib import Path


class LlmCrawlerQualityGateScriptTests(unittest.TestCase):
    def test_quality_gate_script_runs(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "llm_crawler_quality_gate.py"
        proc = subprocess.run(
            [sys.executable, str(script), "--json"],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + "\n" + proc.stderr)
        self.assertIn('"gate"', proc.stdout)
        self.assertIn('"benchmark"', proc.stdout)


if __name__ == "__main__":
    unittest.main()
