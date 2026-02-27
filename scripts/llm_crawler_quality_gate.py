#!/usr/bin/env python
"""Run LLM crawler benchmark quality gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tools.llmCrawler.quality import run_quality_gate_from_file  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LLM crawler quality benchmark gate")
    parser.add_argument(
        "--cases",
        default="",
        help="Path to benchmark cases JSON file (default: tests/fixtures/llm_crawler_benchmark_cases.json)",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON payload")
    args = parser.parse_args()

    payload = run_quality_gate_from_file(args.cases or None)
    gate = payload.get("gate") or {}
    benchmark = payload.get("benchmark") or {}

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("LLM Crawler Quality Gate")
        print(f"- Status: {gate.get('status')}")
        print(f"- Passed: {gate.get('passed')}/{gate.get('total')}")
        print(f"- Cases: {benchmark.get('total_cases')}")
        metrics = benchmark.get("metrics") or {}
        for key in (
            "page_type_accuracy",
            "segmentation_pass_rate",
            "retrieval_pass_rate",
            "schema_recall_rate",
            "citation_pass_rate",
            "gate_pass_rate",
        ):
            print(f"  - {key}: {metrics.get(key)}")

    return 0 if str(gate.get("status") or "") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
