import json
import unittest
from pathlib import Path

from tests.test_site_pro_adapter import build_mock_public_result


class SiteProBaselineDiffTests(unittest.TestCase):
    def test_baseline_metrics_with_tolerance(self):
        root = Path(__file__).resolve().parents[1]
        baseline_path = root / "tests" / "fixtures" / "site_pro_baseline.json"
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

        current = build_mock_public_result()
        pipeline = current.get("pipeline", {})

        current_snapshot = {
            "summary": current.get("summary", {}),
            "pagerank": pipeline.get("pagerank", []),
            "duplicates": pipeline.get("duplicates", {}),
            "anchor_text_quality": pipeline.get("anchor_text_quality", {}),
            "topic_clusters": pipeline.get("topic_clusters", []),
            "pages": {
                row["url"]: {
                    "health_score": row.get("health_score"),
                    "topic_label": row.get("topic_label"),
                    "duplicate_title_count": row.get("duplicate_title_count"),
                    "duplicate_description_count": row.get("duplicate_description_count"),
                    "ai_markers_count": row.get("ai_markers_count"),
                    "link_quality_score": row.get("link_quality_score"),
                    "weak_anchor_ratio": row.get("weak_anchor_ratio"),
                }
                for row in current.get("pages", [])
            },
        }

        self.assertEqual(current_snapshot["summary"]["total_pages"], baseline["summary"]["total_pages"])
        self.assertEqual(current_snapshot["summary"]["issues_total"], baseline["summary"]["issues_total"])
        self.assertAlmostEqual(
            float(current_snapshot["summary"]["score"]),
            float(baseline["summary"]["score"]),
            delta=0.6,
        )

        self.assertEqual(len(current_snapshot["duplicates"]["title_groups"]), len(baseline["duplicates"]["title_groups"]))
        self.assertEqual(
            len(current_snapshot["duplicates"]["description_groups"]),
            len(baseline["duplicates"]["description_groups"]),
        )

        self.assertEqual(len(current_snapshot["pagerank"]), len(baseline["pagerank"]))
        for idx, row in enumerate(current_snapshot["pagerank"]):
            self.assertEqual(row["url"], baseline["pagerank"][idx]["url"])
            self.assertAlmostEqual(float(row["score"]), float(baseline["pagerank"][idx]["score"]), delta=0.8)

        self.assertAlmostEqual(
            float(current_snapshot["anchor_text_quality"]["average_weak_anchor_ratio"]),
            float(baseline["anchor_text_quality"]["average_weak_anchor_ratio"]),
            delta=0.01,
        )
        self.assertEqual(
            int(current_snapshot["anchor_text_quality"]["pages_with_weak_anchors"]),
            int(baseline["anchor_text_quality"]["pages_with_weak_anchors"]),
        )

        self.assertEqual(len(current_snapshot["topic_clusters"]), len(baseline["topic_clusters"]))

        self.assertEqual(set(current_snapshot["pages"].keys()), set(baseline["pages"].keys()))
        for url in current_snapshot["pages"]:
            current_page = current_snapshot["pages"][url]
            baseline_page = baseline["pages"][url]
            self.assertEqual(current_page["topic_label"], baseline_page["topic_label"])
            self.assertEqual(current_page["duplicate_title_count"], baseline_page["duplicate_title_count"])
            self.assertEqual(current_page["duplicate_description_count"], baseline_page["duplicate_description_count"])
            self.assertEqual(current_page["ai_markers_count"], baseline_page["ai_markers_count"])
            self.assertAlmostEqual(
                float(current_page["health_score"]), float(baseline_page["health_score"]), delta=0.6
            )
            self.assertAlmostEqual(
                float(current_page["link_quality_score"]), float(baseline_page["link_quality_score"]), delta=1.0
            )
            self.assertAlmostEqual(
                float(current_page["weak_anchor_ratio"]), float(baseline_page["weak_anchor_ratio"]), delta=0.01
            )


if __name__ == "__main__":
    unittest.main()
