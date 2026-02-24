import unittest

from app.tools.clusterizer import run_keyword_clusterizer


class ClusterizerServiceTests(unittest.TestCase):
    def test_groups_keywords_by_similarity(self):
        result = run_keyword_clusterizer(
            keywords=[
                "купить iphone 16",
                "iphone 16 цена",
                "samsung galaxy s25",
                "galaxy s25 купить",
                "ремонт холодильника",
            ],
            method="jaccard",
            similarity_threshold=0.35,
            min_cluster_size=2,
        )

        self.assertEqual(result.get("task_type"), "clusterizer")
        summary = (result.get("results") or {}).get("summary") or {}
        self.assertEqual(summary.get("keywords_input_total"), 5)
        self.assertEqual(summary.get("keywords_unique_total"), 5)
        self.assertEqual(summary.get("clusters_total"), 3)
        self.assertEqual(summary.get("multi_keyword_clusters"), 2)
        self.assertEqual(summary.get("singleton_clusters"), 1)

    def test_reports_duplicates_removed(self):
        result = run_keyword_clusterizer(
            keywords=[
                "SEO audit",
                "seo audit",
                "seo audit чеклист",
            ],
            method="jaccard",
            similarity_threshold=0.3,
            min_cluster_size=2,
        )

        summary = (result.get("results") or {}).get("summary") or {}
        self.assertEqual(summary.get("keywords_input_total"), 3)
        self.assertEqual(summary.get("keywords_unique_total"), 2)
        self.assertEqual(summary.get("duplicates_removed"), 1)
        self.assertEqual(summary.get("clusters_total"), 1)

    def test_mode_changes_cluster_granularity(self):
        keywords = [
            "купить айфон 16",
            "iphone 16 купить",
            "айфон 16 цена",
            "чехол iphone 16",
            "чехлы для iphone 16",
        ]

        strict_result = run_keyword_clusterizer(
            keywords=keywords,
            method="jaccard",
            similarity_threshold=0.36,
            min_cluster_size=2,
            clustering_mode="strict",
        )
        broad_result = run_keyword_clusterizer(
            keywords=keywords,
            method="jaccard",
            similarity_threshold=0.36,
            min_cluster_size=2,
            clustering_mode="broad",
        )

        strict_summary = (strict_result.get("results") or {}).get("summary") or {}
        broad_summary = (broad_result.get("results") or {}).get("summary") or {}
        self.assertGreaterEqual(
            int(strict_summary.get("clusters_total", 0)),
            int(broad_summary.get("clusters_total", 0)),
        )
        strict_settings = (strict_result.get("results") or {}).get("settings") or {}
        broad_settings = (broad_result.get("results") or {}).get("settings") or {}
        self.assertGreater(
            float(strict_settings.get("similarity_threshold", 0.0)),
            float(broad_settings.get("similarity_threshold", 0.0)),
        )


if __name__ == "__main__":
    unittest.main()
