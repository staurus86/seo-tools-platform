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

    def test_frequency_changes_cluster_priority(self):
        result = run_keyword_clusterizer(
            keyword_rows=[
                {"keyword": "купить iphone 16", "frequency": 500},
                {"keyword": "iphone 16 цена", "frequency": 320},
                {"keyword": "ремонт холодильника", "frequency": 40},
                {"keyword": "ремонт холодильников срочно", "frequency": 25},
            ],
            method="jaccard",
            similarity_threshold=0.34,
            min_cluster_size=2,
            clustering_mode="balanced",
        )

        summary = (result.get("results") or {}).get("summary") or {}
        clusters = (result.get("results") or {}).get("clusters") or []
        self.assertAlmostEqual(float(summary.get("input_demand_total", 0.0)), 885.0, delta=0.001)
        self.assertTrue(len(clusters) >= 2)
        top_cluster = clusters[0]
        self.assertGreater(float(top_cluster.get("demand_total", 0.0)), 700.0)
        self.assertGreater(float(summary.get("top_cluster_demand_share_pct", 0.0)), 70.0)

    def test_zero_frequency_is_preserved(self):
        result = run_keyword_clusterizer(
            keyword_rows=[
                {"keyword": "подготовка к триатлону питание", "frequency": 0},
                {"keyword": "подготовка к триатлону онлайн", "frequency": 0},
            ],
            method="jaccard",
            similarity_threshold=0.3,
            min_cluster_size=1,
            clustering_mode="balanced",
        )
        summary = (result.get("results") or {}).get("summary") or {}
        self.assertAlmostEqual(float(summary.get("input_demand_total", -1.0)), 0.0, delta=0.001)


if __name__ == "__main__":
    unittest.main()
