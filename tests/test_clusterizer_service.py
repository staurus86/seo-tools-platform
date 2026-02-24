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


if __name__ == "__main__":
    unittest.main()

