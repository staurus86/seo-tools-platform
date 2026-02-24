import unittest

from app.tools.link_profile.service_v1 import run_link_profile_audit


class LinkProfileServiceTests(unittest.TestCase):
    def test_lost_flag_is_counted_without_lost_status(self):
        csv_data = (
            "source_url,target_url,follow,dr,traffic,lost,referring page http code\n"
            "https://d1.com/p,https://our.com/,dofollow,40,100,1,200\n"
            "https://d2.com/p,https://comp.com/,dofollow,40,100,0,200\n"
        )
        result = run_link_profile_audit(
            our_domain="our.com",
            backlink_files=[("backlinks.csv", csv_data.encode("utf-8"))],
        )

        summary = result["results"]["summary"]
        self.assertEqual(summary.get("lost_links_pct"), 50.0)
        lost_mix = result["results"]["tables"].get("lost_status_mix", []) or []
        self.assertTrue(any(row.get("lost_status") == "lost" and int(row.get("count", 0)) == 1 for row in lost_mix))

    def test_ready_buy_keeps_domains_with_zero_lost_pct(self):
        csv_data = (
            "source_url,target_url,follow,dr,traffic,lost\n"
            "https://foo.com/p,https://comp1.com/,dofollow,50,200,0\n"
        )
        result = run_link_profile_audit(
            our_domain="our.com",
            backlink_files=[("backlinks.csv", csv_data.encode("utf-8"))],
        )

        ready_buy_rows = result["results"]["tables"].get("ready_buy_domains", []) or []
        self.assertTrue(any(str(row.get("domain") or "") == "foo.com" for row in ready_buy_rows))

    def test_redirect_301_detects_http_code_when_chain_is_missing(self):
        csv_data = (
            "source_url,target_url,follow,dr,traffic,referring page http code\n"
            "https://r1.com/p,https://our.com/,dofollow,30,10,301\n"
        )
        result = run_link_profile_audit(
            our_domain="our.com",
            backlink_files=[("backlinks.csv", csv_data.encode("utf-8"))],
        )

        summary = result["results"]["summary"]
        self.assertEqual(summary.get("donors_with_301"), 1)
        redirect_rows = result["results"]["tables"].get("donors_with_redirect_301", []) or []
        self.assertTrue(any(str(row.get("domain") or "") == "r1.com" for row in redirect_rows))

    def test_batch_duplicate_domains_choose_most_complete_row_stably(self):
        backlinks_csv = (
            "source_url,target_url,follow\n"
            "https://d1.com/p,https://comp.com/,dofollow\n"
            "https://d2.com/p,https://our.com/,dofollow\n"
        )
        batch_rows_a = (
            "target,domain rating,organic / traffic,backlinks / all\n"
            "comp.com,30,,\n"
            "comp.com,45,100,500\n"
        )
        batch_rows_b = (
            "target,domain rating,organic / traffic,backlinks / all\n"
            "comp.com,45,100,500\n"
            "comp.com,30,,\n"
        )

        result_a = run_link_profile_audit(
            our_domain="our.com",
            backlink_files=[("backlinks.csv", backlinks_csv.encode("utf-8"))],
            batch_file=("batch.csv", batch_rows_a.encode("utf-8")),
        )
        result_b = run_link_profile_audit(
            our_domain="our.com",
            backlink_files=[("backlinks.csv", backlinks_csv.encode("utf-8"))],
            batch_file=("batch.csv", batch_rows_b.encode("utf-8")),
        )

        def _comp_row(result):
            rows = result["results"]["tables"].get("competitor_analysis", []) or []
            for row in rows:
                if str(row.get("competitor_domain") or "") == "comp.com":
                    return row
            return {}

        comp_a = _comp_row(result_a)
        comp_b = _comp_row(result_b)
        self.assertEqual(comp_a.get("batch_dr"), 45.0)
        self.assertEqual(comp_a.get("batch_traffic"), 100.0)
        self.assertEqual(comp_a.get("batch_backlinks_all"), 500.0)
        self.assertEqual(comp_b.get("batch_dr"), 45.0)
        self.assertEqual(comp_b.get("batch_traffic"), 100.0)
        self.assertEqual(comp_b.get("batch_backlinks_all"), 500.0)

        summary_a = result_a["results"]["summary"]
        summary_b = result_b["results"]["summary"]
        self.assertEqual(summary_a.get("batch_duplicate_domains"), 1)
        self.assertEqual(summary_a.get("batch_duplicate_rows_ignored"), 1)
        self.assertEqual(summary_b.get("batch_duplicate_domains"), 1)
        self.assertEqual(summary_b.get("batch_duplicate_rows_ignored"), 1)
        warnings_a = result_a["results"].get("warnings", []) or []
        self.assertTrue(any("дубликаты доменов" in str(item).lower() for item in warnings_a))


if __name__ == "__main__":
    unittest.main()
