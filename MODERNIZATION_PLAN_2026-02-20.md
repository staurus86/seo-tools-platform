# SEO Tools Platform Modernization Plan
Updated: 2026-02-20

## Scope
- Platform tools in active use:
  - OnPage Audit
  - Site Audit Pro
  - Robots.txt Audit
  - Sitemap Validation
  - Render Audit
  - Mobile Audit
  - Bot Access Check

## Tool 1: OnPage Audit (10 enhancements)
1. Add batch URL mode with queue progress and per-URL result cards.
2. Add SERP snippet simulator with title/description truncation previews.
3. Add intent classifier (informational/commercial/navigational) per page.
4. Add internal linking context quality score by anchor semantics.
5. Add section-level quality map (intro/body/faq/footer).
6. Add result page comparisons against previous run (delta mode).
7. Add DOCX executive summary with top 10 actionable fixes.
8. Add XLSX worksheet with phrase-level content decay/overuse tracking.
9. Add configurable threshold profiles by language/vertical.
10. Add rule presets for ecommerce/blog/docs content types.

## Tool 2: Site Audit Pro (10 enhancements)
1. Add stable strict preflight fallback when legacy reference file is absent.
2. Move KPI thresholds to structured config profiles per domain type.
3. Add score explainability block (factor weights and contribution deltas).
4. Add issue ownership routing (SEO/Dev/Content/Product) with confidence.
5. Add run-to-run baseline regression alerts for key health metrics.
6. Add drill-down links from summary KPI cards to exact issue rows.
7. Add batch-mode aggregate dashboard (cross-domain summary).
8. Add DOCX roadmap section (Now/Next/Later + sprint suggestions).
9. Add XLSX contract snapshots for stable sheet/header schema.
10. Add anomaly suppression controls to tune false-positive density.

## Tool 3: Robots.txt Audit (10 enhancements)
1. Validate robots across protocol/host variants (http/https/www/non-www).
2. Add interactive URL path tester with per-bot verdict.
3. Add strict parser mode for unsupported directives and policy conflicts.
4. Add robots vs meta/x-robots consistency checks.
5. Add conflict matrix for overlapping Allow/Disallow patterns.
6. Add result page patch preview for safe fixes.
7. Add XLSX export (currently only DOCX).
8. Add priority score for each detected issue (impact and effort).
9. Add automatic sitemap endpoint validation from robots file entries.
10. Add historical diff view for robots changes between runs.

## Tool 4: Sitemap Validation (10 enhancements)
1. Add streaming parser mode for very large sitemap indexes.
2. Add canonical mismatch checks against sampled live pages.
3. Add indexability sampling (status/noindex/redirect loops).
4. Add freshness checks using `lastmod` consistency rules.
5. Add per-file quality scoring and weighted health index.
6. Add result page treemap by file and error density.
7. Add duplicate URL root-cause grouping.
8. Add DOCX export (currently only XLSX).
9. Add XLSX tabs for invalid URLs, duplicates, and indexability sample.
10. Add incremental validation mode against previous successful run.

## Tool 5: Render Audit (10 enhancements)
1. Add profile matrix: desktop/mobile + no-JS/JS + optional auth.
2. Add semantic DOM diff (title/meta/schema/headings) with impact score.
3. Add hydration/runtime JS error taxonomy with grouped causes.
4. Add soft-404 detection in rendered and raw variants.
5. Add side-by-side synced compare panel in result page.
6. Add trace artifact bundle (HTML snapshots + screenshots + metadata).
7. Add DOCX appendix with visual evidence and major diffs.
8. Add XLSX diff matrix by variant/profile.
9. Add regression view against baseline snapshots.
10. Add structured-data parity checks per schema type.

## Tool 6: Mobile Audit (10 enhancements)
1. Add CWV metrics per device (LCP/CLS/INP) in audit output.
2. Add tap latency and interactivity diagnostics.
3. Add overlap detector for fixed banners and CTA collisions.
4. Add foldable posture checks and safe-area padding validation.
5. Add result page device ranking with issue heat score.
6. Add viewport and typography readability cluster analysis.
7. Add DOCX checklist by device family (phone/tablet/foldable).
8. Add XLSX tabs: device metrics, vitals, issues, screenshot index.
9. Add custom device profile support from UI.
10. Add baseline-mode alerts only on degradations.

## Tool 7: Bot Access Check (10 enhancements)
1. Add DOCX export endpoint and result page action.
2. Split availability into two KPIs: reachable vs indexable.
3. Improve robots matching to exact token-level bot identity.
4. Add consistency checks for `X-Robots-Tag` vs meta robots.
5. Add anti-bot challenge heuristics and blocked-response signatures.
6. Add bot matrix view (bots x signals) with filtering and sorting.
7. Add issue prioritization based on business-critical bot groups.
8. Add baseline diff between runs with regression alerts.
9. Add recommendation templates by blocker type (WAF/CDN/robots/meta).
10. Add optional retry strategy profiles for unstable origins.

## Platform Cross-Cutting Improvements
1. Add unified scenario fixtures for all tools (contract test pack).
2. Add centralized report schema versioning for DOCX/XLSX generators.
3. Add automated E2E smoke run for all task types in CI.
4. Add structured event logging per tool run (duration, errors, retries).
5. Add stronger encoding guard gates for templates and JS output text.
6. Add artifact retention policies with environment-based TTL.
7. Add capability flags in UI to show enabled export formats per tool.
8. Add task result comparison API (`current` vs `baseline`).
9. Add release checklist for report compatibility and API stability.
10. Add governance board for threshold tuning and false-positive tracking.
