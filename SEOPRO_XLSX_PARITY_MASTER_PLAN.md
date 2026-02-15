# Site Audit Pro XLSX Parity Master Plan (vs seopro.py)
Updated: 2026-02-15

## 1. Source and Scope
- Legacy reference report: `Py scripts/Анализ сайтов/20260213_175125.xlsx`.
- Current platform report template: `app/reports/xlsx_generator.py` (`generate_site_audit_pro_report`, full mode).
- Goal: final platform XLSX must preserve and extend legacy coverage.
- Constraint: do not reduce checks/tabs vs legacy.

## 2. Current State Summary
- Legacy report tabs: 17.
- Current platform full report tabs: 12.
- Existing platform tabs are more normalized and deduplicated, but several legacy tab views are missing as first-class tabs.

## 3. Tab-Level Gap Matrix

| Legacy tab | Current coverage | Gap status | Action |
|---|---|---|---|
| 1. Main report | Distributed across 2/3/4/7/12 | Missing dedicated consolidated table | Add `1_MainReport_Compat` with legacy-like columns + solution |
| 2. Hierarchy errors | `7_HierarchyErrors` | Partial | Add `Status`, `Total headers`, `H1 count`, `Solution` columns |
| 3. On-Page SEO | `2_OnPage+Structured`, `12_AdvancedDeep` | Partial | Add `Title Len`, `Meta Len`, explicit `Canonical Status`, `Breadcrumbs`, `Solution` |
| 4. Content | `4_Content+AI` | Partial | Add `Unique %`, `AI Markers List`, `Filler` absolute metric, `Solution` |
| 5. Technical | `3_Technical`, `12_AdvancedDeep` | Partial | Add `Size KB`, `HTML Score`, `Canonical status`, `Robots`, `Deprecated`, `Solution` |
| 6. E-E-A-T | `12_AdvancedDeep` (score only) | Missing dedicated tab | Add `6_EEAT_Compat` with components + solution |
| 7. Trust | `12_AdvancedDeep` (score only) | Missing dedicated tab | Add `7_Trust_Compat` with Contact/Legal/Reviews/Badges + solution |
| 8. Health | `3_Technical` (Health) + others | Missing dedicated tab | Add `8_Health_Compat` with health factors and solution |
| 9. Internal Links | `5_LinkGraph` | Partial | Add explicit `Authority`, `Incoming`, `Outgoing`, `Is Orphan`, `Solution` |
| 10. Images | `6_Images+External` | Partial | Add `No Width`, `No Lazy`, `Issues`, `Solution` |
| 11. External Links | `6_Images+External` (split) | Partial | Add dedicated external links tab with Follow% + solution |
| 12. Structured Data | `2_OnPage+Structured`, `12_AdvancedDeep` | Partial | Add dedicated tab with JSON-LD/Microdata/RDFa and solution |
| 13. Keywords & TF-IDF | `8_Keywords` | Partial | Add explicit `Top Keywords`, `TF-IDF 1..3`, `Solution` |
| 14. Topics | `5_LinkGraph`, `9_SemanticMap` | Partial | Add `Is Hub`, `Cluster`, `Incoming`, compact semantic links summary, `Solution` |
| 15. Advanced | `3_Technical`, `12_AdvancedDeep` | Partial | Add hidden/cloaking/cta/list-table layout and legacy-like view |
| 16. Link Quality | `5_LinkGraph` | Partial | Add dedicated legacy view with `Anchor Score` and `Solution` |
| 17. AI Markers | `4_Content+AI` | Partial | Add dedicated tab with markers found + text sample + recommendation |

## 4. Parameter-Level Missing/Partial Areas

### 4.1 Report projection gaps (data mostly exists in schema)
- `title_len`, `description_len` not projected to current OnPage sheet.
- `images_optimization.no_width_height`, `images_optimization.no_lazy_load` not projected.
- `structured_data_detail.json_ld/microdata/rdfa` not projected.
- `eeat_components` and trust boolean signals not projected.
- `ai_markers_list` not projected; marker snippet context is not projected.
- explicit per-tab `Solution` fields are mostly absent in current normalized sheets.

### 4.2 Data/logic parity gaps (not only projection)
- `cloaking_detected` currently placeholder.
- Some legacy formula parity remains heuristic (`h_hierarchy` details, trust/eeat components, cta quality edge cases).
- Need deterministic text snippet extraction for AI markers tab.

## 5. Target Final Report Design

## 5.1 Tabs policy
- Keep existing optimized tabs (`1_Executive` ... `12_AdvancedDeep`).
- Add compatibility tabs that reconstruct all legacy views.
- Final tab count target: >= 17 legacy-equivalent views, preferably 20+ (12 core + compat pack).

## 5.2 No-duplicate metric ownership policy
- One canonical metric source in payload/schema.
- Multiple tabs may display it, but transformations must be read-only projections.
- Implement a centralized metric registry in XLSX generator to avoid drift.

## 6. Implementation Phases

### Phase A: Projection Parity (fast)
1. Extend `xlsx_generator.py` with compatibility tabs for all missing legacy views.
2. Add per-tab `Solution` composer helpers (deterministic rule-based text).
3. Keep core 12 tabs unchanged for API/UI continuity.

### Phase B: Metric Parity (medium)
1. Complete remaining legacy-equivalent calculations in adapter:
- hierarchy diagnostics details,
- trust and eeat component weights,
- cloaking logic fallback,
- AI marker snippets extraction.
2. Add strict validation checks for fields required by compatibility tabs.

### Phase C: Formula/UX Enhancements (optional but recommended)
1. Add summary KPI formulas in executive and compat summary tab.
2. Add conditional formatting parity (critical/warning/info + score bands).
3. Add cross-sheet hyperlinks for drill-down (`URL` -> per-topic tabs).

### Phase D: Contract and Regression Hardening
1. Add tests for sheet count/name/order in full mode.
2. Add tests for required headers per compatibility tab.
3. Add snapshot test for generated XLSX structure (headers + sample row values).
4. Keep `encoding_guard` and `site_pro_gap_report --strict` in preflight.

## 7. Detailed Work Backlog
- [ ] Add `1_MainReport_Compat`.
- [ ] Expand `7_HierarchyErrors` columns to legacy-equivalent.
- [ ] Add `6_EEAT_Compat`.
- [ ] Add `7_Trust_Compat`.
- [ ] Add `8_Health_Compat`.
- [ ] Add `10_Images_Compat`.
- [ ] Add `11_ExternalLinks_Compat`.
- [ ] Add `12_StructuredData_Compat`.
- [ ] Add `13_KeywordsTFIDF_Compat`.
- [ ] Add `14_Topics_Compat`.
- [ ] Add `15_Advanced_Compat`.
- [ ] Add `16_LinkQuality_Compat`.
- [ ] Add `17_AIMarkers_Compat`.
- [ ] Add `SolutionBuilder` utility for all compat tabs.
- [ ] Add XLSX parity tests (sheet names, headers, non-empty key columns).

## 8. Acceptance Criteria
- Full mode exports all legacy-equivalent tabs and checks.
- No legacy metric/check is missing.
- Existing platform core tabs remain valid and backward compatible.
- Preflight passes:
  - unit tests,
  - encoding guard,
  - strict legacy-key coverage report.

## 9. Execution Order (recommended)
1. Projection parity tabs (A1-A3).
2. Trust/EEAT/AI marker data completion (B1).
3. Hierarchy + cloaking parity completion (B1).
4. Testing and snapshots (D1-D4).
5. Final visual polish and formulas (C1-C3).
