# Site Audit Pro: Full Parameter Migration Plan
Updated: 2026-02-15

## 1) Verification Summary
- Source analyzed: `Py scripts/Анализ сайтов/seopro.py` (`analyze_page` return payload + post-processing pipeline).
- Current platform target: `app/tools/site_pro/*` + `app/reports/*`.
- Automated key coverage check: `python scripts/site_pro_gap_report.py`.
- Result: `96/96` legacy `analyze_page` parameter keys are now represented in `NormalizedSiteAuditRow`.

## 2) What Is Still Not Parity-Accurate
All keys are present, but part of calculations are still heuristic and need exact porting from legacy logic.

### A. High Priority (Business metrics that affect scoring)
- `unique_percent` currently derived from lexical diversity proxy.
- `tf_idf_keywords` currently rank-based proxy, not full TF-IDF score map from legacy.
- `semantic_links` currently based on compact semantic suggestions, not full relevance map parity.
- `anchor_text_quality_score` currently ratio proxy, not full legacy scoring chain.
- `site_health_score` parity depends on canonical/quality/penalty details.

### B. Medium Priority (Content quality and technical depth)
- `h_hierarchy`, `h_errors`, `h_details` currently simplified.
- `filler_phrases` list currently token-based, not full phrase-level legacy extraction.
- `keyword_stuffing_score` currently simplified.
- `content_density` and `boilerplate_percent` still lightweight approximations.
- `html_quality_score` currently simplified formula.

### C. Medium Priority (Trust/E-E-A-T)
- `trust_score`, `eeat_score`, `eeat_components` currently heuristic.
- Signals `has_contact_info`, `has_legal_docs`, `has_author_info`, `has_reviews`, `trust_badges` need stricter legacy-compatible detectors.

### D. Low Priority (Detection edge-cases and anti-abuse)
- `cloaking_detected` currently placeholder (`False`).
- `js_dependence` threshold-based approximation.
- `cta_text_quality` currently simple keyword ratio.

## 3) Implementation Phases

### Phase 1: Scoring-Parity Core (must be first)
1. Port exact legacy formulas for:
   - `unique_percent`
   - `tf_idf_keywords`
   - `site_health_score`
2. Port `build_semantic_linking_map` logic for row-level `semantic_links`.
3. Port `analyze_anchor_text_quality` + `calculate_linking_quality_score` parity.
4. Validate against fixture baseline deltas.

### Phase 2: Content/Hierarchy Parity
1. Port `analyze_h_hierarchy_detailed` behavior one-to-one.
2. Port phrase-level `count_filler_phrases`.
3. Port `detect_keyword_stuffing`, `calculate_content_density`, `calculate_boilerplate`.
4. Expand tests for exact field values and tolerances.

### Phase 3: Trust and E-E-A-T Parity
1. Port detector functions:
   - contact/legal/author/reviews/badges
2. Port exact trust/E-E-A-T component weighting.
3. Verify output stability on multilingual pages.

### Phase 4: Deep Diagnostics and Edge Cases
1. Port `detect_cloaking` with deterministic fallback behavior.
2. Port `check_js_dependence` parity logic.
3. Port CTA quality analyzer parity.

### Phase 5: Report Parity and UX
1. Add full parity fields to deep report tabs (`12_AdvancedDeep` and optional extra tabs if needed).
2. Keep quick mode deduplicated and compact.
3. Keep heavy details in chunk artifacts with manifest links.

## 4) Testing and Acceptance Criteria
- Unit:
  - parity tests per migrated function (`tf-idf`, hierarchy, trust, E-E-A-T, link quality).
- Snapshot:
  - fixture baseline with tolerances for key numeric fields.
- Contract:
  - no regressions in existing `site_audit_pro` API schema.
- Reports:
  - XLSX quick dedup rules preserved.
  - full mode includes all migrated deep metrics.

## 5) Immediate Next Iteration
1. Replace proxy `tf_idf_keywords` with exact legacy TF-IDF score dict.
2. Replace proxy `semantic_links` with legacy relevance map.
3. Port exact `anchor_text_quality_score` and linking quality chain.
