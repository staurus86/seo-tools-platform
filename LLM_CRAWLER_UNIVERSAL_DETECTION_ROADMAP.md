# LLM Crawler Universal Detection Roadmap

## Goal
Make detection robust across any website/page type while keeping backward compatibility and fail-soft behavior.

## Principles
- JSON result is the source of truth.
- No site/domain/CMS hardcoding.
- Every detector returns: `status`, `confidence`, `version`, `evidence`.
- New fields are optional and non-breaking.
- If confidence is low, return `not_evaluated` or `partial` with explicit reason.

## KPI Targets
- Main content detection F1: >= 0.90
- Structured data type recall: >= 0.95
- Page type top-1 accuracy: >= 0.88
- Retrieval simulation avg score: >= 0.70
- Citation calibration ECE: <= 0.08

## P0 (Hard Must)
1. Detector quality layer in JSON
- Add unified detector reports per module.
- Add global detector summary: coverage and avg confidence.
- Output path: `result.detectors.*`.

2. Universal content extraction fusion
- Arbitration between readability/trafilatura/dom/heading extractors.
- Persist extractor scores and primary extractor.
- Output path: `result.content_extraction`.

3. Segmentation confidence normalization
- Use consistent ratios and confidence scales.
- Mark uncertain pages as `partial/not_evaluated` when needed.

4. Validation guardrails
- Add content sufficiency checks and warnings.
- Ensure `not_evaluated` instead of silent zeros.

5. Quality contract tests
- Test presence/shape of detector layer.
- Test fail-soft behavior when extractor modules are unavailable.

## P1 (PRO)
1. Entity detection v4
- Source fusion: schema + meta + headings + body + anchors + optional NLP.
- Entity normalization/dedup and confidence calibration per source mix.

2. Page classification v4
- Feature-based classifier with confidence thresholding.
- Better docs/listing/mixed differentiation in ambiguous pages.

3. Retrieval simulation v2
- Multi-intent synthetic queries (informational/commercial/navigational).
- Add per-query diagnostics and confidence intervals.

4. Citation model calibration
- Refit weights using benchmark dataset.
- Add calibration diagnostics in JSON (`calibration_error`, `support`).

## P2 (Advanced)
1. Hard-case rendering profiles
- SPA/hydration/infinite-scroll/lazy widgets.
- Optional screenshot/DOM region overlays.

2. Benchmark dataset and drift monitoring
- Curated page corpus by page type.
- Track detector quality drift over commits/releases.

3. Auto-repair recommendations quality
- Recommendation ranking by expected lift confidence.
- Contradiction checker across recommendations.

## Tracker Backlog (Ready-to-Use)
### P0
- `LLMDET-001` Add detector layer (`result.detectors`) and summary.
- `LLMDET-002` Add extractor arbitration confidence outputs.
- `LLMDET-003` Add validation warnings for low-content and low-confidence extraction.
- `LLMDET-004` Add quality contract tests for detector shape and fail-soft.

### P1
- `LLMDET-101` Entity extraction source fusion and confidence calibration.
- `LLMDET-102` Page classifier confidence tuning and ambiguous fallback policy.
- `LLMDET-103` Retrieval multi-intent simulation and diagnostics.
- `LLMDET-104` Citation model calibration metrics.

### P2
- `LLMDET-201` SPA/hydration render stress profiles.
- `LLMDET-202` Dataset-driven drift dashboard.
- `LLMDET-203` Recommendation contradiction/consistency validator.

## Rollout Strategy
1. Ship new fields as optional.
2. Keep old scoring/fields intact.
3. Add monitoring on detector coverage and confidence.
4. Enable stricter gating only after KPI baseline stabilizes.
