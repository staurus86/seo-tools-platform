# SEO Tools Platform — Plan improvements 2026-03-21

## Phase A — Quick wins
- 2.1 Bot Checker: async parallel requests (concurrent.futures)
- 2.2 Bot Checker: custom bot UA input in UI card
- 2.3 Bot Checker: content body verification (not just status code)
- 3.1 Mobile Audit: landscape orientation testing
- 4.1 Render Audit: JS framework detection (React/Vue/Next/Nuxt/Angular/Svelte)
- 4.2 Render Audit: console errors/warnings in report output
- 5.3 OnPage: Twitter Card validation (twitter:title, twitter:description, twitter:image, twitter:card)
- 6.1 Clusterizer: XLSX export with formatting
- 6.4 Clusterizer: pymorphy3 for Russian morphology in stemming
- 8.4 Redirect Checker: timing breakdown per hop (DNS/connect/TLS/TTFB/transfer)
- 9.2 CWV: combined mobile+desktop report in single run
- 10.1 LLM Crawler: llms.txt file detection and parsing
- 10.2 LLM Crawler: CSS visibility check (display:none, visibility:hidden, opacity:0)

## Phase B — Medium improvements
- 1.3 Site Audit Pro: broken link checking with batched requests
- 1.4 Site Audit Pro: image analysis (size, format, compression, modern formats)
- 2.4 Bot Checker: conditional JS rendering (only if raw HTML has no content)
- 3.2 Mobile Audit: network throttling simulation (3G/4G)
- 3.3 Mobile Audit: WCAG AA accessibility checks (careful: false positives)
- 3.4 Mobile Audit: color contrast checking
- 4.3 Render Audit: CSS-based content visibility detection
- 4.4 Render Audit: SSR vs CSR hydration test
- 5.1 OnPage: multi-language support (beyond RU/EN)
- 5.2 OnPage: broken link checking on page (batched, same as 1.3)
- 5.4 OnPage: SERP preview simulation (visual)
- 7.1 Link Profile: auto disavow file generation
- 7.2 Link Profile: anchor lemmatization (pymorphy3)
- 7.3 Link Profile: link velocity chart (from uploaded data dates)
- 7.4 Link Profile: DOCX/XLSX full report export
- 8.1 Redirect Checker: JS redirect detection via Playwright

## Phase C — Big features
- X.2 PDF/DOCX reports for all tools
- X.3 WebSocket real-time updates (replace polling)
- 8.new Redirect Checker: new unique SEO checks (research needed)
- 11.2 Robots.txt: visual constructor + URL accessibility validator
- 6.3 Clusterizer: keyword expansion (People Also Ask / suggestions)
- 5.5 OnPage: competitor comparison (top-10 SERP)

## Phase D — Unified platform
- X.4 Batch mode for all tools
- X.5 Unified Full SEO Audit: single URL -> all tools -> combined report + dev task specs
