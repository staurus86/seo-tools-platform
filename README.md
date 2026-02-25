# SEO Tools Platform

SEO Tools Platform is an async web platform for technical and content SEO audits with XLSX/DOCX reporting.

## Stack
- Backend: FastAPI, Celery, Redis
- Frontend: Tailwind CSS, Vanilla JS
- Reporting: openpyxl, python-docx
- Browser automation: Playwright

## Tools
- Site Analyze
- Robots.txt Audit
- Sitemap Validation
- Render Audit (JS vs No-JS)
- Mobile Audit
- Bot Accessibility Check
- Site Audit Pro
- LLM Crawler Simulation (feature-flagged)

## Quick Start
```bash
git clone <repo-url>
cd seo-tools-platform

python -m venv venv
# Linux/macOS
source venv/bin/activate
# Windows
# venv\Scripts\activate

pip install -r requirements.txt
playwright install chromium

docker run -d -p 6379:6379 redis:alpine
celery -A app.core.celery_app worker --loglevel=info -Q seo
uvicorn app.main:app --reload

# Optional isolated worker for LLM Crawler Simulation
set FEATURE_LLM_CRAWLER=true
set SERVICE_MODE=llm-worker
python -m app.tools.llmCrawler.worker
```

Open: `http://localhost:8000`

## LLM Crawler env vars
- `FEATURE_LLM_CRAWLER` (default `false`)
- `LLM_CRAWLER_ALLOWLIST` (comma-separated user/project ids for canary)
- `JOB_CONCURRENCY` (default `2`)
- `FETCH_TIMEOUT_MS` (default `20000`)
- `MAX_HTML_BYTES` (default `2000000`)
- `LLM_CRAWLER_MAX_REDIRECT_HOPS` (default `8`)
- `PLAYWRIGHT_BROWSERS_PATH` (default `/ms-playwright` in Railway configs)
- `PLAYWRIGHT_AUTO_INSTALL_ON_BOOT` (default `0`; set `1` only if you intentionally allow slow runtime browser install)
- `LLM_CRAWLER_REQUIRE_HEALTHY_WORKER` (default `true`; reject new jobs if worker heartbeat is stale/missing)
- `LLM_CRAWLER_STUCK_JOB_TIMEOUT_SEC` (default `300`; convert stuck queued/running jobs to error)

## Railway config-as-code (web + worker)
- `railway.web.toml` for the web service (has `healthcheckPath = "/health"`).
- `railway.worker.toml` for the worker service (no HTTP healthcheck).
- In Railway service settings set:
  - web service `Config as Code file path` -> `railway.web.toml`
  - worker service `Config as Code file path` -> `railway.worker.toml`
- Both configs pin `PLAYWRIGHT_BROWSERS_PATH=/ms-playwright`.

## API Docs
- Swagger: `/api/docs`

## Encoding Guard (required)
Install hooks:
```bash
python scripts/install_git_hooks.py
```

Check:
```bash
python scripts/encoding_guard.py check --root app --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root scripts --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root tests --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root "Py scripts" --ext .py .html .js .md .txt .json .yml .yaml
```

Preflight:
```bash
python scripts/site_pro_preflight.py
```

## Community and Policy Files
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Contributing: `CONTRIBUTING.md`
- Security Policy: `SECURITY.md`
- License: `LICENSE`
- Issue Templates: `.github/ISSUE_TEMPLATE/`
- Pull Request Template: `.github/pull_request_template.md`
