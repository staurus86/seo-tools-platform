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
```

Open: `http://localhost:8000`

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
