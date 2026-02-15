# SEO Tools Platform

Асинхронная веб-платформа для комплексного SEO-анализа сайтов с 6 мощными инструментами.

## 🚀 Инструменты

1. **Анализ сайта** - Полный SEO аудит с краулингом до 2000 страниц
2. **Robots.txt** - Проверка доступа и синтаксиса
3. **Sitemap.xml** - Валидация карты сайта
4. **Аудит рендеринга** - Сравнение JS vs No-JS
5. **Мобильная версия** - Тестирование на 20+ устройствах
6. **Проверка ботов** - Доступность для поисковиков и AI

## 🛠️ Технологии

- **Backend**: FastAPI, Celery, Redis
- **Frontend**: Tailwind CSS, Vanilla JS
- **Reports**: Excel (openpyxl), Word (python-docx)
- **Browser**: Playwright
- **Deployment**: Docker, Railway

## 📋 Требования

- Python 3.11+
- Redis 6.0+
- 2GB RAM minimum

## 🚀 Быстрый старт (локально)

```bash
# 1. Клонировать репозиторий
git clone <repo-url>
cd seo-tools-platform

# 2. Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Установить зависимости
pip install -r requirements.txt

# 4. Установить Playwright browsers
playwright install chromium

# 5. Запустить Redis
docker run -d -p 6379:6379 redis:alpine

# 6. Запустить Celery worker
 celery -A app.core.celery_app worker --loglevel=info -Q seo

# 7. Запустить приложение
uvicorn app.main:app --reload
```

Приложение будет доступно по адресу: http://localhost:8000

## 🌐 Деплой на Railway

### Автоматический деплой

1. Форкните этот репозиторий
2. Создайте новый проект на [Railway](https://railway.app)
3. Подключите GitHub репозиторий
4. Railway автоматически:
   - Соберет Docker образ
   - Запустит Redis
   - Развернет приложение
   - Настроит окружение

### Ручная настройка

```bash
# Установить Railway CLI
npm install -g @railway/cli

# Логин
railway login

# Инициализировать проект
railway init

# Добавить Redis
railway add --database redis

# Деплой
railway up
```

### Переменные окружения

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `PORT` | `8000` | Application port |
| `MAX_PAGES_DEFAULT` | `100` | Default pages limit |
| `MAX_PAGES_LIMIT` | `2000` | Maximum pages limit |
| `RATE_LIMIT_PER_HOUR` | `10` | Requests per hour per IP |

## 📊 Ограничения

- **Rate Limit**: 10 запросов в час с одного IP
- **Макс. страниц**: 100 по умолчанию, до 2000 максимум
- **Таймаут задачи**: 20 минут с возможностью продолжения
- **История**: Последние 10 проверок (LocalStorage)

## 🔌 API Endpoints

```
POST /api/tasks/site-analyze      # Анализ сайта
POST /api/tasks/robots-check      # Robots.txt
POST /api/tasks/sitemap-validate  # Sitemap
POST /api/tasks/render-audit      # Рендеринг
POST /api/tasks/mobile-check      # Мобильная версия
POST /api/tasks/bot-check         # Проверка ботов

GET  /api/tasks/{task_id}         # Статус задачи
GET  /api/download/{task_id}/xlsx # Скачать Excel
GET  /api/download/{task_id}/docx # Скачать Word
GET  /api/rate-limit              # Rate limit info
```

Документация API: `/api/docs`

## 📁 Структура проекта

```
seo-tools-platform/
├── app/
│   ├── api/           # API routes & schemas
│   ├── core/          # Celery, Redis, rate limiter
│   ├── tools/         # SEO tools integration
│   ├── reports/       # XLSX & DOCX generators
│   ├── templates/     # HTML templates
│   └── static/        # CSS & JS
├── reports_output/    # Generated reports
├── requirements.txt
├── Dockerfile
├── railway.toml
└── Procfile
```

## 🔄 Архитектура

```
Client → FastAPI → Celery Task → Redis Queue
                              ↓
                         Celery Worker
                              ↓
                    SEO Tool Execution
                              ↓
                    Report Generation (XLSX/DOCX)
                              ↓
                         Response
```

## 📝 Лицензия

MIT License

## 🤝 Поддержка

Если у вас есть вопросы или предложения, создайте Issue в репозитории.

## Encoding Guard (required)

Enable automatic pre-commit encoding checks (one-time setup):

```bash
python scripts/install_git_hooks.py
```

For Windows, this enables `.githooks/pre-commit.cmd`.

Manual full-platform check (same as hook):

```bash
python scripts/encoding_guard.py check --root app --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root scripts --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root tests --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py check --root "Py scripts" --ext .py .html .js .md .txt .json .yml .yaml
```

If issues are found:

```bash
python scripts/encoding_guard.py fix --root app --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py fix --root scripts --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py fix --root tests --ext .py .html .js .md .txt .json .yml .yaml
python scripts/encoding_guard.py fix --root "Py scripts" --ext .py .html .js .md .txt .json .yml .yaml
```

Validation tests:

```bash
python -m unittest tests/test_encoding_guard.py tests/test_site_pro_adapter.py tests/test_site_pro_baseline_diff.py
python scripts/site_pro_preflight.py
```
