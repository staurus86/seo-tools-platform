"""
SEO Tools Platform - FastAPI Application (Simplified for Railway)
"""
import os
import sys
import logging

# Setup logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

logger.info("=" * 50)
logger.info("STARTING SEO TOOLS PLATFORM")
logger.info("=" * 50)

# Log environment
logger.info(f"Python executable: {sys.executable}")
logger.info(f"Python version: {sys.version}")
logger.info(f"Current directory: {os.getcwd()}")
logger.info(f"Files in current dir: {os.listdir('.')}")
logger.info(f"PORT env var: {os.environ.get('PORT', 'NOT SET')}")
logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'NOT SET')}")

# Try importing FastAPI
try:
    logger.info("Importing FastAPI...")
    from fastapi import FastAPI, Request
    from fastapi.staticfiles import StaticFiles
    from fastapi.templating import Jinja2Templates
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse
    logger.info("✓ FastAPI imported successfully")
except Exception as e:
    logger.error(f"✗ FastAPI import failed: {e}")
    import traceback
    traceback.print_exc()
    raise

# Try importing app config
try:
    logger.info("Importing app.config...")
    from app.config import settings
    logger.info(f"✓ Config loaded: PORT={settings.PORT}, HOST={settings.HOST}")
except Exception as e:
    logger.error(f"✗ Config import failed: {e}")
    import traceback
    traceback.print_exc()
    raise

# Try importing routes
try:
    logger.info("Importing app.api.routes...")
    from app.api.routes import router as api_router
    logger.info("✓ Routes imported successfully")
except Exception as e:
    logger.error(f"✗ Routes import failed: {e}")
    import traceback
    traceback.print_exc()
    raise

# Create FastAPI app
logger.info("Creating FastAPI app...")
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Asynchronous SEO analysis platform with 6 powerful tools",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files - try multiple paths
static_mounted = False
static_paths = ["app/static", "/app/app/static", "./app/static"]

for path in static_paths:
    try:
        logger.info(f"Trying static path: {path}")
        if os.path.exists(path):
            app.mount("/static", StaticFiles(directory=path), name="static")
            logger.info(f"✓ Static files mounted from: {path}")
            static_mounted = True
            break
        else:
            logger.warning(f"Static path does not exist: {path}")
    except Exception as e:
        logger.warning(f"Failed to mount static from {path}: {e}")

if not static_mounted:
    logger.error("✗ Could not mount static files from any path")

# Templates - try multiple paths for different environments
templates = None
template_paths = ["app/templates", "/app/app/templates", "./app/templates"]

for path in template_paths:
    try:
        logger.info(f"Trying templates path: {path}")
        if os.path.exists(path):
            templates = Jinja2Templates(directory=path)
            logger.info(f"✓ Templates configured from: {path}")
            break
        else:
            logger.warning(f"Path does not exist: {path}")
    except Exception as e:
        logger.warning(f"Failed to load templates from {path}: {e}")

if templates is None:
    logger.error("✗ Could not load templates from any path")

# API routes
app.include_router(api_router)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Главная страница с инструментами"""
    if templates:
        return templates.TemplateResponse("index.html", {"request": request})
    return HTMLResponse("<h1>SEO Tools Platform</h1><p>Templates not loaded</p>")


@app.get("/results/{task_id}", response_class=HTMLResponse)
async def results_page(request: Request, task_id: str):
    """Страница результатов"""
    if templates:
        return templates.TemplateResponse(
            "task_progress.html", 
            {"request": request, "task_id": task_id}
        )
    return HTMLResponse(f"<h1>Results</h1><p>Task: {task_id}</p>")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": settings.APP_VERSION}


@app.get("/robots.txt")
async def robots_txt():
    """Serve robots.txt"""
    return """User-agent: *
Allow: /
Sitemap: https://seo-tools-platform.up.railway.app/sitemap.xml
"""


@app.get("/favicon.ico")
async def favicon():
    """Serve favicon"""
    from fastapi.responses import Response
    # Return empty 204 No Content
    return Response(status_code=204)


logger.info("=" * 50)
logger.info("APP INITIALIZED SUCCESSFULLY")
logger.info("=" * 50)


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting uvicorn on {settings.HOST}:{settings.PORT}")
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
