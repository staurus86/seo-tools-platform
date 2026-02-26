"""
SEO Tools API Routes — thin router that aggregates all tool sub-routers.
"""
from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["SEO Tools"])

# ─── sub-routers (extracted modules) ──────────────────────────────────────
from app.api.routers import exports as _exports_mod          # noqa: E402
from app.api.routers import tasks as _tasks_mod              # noqa: E402
from app.api.routers import redirect as _redirect_mod        # noqa: E402
from app.api.routers import site_pro as _site_pro_mod        # noqa: E402
from app.api.routers import onpage as _onpage_mod            # noqa: E402
from app.api.routers import clusterizer as _clusterizer_mod  # noqa: E402
from app.api.routers import render as _render_mod            # noqa: E402
from app.api.routers import mobile as _mobile_mod            # noqa: E402
from app.api.routers import link_profile as _link_profile_mod  # noqa: E402
from app.api.routers import cwv as _cwv_mod                  # noqa: E402
from app.api.routers import site_analyze as _site_analyze_mod  # noqa: E402
from app.api.routers import robots as _robots_mod            # noqa: E402

router.include_router(_exports_mod.router)
router.include_router(_tasks_mod.router)
router.include_router(_redirect_mod.router)
router.include_router(_site_pro_mod.router)
router.include_router(_onpage_mod.router)
router.include_router(_clusterizer_mod.router)
router.include_router(_render_mod.router)
router.include_router(_mobile_mod.router)
router.include_router(_link_profile_mod.router)
router.include_router(_cwv_mod.router)
router.include_router(_site_analyze_mod.router)
router.include_router(_robots_mod.router)
