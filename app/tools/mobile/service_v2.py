"""Mobile checker v2 with multi-device screenshots and issue analysis."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import re
import time

import requests
from bs4 import BeautifulSoup

from app.config import settings


@dataclass(frozen=True)
class MobileDevice:
    name: str
    category: str
    width: int
    height: int
    dpr: float
    user_agent: str


DEVICE_PROFILES: List[MobileDevice] = [
    MobileDevice(
        "iPhone 15 Pro",
        "phone",
        393,
        852,
        3.0,
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "iPhone 16 Pro",
        "phone",
        402,
        874,
        3.0,
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "Samsung Galaxy S24 Ultra",
        "phone",
        430,
        932,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Samsung Galaxy S25 Ultra",
        "phone",
        430,
        932,
        3.0,
        "Mozilla/5.0 (Linux; Android 15; SM-S938B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Google Pixel 8 Pro",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Google Pixel 9 Pro",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 15; Pixel 9 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Samsung Galaxy A55",
        "phone",
        412,
        915,
        2.75,
        "Mozilla/5.0 (Linux; Android 14; SM-A556B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Huawei Pura 70 Pro",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; HBN-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "OPPO Find X7",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; PHY110) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "vivo X100 Pro",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; V2303A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Honor Magic6 Pro",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; BVL-AN16) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "realme GT 6",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; RMX3851) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "Xiaomi 14",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; 23127PN0CC) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "OnePlus 12",
        "phone",
        412,
        915,
        3.0,
        "Mozilla/5.0 (Linux; Android 14; CPH2581) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    ),
    MobileDevice(
        "iPad Pro 12.9 (2024)",
        "tablet",
        1024,
        1366,
        2.0,
        "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "iPad Pro 11 (2024)",
        "tablet",
        834,
        1194,
        2.0,
        "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "iPad Air (2024)",
        "tablet",
        820,
        1180,
        2.0,
        "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "iPad Mini (2023)",
        "tablet",
        768,
        1024,
        2.0,
        "Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    ),
    MobileDevice(
        "Samsung Galaxy Tab S9",
        "tablet",
        960,
        1376,
        2.25,
        "Mozilla/5.0 (Linux; Android 14; SM-X716B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Safari/537.36",
    ),
]

DEFAULT_DEVICE_NAMES = [
    "iPhone 15 Pro",
    "iPhone 16 Pro",
    "Samsung Galaxy S24 Ultra",
    "Samsung Galaxy S25 Ultra",
    "Google Pixel 8 Pro",
    "Google Pixel 9 Pro",
    "Samsung Galaxy A55",
    "Huawei Pura 70 Pro",
    "OPPO Find X7",
    "vivo X100 Pro",
    "Honor Magic6 Pro",
    "realme GT 6",
    "Xiaomi 14",
    "iPad Pro 12.9 (2024)",
    "iPad Pro 11 (2024)",
    "iPad Air (2024)",
    "iPad Mini (2023)",
    "Samsung Galaxy Tab S9",
]

QUICK_DEVICE_NAMES = [
    "iPhone 16 Pro",
    "Samsung Galaxy S25 Ultra",
    "iPad Pro 11 (2024)",
]


def _slug(text: str) -> str:
    val = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return val[:80] if len(val) > 80 else val


def _extract_meta_viewport(soup: BeautifulSoup) -> Optional[str]:
    meta = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    if not meta:
        return None
    return (meta.get("content") or "").strip() or None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


class MobileCheckServiceV2:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def _select_devices(self, mode: str = "full", selected: Optional[List[str]] = None) -> List[MobileDevice]:
        by_name = {d.name: d for d in DEVICE_PROFILES}
        if selected:
            out = [by_name[name] for name in selected if name in by_name]
            if out:
                return out
        names = QUICK_DEVICE_NAMES if mode == "quick" else DEFAULT_DEVICE_NAMES
        return [by_name[n] for n in names if n in by_name]

    def _http_prefetch(self, url: str) -> Dict[str, Any]:
        try:
            resp = requests.get(url, timeout=self.timeout, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "html.parser")
            viewport = _extract_meta_viewport(soup)
            return {
                "ok": True,
                "status_code": resp.status_code,
                "final_url": resp.url,
                "viewport_content": viewport,
                "has_viewport": bool(viewport),
            }
        except Exception as e:
            return {
                "ok": False,
                "status_code": None,
                "error": str(e),
                "final_url": url,
                "viewport_content": None,
                "has_viewport": False,
            }

    def _evaluate_page(self, page) -> Dict[str, Any]:
        js = """
        (() => {
          const viewportWidth = window.innerWidth || 0;
          const docWidth = Math.max(document.body?.scrollWidth || 0, document.documentElement?.scrollWidth || 0);
          const horizontalOverflow = docWidth > viewportWidth + 2;

          const touchElements = Array.from(document.querySelectorAll('a,button,input,select,textarea,[role="button"]'));
          const touchRects = touchElements.map(el => {
            const r = el.getBoundingClientRect();
            return {w:r.width,h:r.height,visible:r.width>0&&r.height>0};
          }).filter(x => x.visible);
          const smallTouchTargets = touchRects.filter(r => r.w < 44 || r.h < 44).length;

          const textNodes = Array.from(document.querySelectorAll('p,span,li,a,button,label,input,textarea,h1,h2,h3,h4,h5,h6'));
          const smallFonts = textNodes.filter(el => {
            const style = window.getComputedStyle(el);
            const size = parseFloat(style.fontSize || '0');
            return size > 0 && size < 16;
          }).length;

          const images = Array.from(document.images || []);
          const largeImages = images.filter(img => {
            const r = img.getBoundingClientRect();
            return r.width > viewportWidth + 2;
          }).length;

          const menus = document.querySelectorAll('.hamburger,.menu-toggle,.navbar-toggle,[aria-label=\"menu\"]');
          const hasHamburger = menus.length > 0;

          return {
            viewport_width: viewportWidth,
            document_width: docWidth,
            horizontal_overflow: horizontalOverflow,
            touch_targets_total: touchRects.length,
            small_touch_targets: smallTouchTargets,
            small_fonts: smallFonts,
            large_images: largeImages,
            has_hamburger_menu: hasHamburger,
          };
        })();
        """
        return page.evaluate(js)

    def _collect_issues(self, eval_data: Dict[str, Any], viewport_content: Optional[str], console_errors: int) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        if not viewport_content:
            issues.append({"severity": "critical", "code": "viewport_missing", "title": "Отсутствует meta viewport", "details": "Добавьте meta viewport для корректного отображения на мобильных устройствах."})
        elif "width=device-width" not in viewport_content:
            issues.append({"severity": "critical", "code": "viewport_invalid", "title": "Некорректный viewport", "details": f"Текущее значение: {viewport_content}"})
        if eval_data.get("horizontal_overflow"):
            issues.append({"severity": "critical", "code": "horizontal_overflow", "title": "Обнаружен горизонтальный скролл", "details": f"Ширина документа {eval_data.get('document_width')}px превышает viewport {eval_data.get('viewport_width')}px"})
        if _safe_int(eval_data.get("small_touch_targets")) > 0:
            issues.append({"severity": "warning", "code": "small_touch_targets", "title": "Слишком маленькие интерактивные элементы", "details": f"{eval_data.get('small_touch_targets')} элементов меньше 44x44px."})
        if _safe_int(eval_data.get("small_fonts")) > 0:
            issues.append({"severity": "warning", "code": "small_fonts", "title": "Слишком мелкий текст", "details": f"{eval_data.get('small_fonts')} элементов с размером шрифта меньше 16px."})
        if _safe_int(eval_data.get("large_images")) > 0:
            issues.append({"severity": "warning", "code": "large_images", "title": "Изображения шире экрана", "details": f"{eval_data.get('large_images')} изображений выходят за границы viewport."})
        if console_errors > 0:
            issues.append({"severity": "warning", "code": "console_errors", "title": "Ошибки JavaScript в консоли", "details": f"Зафиксировано {console_errors} ошибок в консоли браузера."})
        if not eval_data.get("has_hamburger_menu"):
            issues.append({"severity": "info", "code": "nav_menu", "title": "Не найдено мобильное меню", "details": "Навигация может быть неудобной на узких экранах."})
        return issues

    def run(
        self,
        url: str,
        task_id: str,
        mode: str = "full",
        selected_devices: Optional[List[str]] = None,
        progress_callback=None,
    ) -> Dict[str, Any]:
        def _notify(progress: int, message: str) -> None:
            if callable(progress_callback):
                try:
                    progress_callback(progress, message)
                except Exception:
                    pass

        devices = self._select_devices(mode=mode, selected=selected_devices)
        _notify(8, "Получение метаданных страницы")
        prefetch = self._http_prefetch(url)
        domain = urlparse(url).netloc or "site"
        stamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        shot_dir = Path(settings.REPORTS_DIR) / "mobile" / task_id / "screenshots"
        shot_dir.mkdir(parents=True, exist_ok=True)

        results: List[Dict[str, Any]] = []
        global_issues: List[Dict[str, Any]] = []
        try:
            from playwright.sync_api import sync_playwright
        except Exception as e:
            _notify(100, "Среда Playwright недоступна")
            return {
                "task_type": "mobile_check",
                "url": url,
                "completed_at": datetime.utcnow().isoformat(),
                "results": {
                    "engine": "v2",
                    "error": f"Playwright unavailable: {e}",
                    "mobile_friendly": False,
                    "devices_tested": [d.name for d in devices],
                    "device_results": [],
                    "issues": [{"severity": "critical", "code": "playwright_unavailable", "title": "Playwright runtime unavailable", "details": str(e)}],
                    "issues_count": 1,
                    "artifacts": {"screenshot_dir": str(shot_dir), "screenshots": []},
                },
            }

        with sync_playwright() as p:
            _notify(12, "Запуск браузерного движка")
            browser = p.chromium.launch(headless=True)
            total_devices = len(devices) or 1
            for device in devices:
                started = time.perf_counter()
                device_idx = len(results) + 1
                _notify(12 + int(((device_idx - 1) / total_devices) * 80), f"Проверка {device.name} ({device_idx}/{total_devices})")
                shot_name = f"mobile_{_slug(domain)}_{stamp}_{_slug(device.name)}.png"
                shot_path = shot_dir / shot_name
                context = browser.new_context(
                    viewport={"width": device.width, "height": device.height},
                    device_scale_factor=device.dpr,
                    is_mobile=True,
                    has_touch=True,
                    user_agent=device.user_agent,
                )
                page = context.new_page()
                console_errors = []
                page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
                status_code = None
                final_url = prefetch.get("final_url") or url
                error = None
                eval_data: Dict[str, Any] = {}
                try:
                    response = page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
                    status_code = response.status if response else prefetch.get("status_code")
                    final_url = page.url or final_url
                    page.screenshot(path=str(shot_path), full_page=True)
                    eval_data = self._evaluate_page(page)
                except Exception as e:
                    error = str(e)
                finally:
                    context.close()

                elapsed_ms = int((time.perf_counter() - started) * 1000)
                issues = self._collect_issues(eval_data, prefetch.get("viewport_content"), len(console_errors))
                if error:
                    issues.insert(0, {"severity": "critical", "code": "runtime_error", "title": "Device check failed", "details": error})
                global_issues.extend([{**issue, "device": device.name} for issue in issues])

                mobile_friendly = not any(issue["severity"] in ("critical", "warning") for issue in issues)
                results.append(
                    {
                        "device_name": device.name,
                        "category": device.category,
                        "viewport": {"width": device.width, "height": device.height},
                        "pixel_ratio": device.dpr,
                        "status_code": status_code,
                        "final_url": final_url,
                        "load_time_ms": elapsed_ms,
                        "console_errors_count": len(console_errors),
                        "mobile_friendly": mobile_friendly,
                        "issues_count": len(issues),
                        "issues": issues,
                        "screenshot_path": str(shot_path),
                        "screenshot_name": shot_name,
                        "screenshot_url": f"/api/mobile-artifacts/{task_id}/{shot_name}",
                    }
                )
                _notify(12 + int((device_idx / total_devices) * 80), f"Завершено: {device.name} ({device_idx}/{total_devices})")
            browser.close()

        total = len(results)
        mobile_friendly_devices = sum(1 for r in results if r.get("mobile_friendly"))
        avg_load = round(sum(r.get("load_time_ms", 0) for r in results) / total, 2) if total else 0
        summary = {
            "total_devices": total,
            "mobile_friendly_devices": mobile_friendly_devices,
            "non_friendly_devices": total - mobile_friendly_devices,
            "avg_load_time_ms": avg_load,
            "critical_issues": sum(1 for i in global_issues if i.get("severity") == "critical"),
            "warning_issues": sum(1 for i in global_issues if i.get("severity") == "warning"),
            "info_issues": sum(1 for i in global_issues if i.get("severity") == "info"),
        }
        recommendations = []
        if summary["critical_issues"] > 0:
            recommendations.append("В первую очередь устраните критические проблемы мобильной версии: viewport и горизонтальный скролл.")
        if summary["warning_issues"] > 0:
            recommendations.append("Устраните предупреждения: размеры интерактивных элементов, шрифтов и изображений.")
        if summary["critical_issues"] == 0 and summary["warning_issues"] == 0:
            recommendations.append("Критических проблем мобильной версии не обнаружено.")

        _notify(98, "Формирование итогового отчета")
        return {
            "task_type": "mobile_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "engine": "v2",
                "mode": mode,
                "status_code": prefetch.get("status_code"),
                "final_url": prefetch.get("final_url"),
                "viewport_found": prefetch.get("has_viewport"),
                "viewport_content": prefetch.get("viewport_content"),
                "mobile_friendly": summary["critical_issues"] == 0 and summary["warning_issues"] == 0,
                "score": max(0, 100 - (summary["critical_issues"] * 20 + summary["warning_issues"] * 8)),
                "devices_tested": [r["device_name"] for r in results],
                "device_results": results,
                "issues": global_issues,
                "issues_count": len(global_issues),
                "summary": summary,
                "recommendations": recommendations,
                "artifacts": {
                    "screenshot_dir": str(shot_dir),
                    "screenshots": [r["screenshot_path"] for r in results if r.get("screenshot_path")],
                },
            },
        }
