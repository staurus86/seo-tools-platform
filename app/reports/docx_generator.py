"""
Word Report Generator
"""
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from typing import Dict, Any, List
from datetime import datetime
import os

from app.config import settings


class DOCXGenerator:
    """Генератор Word отчетов"""
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _add_heading(self, doc, text: str, level: int = 1):
        """Добавляет заголовок"""
        heading = doc.add_heading(text, level=level)
        heading.alignment = WD_ALIGN_PARAGRAPH.LEFT
        return heading
    
    def _add_table(self, doc, headers: List[str], rows: List[List[Any]]):
        """Добавляет таблицу"""
        table = doc.add_table(rows=1, cols=len(headers))
        table.style = 'Light Grid Accent 1'
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        
        # Header row
        hdr_cells = table.rows[0].cells
        for i, header in enumerate(headers):
            hdr_cells[i].text = header
            # Make header bold
            for paragraph in hdr_cells[i].paragraphs:
                for run in paragraph.runs:
                    run.font.bold = True
        
        # Data rows
        for row_data in rows:
            row_cells = table.add_row().cells
            for i, value in enumerate(row_data):
                row_cells[i].text = str(value)
        
        return table
    
    def generate_site_analyze_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует клиентский отчет анализа сайта."""
        doc = Document()

        title = doc.add_heading('Отчет по SEO-анализу сайта', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"URL: {data.get('url', 'N/A')}")
        doc.add_paragraph(f"Проверено страниц: {data.get('pages_analyzed', 0)}")
        doc.add_paragraph(f"Дата завершения: {data.get('completed_at', 'N/A')}")
        doc.add_paragraph(
            "Описание: данный отчет фиксирует общее техническое состояние сайта с точки зрения SEO, "
            "чтобы определить приоритеты доработок и снизить риски потери органического трафика."
        )

        self._add_heading(doc, 'Ключевые результаты', level=1)
        results = data.get('results', {})
        headers = ['Показатель', 'Значение', 'Статус']
        rows = [
            ['Всего страниц', results.get('total_pages', 0), 'OK'],
            ['Статус анализа', results.get('status', 'N/A'), 'OK'],
            ['Сводка', results.get('summary', 'N/A'), 'OK']
        ]
        self._add_table(doc, headers, rows)

        recs = results.get("recommendations", []) or data.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath
    
    def generate_robots_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует клиентский отчет robots.txt."""
        doc = Document()

        title = doc.add_heading('Отчет по robots.txt', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'N/A')}")
        results = data.get('results', {})
        doc.add_paragraph(f"Файл robots.txt найден: {'Да' if results.get('robots_txt_found') else 'Нет'}")
        doc.add_paragraph(
            "Описание: robots.txt управляет доступом поисковых и сервисных ботов к разделам сайта. "
            "Ошибки в этом файле могут ограничить индексацию важных страниц."
        )
        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath
    
    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует клиентский отчет по sitemap."""
        doc = Document()

        title = doc.add_heading('Отчет по валидации sitemap', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'N/A')}")
        results = data.get('results', {})
        doc.add_paragraph(f"Валиден: {'Да' if results.get('valid') else 'Нет'}")
        doc.add_paragraph(f"Количество URL: {results.get('urls_count', 0)}")
        doc.add_paragraph(
            "Описание: sitemap помогает поисковым системам быстрее находить и переобходить страницы. "
            "Ошибки структуры и дублей могут ухудшать качество индексации."
        )
        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath
    
    def generate_render_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует отчет аудита рендеринга."""
        doc = Document()

        title = doc.add_heading('Отчет аудита рендеринга', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'N/A')}")
        results = data.get('results', {})
        doc.add_paragraph(f"Разница рендера JS/без JS: {'Да' if results.get('js_render_diff') else 'Нет'}")
        doc.add_paragraph(
            "Описание: аудит рендеринга нужен для оценки доступности контента для поисковых систем "
            "в случаях, когда сайт зависит от JavaScript."
        )
        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:20]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath

    def generate_mobile_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует расширенный клиентский отчет по мобильной версии сайта."""
        doc = Document()

        issue_guides = {
            "viewport_missing": {
                "name": "Отсутствует meta viewport",
                "why": "Без viewport страница отображается как десктопная версия, что ухудшает UX и SEO-поведенческие факторы.",
                "impact": "Высокий",
                "fix": [
                    "Добавить в <head> тег: <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">.",
                    "Проверить, что тег присутствует на всех шаблонах страниц.",
                ],
            },
            "viewport_invalid": {
                "name": "Некорректная настройка viewport",
                "why": "Неверные параметры viewport приводят к неправильному масштабу и проблемам с адаптивностью.",
                "impact": "Высокий",
                "fix": [
                    "Исправить content viewport на width=device-width, initial-scale=1.",
                    "Проверить отсутствие конфликтующих мета-тегов viewport.",
                ],
            },
            "horizontal_overflow": {
                "name": "Горизонтальная прокрутка",
                "why": "Пользователи вынуждены прокручивать страницу по горизонтали, что ухудшает конверсию и удобство.",
                "impact": "Высокий",
                "fix": [
                    "Найти блоки, выходящие за ширину экрана (ширина документа больше viewport).",
                    "Использовать адаптивные единицы и ограничить ширину медиа-элементов через max-width: 100%.",
                ],
            },
            "small_touch_targets": {
                "name": "Маленькие кликабельные элементы",
                "why": "Элементы меньше 44x44px затрудняют навигацию с сенсорного экрана.",
                "impact": "Средний",
                "fix": [
                    "Увеличить размеры кнопок/ссылок до минимум 44x44px.",
                    "Добавить отступы между соседними интерактивными элементами.",
                ],
            },
            "small_fonts": {
                "name": "Слишком мелкий текст",
                "why": "Мелкий текст снижает читаемость и увеличивает показатель отказов.",
                "impact": "Средний",
                "fix": [
                    "Установить базовый размер шрифта не менее 16px для мобильных экранов.",
                    "Проверить масштабирование текста в ключевых блоках (меню, карточки, формы).",
                ],
            },
            "large_images": {
                "name": "Изображения шире экрана",
                "why": "Слишком широкие изображения ломают сетку и вызывают горизонтальный скролл.",
                "impact": "Средний",
                "fix": [
                    "Добавить для изображений max-width: 100%; height: auto;.",
                    "Проверить адаптивность слайдеров, баннеров и встраиваемых медиа-блоков.",
                ],
            },
            "console_errors": {
                "name": "Ошибки JavaScript в консоли",
                "why": "JS-ошибки могут ломать меню, формы, фильтры и другие элементы интерфейса.",
                "impact": "Средний",
                "fix": [
                    "Разобрать ошибки консоли в порядке критичности.",
                    "Исправить недоступные ресурсы и исключения в клиентском коде.",
                ],
            },
            "runtime_error": {
                "name": "Ошибка выполнения проверки",
                "why": "Проверка конкретного устройства не завершилась, данные неполные.",
                "impact": "Высокий",
                "fix": [
                    "Проверить доступность сайта, редиректы и блокировки.",
                    "Повторить тест после исправления инфраструктурных ограничений.",
                ],
            },
            "playwright_unavailable": {
                "name": "Среда браузерного тестирования недоступна",
                "why": "Без браузерного движка нельзя получить реальные скриншоты и измерения мобильной верстки.",
                "impact": "Высокий",
                "fix": [
                    "Установить зависимости Playwright и браузер Chromium в окружении сервера.",
                    "Перезапустить сервис и повторить анализ.",
                ],
            },
            "mobile_engine_error": {
                "name": "Сбой движка мобильной проверки",
                "why": "Инструмент не смог выполнить полноценный мобильный аудит.",
                "impact": "Высокий",
                "fix": [
                    "Проверить логи сервиса и окружение выполнения.",
                    "Устранить причину ошибки и повторно запустить проверку.",
                ],
            },
        }

        title = doc.add_heading('Клиентский отчет: мобильная версия сайта', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        url = data.get("url", "N/A")
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        doc.add_paragraph(f"Сайт: {url}")
        doc.add_paragraph(f"Дата и время отчета: {generated_at}")
        doc.add_paragraph(
            "Цель отчета: оценить удобство использования сайта на популярных мобильных устройствах, "
            "выявить технические ошибки адаптивности и предоставить понятный план исправлений для команды разработки."
        )

        results = data.get('results', {}) or {}
        summary = results.get('summary', {}) or {}
        devices = results.get('device_results', []) or []
        all_issues = results.get('issues', []) or []
        actionable_issues = [i for i in all_issues if i.get("severity") in ("critical", "warning")]
        info_issues = [i for i in all_issues if i.get("severity") == "info"]

        self._add_heading(doc, '1. Сводка по проверке', level=1)
        summary_rows = [
            ["Движок проверки", results.get("engine", "legacy")],
            ["Режим проверки", "Быстрый" if results.get("mode") == "quick" else "Полный"],
            ["Проверено устройств", summary.get("total_devices", len(results.get("devices_tested", [])))],
            ["Устройств без критичных проблем", summary.get("mobile_friendly_devices", 0)],
            ["Устройств с проблемами", summary.get("non_friendly_devices", 0)],
            ["Среднее время загрузки, мс", summary.get("avg_load_time_ms", 0)],
            ["Количество ошибок (critical + warning)", len(actionable_issues)],
            ["Информационные замечания", len(info_issues)],
            ["Интегральная оценка", results.get("score", "N/A")],
            ["Итог", "Сайт соответствует мобильным требованиям" if results.get("mobile_friendly") else "Требуются доработки мобильной версии"],
        ]
        self._add_table(doc, ["Показатель", "Значение"], summary_rows)

        self._add_heading(doc, '2. Технические параметры аудита', level=1)
        tech_rows = [
            ["HTTP статус", results.get("status_code", "N/A")],
            ["Финальный URL", results.get("final_url", url)],
            ["Viewport найден", "Да" if results.get("viewport_found") else "Нет"],
            ["Содержимое viewport", results.get("viewport_content") or "Не найдено"],
        ]
        self._add_table(doc, ["Параметр", "Значение"], tech_rows)

        self._add_heading(doc, '3. Результаты по устройствам', level=1)
        device_rows = []
        for d in devices:
            device_rows.append([
                d.get("device_name", ""),
                "Телефон" if d.get("category") == "phone" else ("Планшет" if d.get("category") == "tablet" else d.get("category", "")),
                f"{(d.get('viewport') or {}).get('width', '-') }x{(d.get('viewport') or {}).get('height', '-')}",
                d.get("status_code", "N/A"),
                d.get("load_time_ms", 0),
                d.get("issues_count", 0),
                "Да" if d.get("mobile_friendly") else "Нет",
            ])
        if device_rows:
            self._add_table(
                doc,
                ["Устройство", "Тип", "Viewport", "HTTP", "Загрузка (мс)", "Ошибок", "ОК для mobile"],
                device_rows,
            )
        else:
            doc.add_paragraph("Данные по устройствам отсутствуют.")

        self._add_heading(doc, '4. Выявленные ошибки и план исправления', level=1)
        if not actionable_issues:
            doc.add_paragraph("Критические ошибки и предупреждения не обнаружены.")
        else:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for issue in actionable_issues:
                code = issue.get("code", "unknown")
                grouped.setdefault(code, []).append(issue)

            for idx, (code, items) in enumerate(grouped.items(), start=1):
                guide = issue_guides.get(code, {
                    "name": items[0].get("title", code),
                    "why": "Ошибка влияет на качество мобильного интерфейса и пользовательский опыт.",
                    "impact": "Средний",
                    "fix": ["Проверить верстку и исправить причину ошибки в шаблонах/стилях."],
                })

                self._add_heading(doc, f"4.{idx} {guide['name']}", level=2)
                devices_list = sorted({str(i.get("device", "не указано")) for i in items})
                doc.add_paragraph(f"Серьезность: {items[0].get('severity', 'warning')}")
                doc.add_paragraph(f"Затронуто устройств: {len(devices_list)}")
                doc.add_paragraph(f"Устройства: {', '.join(devices_list)}")
                doc.add_paragraph(f"Почему это важно: {guide['why']}")
                doc.add_paragraph(f"Бизнес-влияние: {guide['impact']}")
                doc.add_paragraph("Что сделать:")
                for step in guide["fix"]:
                    doc.add_paragraph(step, style='List Number')
                doc.add_paragraph("Технические детали из проверки:")
                for example in items[:5]:
                    detail = str(example.get("details", "") or "").strip()
                    if detail:
                        doc.add_paragraph(f"• {detail}")

        self._add_heading(doc, '5. Информационные наблюдения', level=1)
        if info_issues:
            for issue in info_issues:
                doc.add_paragraph(f"{issue.get('title', 'Наблюдение')}: {issue.get('details', '')}", style='List Bullet')
        else:
            doc.add_paragraph("Дополнительные информационные замечания отсутствуют.")

        self._add_heading(doc, '6. Скриншоты проверенных устройств', level=1)
        added = 0
        for d in devices:
            shot = d.get("screenshot_path")
            if not shot or not os.path.exists(shot):
                continue
            doc.add_paragraph(
                f"{d.get('device_name', 'Устройство')} | "
                f"Viewport {(d.get('viewport') or {}).get('width', '-') }x{(d.get('viewport') or {}).get('height', '-') } | "
                f"Ошибок: {d.get('issues_count', 0)}"
            )
            try:
                doc.add_picture(shot, width=Inches(5.8))
                added += 1
            except Exception:
                doc.add_paragraph(f"Не удалось встроить скриншот: {shot}")
        if added == 0:
            doc.add_paragraph("Скриншоты отсутствуют.")

        self._add_heading(doc, '7. Итоги', level=1)
        if actionable_issues:
            doc.add_paragraph(
                "Обнаружены ошибки мобильной версии, которые требуют исправления для повышения "
                "качества пользовательского опыта и стабильности SEO-показателей на мобильном трафике."
            )
            doc.add_paragraph(
                "Рекомендуется выполнить исправления по приоритету (critical -> warning), "
                "после чего провести повторную проверку и зафиксировать улучшение метрик."
            )
        else:
            doc.add_paragraph(
                "Критичных проблем не обнаружено. Мобильная версия сайта в текущей конфигурации "
                "соответствует базовым требованиям удобства и технической корректности."
            )

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath

    def generate_bot_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует клиентский отчет проверки ботов."""
        doc = Document()

        title = doc.add_heading('Отчет по доступности ботов', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'N/A')}")
        results = data.get('results', {})
        bots = results.get('bots_checked', [])
        doc.add_paragraph(
            "Описание: отчет показывает, как поисковые и AI-боты видят сайт, "
            "и помогает исключить ограничения, мешающие индексации."
        )

        self._add_heading(doc, 'Проверенные боты', level=1)
        for bot in bots:
            doc.add_paragraph(bot, style='List Bullet')

        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath
    
    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Генерирует отчет в зависимости от типа задачи"""
        generators = {
            'site_analyze': self.generate_site_analyze_report,
            'robots_check': self.generate_robots_report,
            'sitemap_validate': self.generate_sitemap_report,
            'render_audit': self.generate_render_report,
            'mobile_check': self.generate_mobile_report,
            'bot_check': self.generate_bot_report
        }
        
        generator = generators.get(task_type, self.generate_site_analyze_report)
        return generator(task_id, data)


# Singleton
docx_generator = DOCXGenerator()
