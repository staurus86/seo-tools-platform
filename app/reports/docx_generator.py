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
import json
import tempfile
from urllib.parse import urljoin

import requests

from app.config import settings


class DOCXGenerator:
    """Генератор Word-отчетов."""
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _add_heading(self, doc, text: str, level: int = 1):
        """Добавляет заголовок."""
        heading = doc.add_heading(self._fix_text(text), level=level)
        heading.alignment = WD_ALIGN_PARAGRAPH.LEFT
        return heading
    
    def _add_table(self, doc, headers: List[str], rows: List[List[Any]]):
        """Добавляет таблицу."""
        table = doc.add_table(rows=1, cols=len(headers))
        table.style = 'Light Grid Accent 1'
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        
        # Header row
        hdr_cells = table.rows[0].cells
        for i, header in enumerate(headers):
            hdr_cells[i].text = self._fix_text(str(header))
            # Make header bold
            for paragraph in hdr_cells[i].paragraphs:
                for run in paragraph.runs:
                    run.font.bold = True
        
        # Data rows
        for row_data in rows:
            row_cells = table.add_row().cells
            for i, value in enumerate(row_data):
                row_cells[i].text = self._fix_text(str(value))
        
        return table

    def _fix_text(self, text: str) -> str:
        """Repair common mojibake from UTF-8<->cp1251 mixups."""
        if not isinstance(text, str) or not text:
            return text

        weird_chars = set("Р‚РѓвЂљС“вЂћвЂ¦вЂ вЂЎв‚¬вЂ°Р‰вЂ№РЉРЊР‹РЏС’вЂвЂ™вЂњвЂќвЂўвЂ“вЂ”в„ўС™вЂєСљСњС›Сџ")

        def _quality(value: str) -> int:
            cyr = sum(1 for ch in value if ("А" <= ch <= "я") or ch in ("Ё", "ё"))
            weird = sum(1 for ch in value if ch in weird_chars)
            c1 = sum(1 for ch in value if 0x80 <= ord(ch) <= 0x9F)
            return cyr - (weird * 4) - (value.count("пїЅ") * 8) - (c1 * 6)

        def _looks_mojibake(value: str) -> bool:
            if any(marker in value for marker in ("вЂ", "в„", "Ѓ", "вЂљ", "в‚¬", "в„ў", "љ", "њ", "ћ", "џ")):
                return True
            if any(0x80 <= ord(ch) <= 0x9F for ch in value):
                return True
            letters = [ch for ch in value if ch.isalpha()]
            if not letters:
                return False
            rs_count = sum(1 for ch in letters if ch in ("Р ", "РЎ"))
            return (rs_count / len(letters)) > 0.28

        def _cp1251_byte_for_char(ch: str) -> int | None:
            code = ord(ch)
            if code <= 0xFF:
                return code
            if ch == "Ё":
                return 0xA8
            if ch == "ё":
                return 0xB8
            if "А" <= ch <= "я":
                return code - 0x350
            try:
                raw = ch.encode("cp1251", errors="strict")
                if len(raw) == 1:
                    return raw[0]
            except Exception:
                return None
            return None

        def _decode_mixed_cp1251_utf8(value: str) -> str | None:
            raw = bytearray()
            for ch in value:
                b = _cp1251_byte_for_char(ch)
                if b is None:
                    return None
                raw.append(b)
            try:
                return raw.decode("utf-8", errors="strict")
            except Exception:
                return None

        repaired = text
        for _ in range(3):
            candidates = []
            converted = _decode_mixed_cp1251_utf8(repaired)
            if converted and converted != repaired:
                candidates.append(converted)
            try:
                latin1 = repaired.encode("latin1", errors="strict").decode("utf-8", errors="strict")
                if latin1 != repaired:
                    candidates.append(latin1)
            except Exception:
                pass
            if not candidates:
                break
            best = max(candidates, key=_quality)
            if _looks_mojibake(repaired) or _quality(best) > (_quality(repaired) + 1):
                repaired = best
                continue
            break
        return repaired

    def _normalize_document_text(self, doc: Document) -> None:
        """Normalize paragraph/table text before saving DOCX."""
        def _fix_paragraph(paragraph) -> None:
            if paragraph.runs:
                for run in paragraph.runs:
                    run.text = self._fix_text(run.text)
            elif paragraph.text:
                paragraph.text = self._fix_text(paragraph.text)

        def _fix_table(table) -> None:
            for row in table.rows:
                for cell in row.cells:
                    for p in cell.paragraphs:
                        _fix_paragraph(p)
                    for nested in cell.tables:
                        _fix_table(nested)

        for p in doc.paragraphs:
            _fix_paragraph(p)
        for t in doc.tables:
            _fix_table(t)

    def _save_document(self, doc: Document, filepath: str) -> None:
        """Normalize text and save DOCX."""
        self._normalize_document_text(doc)
        doc.save(filepath)
    
    def generate_site_analyze_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует клиентский DOCX-отчет общего анализа сайта."""
        doc = Document()

        title = doc.add_heading('Отчет по SEO-анализу сайта', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        doc.add_paragraph(f"Проверено страниц: {data.get('pages_analyzed', 0)}")
        doc.add_paragraph(f"Завершено: {data.get('completed_at', 'н/д')}")
        doc.add_paragraph(
            "Отчет фиксирует текущее техническое состояние сайта и ключевые SEO-риски, "
            "чтобы определить приоритетные доработки и снизить риски потери органического трафика."
        )

        self._add_heading(doc, 'Ключевые результаты', level=1)
        results = data.get('results', {})
        headers = ['Показатель', 'Значение', 'Статус']
        rows = [
            ['Всего страниц', results.get('total_pages', 0), 'OK'],
            ['Статус анализа', results.get('status', 'н/д'), 'OK'],
            ['Сводка', results.get('summary', 'н/д'), 'OK']
        ]
        self._add_table(doc, headers, rows)

        recs = results.get("recommendations", []) or data.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Рекомендации', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Tools Platform: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    
    def generate_robots_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate full robots.txt DOCX report with complete result coverage."""
        doc = Document()

        url = data.get('url', 'н/д')
        results = data.get('results', {}) or {}
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def yes_no(value: Any) -> str:
            return 'Да' if bool(value) else 'Нет'

        title = doc.add_heading('Отчет по аудиту Robots.txt', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        subtitle = doc.add_paragraph('Технические директивы сканирования, профиль рисков и план исправлений')
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Сформирован: {generated_at}")

        self._add_heading(doc, '1. Ключевая сводка', level=1)
        summary_rows = [
            ['robots.txt найден', yes_no(results.get('robots_txt_found'))],
            ['HTTP-статус', str(results.get('status_code', 'н/д'))],
            ['Оценка качества', str(results.get('quality_score', 'н/д'))],
            ['Грейд качества', str(results.get('quality_grade', 'н/д'))],
            ['Готов к продакшн', yes_no(results.get('production_ready'))],
            ['Быстрый статус', str(results.get('quick_status', 'н/д'))],
            ['Размер файла (байт)', str(results.get('content_length', 0))],
            ['Количество строк', str(results.get('lines_count', 0))],
            ['User-agent групп', str(results.get('user_agents', 0))],
            ['Правил Disallow', str(results.get('disallow_rules', 0))],
            ['Правил Allow', str(results.get('allow_rules', 0))],
            ['Объявлено sitemap', str(len(results.get('sitemaps', []) or []))],
            ['Объявлено host', str(len(results.get('hosts', []) or []))],
            ['Директив crawl-delay', str(len(results.get('crawl_delays', {}) or {}))],
            ['Директив clean-param', str(len(results.get('clean_params', []) or []))],
        ]
        self._add_table(doc, ['Метрика', 'Значение'], summary_rows)

        severity = results.get('severity_counts', {}) or {}
        self._add_heading(doc, '2. Обзор критичности', level=1)
        self._add_table(
            doc,
            ['Критично', 'Предупреждение', 'Инфо'],
            [[
                str(severity.get('critical', 0)),
                str(severity.get('warning', 0)),
                str(severity.get('info', 0)),
            ]],
        )

        self._add_heading(doc, '3. Критические проблемы', level=1)
        critical = results.get('critical_issues', []) or results.get('issues', []) or []
        if critical:
            for item in critical:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('Критические проблемы не обнаружены.')

        self._add_heading(doc, '4. Предупреждения', level=1)
        warnings = results.get('warning_issues', []) or results.get('warnings', []) or []
        if warnings:
            for item in warnings:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('Предупреждения не обнаружены.')

        self._add_heading(doc, '5. Информационные заметки', level=1)
        info_issues = results.get('info_issues', []) or []
        if info_issues:
            for item in info_issues:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('Информационные заметки отсутствуют.')

        self._add_heading(doc, '6. Рекомендации', level=1)
        recommendations = results.get('recommendations', []) or []
        if recommendations:
            for item in recommendations:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('Рекомендации не сформированы.')

        self._add_heading(doc, '7. Топ исправлений', level=1)
        top_fixes = results.get('top_fixes', []) or []
        if top_fixes:
            rows = []
            for fix in top_fixes:
                rows.append([
                    str(fix.get('priority', 'medium')).upper(),
                    str(fix.get('title', '')),
                    str(fix.get('why', '')),
                    str(fix.get('action', '')),
                ])
            self._add_table(doc, ['Приоритет', 'Заголовок', 'Почему', 'Действие'], rows)
        else:
            doc.add_paragraph('Приоритизированные исправления не сформированы.')

        self._add_heading(doc, '8. Проверки Sitemap', level=1)
        sitemap_checks = results.get('sitemap_checks', []) or []
        if sitemap_checks:
            rows = []
            for check in sitemap_checks:
                ok = check.get('ok')
                if ok is True:
                    status = 'OK'
                elif ok is False:
                    status = 'ОШИБКА'
                else:
                    status = 'ПРОПУЩЕНО'
                rows.append([
                    str(check.get('url', '')),
                    status,
                    str(check.get('status_code', '')),
                    str(check.get('error', '')),
                ])
            self._add_table(doc, ['URL', 'Статус', 'HTTP', 'Ошибка'], rows)
        else:
            doc.add_paragraph('Проверки sitemap недоступны.')

        self._add_heading(doc, '9. Детали правил групп', level=1)
        groups = results.get('groups_detail', []) or []
        if groups:
            for idx, group in enumerate(groups, start=1):
                uas = ', '.join(group.get('user_agents', []) or [])
                doc.add_paragraph(f"Группа {idx}: {uas}")
                disallow_rows = [
                    [str(item.get('path', '')), str(item.get('line', ''))]
                    for item in (group.get('disallow', []) or [])
                ]
                allow_rows = [
                    [str(item.get('path', '')), str(item.get('line', ''))]
                    for item in (group.get('allow', []) or [])
                ]
                if disallow_rows:
                    doc.add_paragraph('Disallow:')
                    self._add_table(doc, ['Путь', 'Строка'], disallow_rows)
                if allow_rows:
                    doc.add_paragraph('Allow:')
                    self._add_table(doc, ['Путь', 'Строка'], allow_rows)
                if (not disallow_rows) and (not allow_rows):
                    doc.add_paragraph('В этой группе нет правил allow/disallow.')
        else:
            doc.add_paragraph('Детализация групп недоступна.')

        self._add_heading(doc, '10. Синтаксические ошибки', level=1)
        syntax_errors = results.get('syntax_errors', []) or []
        if syntax_errors:
            rows = []
            for err in syntax_errors:
                rows.append([
                    str(err.get('line', '')),
                    str(err.get('error', '')),
                    str(err.get('content', '')),
                ])
            self._add_table(doc, ['Строка', 'Ошибка', 'Содержимое'], rows)
        else:
            doc.add_paragraph('Синтаксические ошибки не обнаружены.')

        self._add_heading(doc, '11. Расширенный анализ', level=1)
        http_status_analysis = results.get('http_status_analysis', {}) or {}
        if http_status_analysis:
            doc.add_paragraph(f"Контекст HTTP-статуса: {http_status_analysis.get('status_code', 'н/д')}")
            for note in (http_status_analysis.get('notes', []) or []):
                doc.add_paragraph(str(note), style='List Bullet')

        unsupported_directives = results.get('unsupported_directives', []) or []
        if unsupported_directives:
            rows = []
            for item in unsupported_directives:
                rows.append([
                    str(item.get('line', '')),
                    str(item.get('directive', '')),
                    str(item.get('value', '')),
                ])
            self._add_table(doc, ['Строка', 'Директива', 'Значение'], rows)

        host_validation = results.get('host_validation', {}) or {}
        if host_validation:
            hosts = ', '.join(host_validation.get('hosts', []) or []) or 'н/д'
            doc.add_paragraph(f"Директивы Host: {hosts}")
            for note in (host_validation.get('warnings', []) or []):
                doc.add_paragraph(str(note), style='List Bullet')

        directive_conflicts = results.get('directive_conflicts', {}) or {}
        conflict_items = directive_conflicts.get('details', []) or []
        if conflict_items:
            rows = []
            for item in conflict_items:
                rows.append([
                    str(item.get('type', '')),
                    str(item.get('user_agent', '')),
                    str(item.get('path', item.get('groups', ''))),
                ])
            self._add_table(doc, ['Тип конфликта', 'User-agent', 'Путь/значение'], rows)

        longest_match = results.get('longest_match_analysis', {}) or {}
        longest_notes = longest_match.get('notes', []) or []
        if longest_notes:
            doc.add_paragraph('Примечания по longest-match:')
            for note in longest_notes:
                doc.add_paragraph(str(note), style='List Bullet')

        param_recommendations = results.get('param_recommendations', []) or []
        if param_recommendations:
            doc.add_paragraph('Рекомендации по Yandex Clean-param:')
            for rec in param_recommendations:
                doc.add_paragraph(str(rec), style='List Bullet')

        self._add_heading(doc, '12. Покрытие по ботам', level=1)
        present_agents = results.get('present_agents', []) or []
        missing_bots = results.get('missing_bots', []) or []
        if present_agents:
            doc.add_paragraph('Обнаруженные группы user-agent:')
            for ua in present_agents:
                doc.add_paragraph(str(ua), style='List Bullet')
        else:
            doc.add_paragraph('Явные группы ботов не обнаружены.')
        if missing_bots:
            doc.add_paragraph('Рекомендуемые боты для явных правил:')
            for bot in missing_bots:
                doc.add_paragraph(str(bot), style='List Bullet')
        else:
            doc.add_paragraph('Key bots coverage looks complete.')

        self._add_heading(doc, '13. Raw robots.txt (line-numbered view)', level=1)
        raw = str(results.get('raw_content', '') or '')
        if raw:
            for idx, line in enumerate(raw.splitlines(), start=1):
                p_line = doc.add_paragraph()
                run_num = p_line.add_run(f"{idx:4} | ")
                run_num.font.size = Pt(8)
                run_num.font.color.rgb = RGBColor(120, 120, 120)
                run_num.font.name = 'Consolas'
                run_txt = p_line.add_run(line)
                run_txt.font.size = Pt(9)
                run_txt.font.name = 'Consolas'
        else:
            doc.add_paragraph('Raw robots.txt content is unavailable.')

        self._add_heading(doc, '14. Additional Fields Snapshot', level=1)
        covered = {
            'robots_txt_found', 'status_code', 'quality_score', 'quality_grade', 'production_ready', 'quick_status',
            'content_length', 'lines_count', 'user_agents', 'disallow_rules', 'allow_rules', 'sitemaps', 'hosts',
            'crawl_delays', 'clean_params', 'severity_counts', 'critical_issues', 'issues', 'warning_issues',
            'warnings', 'info_issues', 'recommendations', 'top_fixes', 'sitemap_checks', 'groups_detail', 'syntax_errors', 'raw_content',
            'http_status_analysis', 'unsupported_directives', 'host_validation', 'directive_conflicts', 'longest_match_analysis',
            'param_recommendations', 'present_agents', 'missing_bots',
            'machine_summary', 'error', 'can_continue',
        }
        extra_rows = []
        for key in sorted(results.keys()):
            if key in covered:
                continue
            value = results.get(key)
            if isinstance(value, (dict, list)):
                rendered = json.dumps(value, ensure_ascii=False)
            else:
                rendered = str(value)
            if len(rendered) > 500:
                rendered = rendered[:500] + ' ...'
            extra_rows.append([key, rendered])
        if extra_rows:
            self._add_table(doc, ['Field', 'Value'], extra_rows)
        else:
            doc.add_paragraph('No additional fields beyond core sections.')

        self._add_heading(doc, '15. Official Documentation', level=1)
        doc.add_paragraph('Google Search Central: https://developers.google.com/search/docs/crawling-indexing/robots/robots_txt')
        doc.add_paragraph('Yandex Webmaster: https://yandex.com/support/webmaster/en/robot-workings/allow-disallow')
        doc.add_paragraph('Yandex Clean-param: https://yandex.com/support/webmaster/en/robot-workings/clean-param')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Сформировано SEO Tools Platform: {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if footer.runs:
            footer.runs[0].font.size = Pt(8)
            footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath

    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate extended sitemap validation DOCX report."""
        doc = Document()
        results = data.get('results', {}) or {}
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        title = doc.add_heading('Отчет по валидации Sitemap', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        input_url = results.get("input_url") or data.get('url', 'н/д')
        resolved_url = results.get("resolved_sitemap_url") or data.get('url', 'н/д')
        discovery_source = results.get("sitemap_discovery_source", "")
        doc.add_paragraph(f"Входной URL: {input_url}")
        doc.add_paragraph(f"URL sitemap: {resolved_url}")
        if discovery_source:
            source_map = {
                "direct_input": "прямой URL",
                "robots.txt": "robots.txt",
                "common_path": "стандартный путь",
                "auto_discovery": "автоопределение",
            }
            doc.add_paragraph(f"Источник обнаружения: {source_map.get(discovery_source, discovery_source)}")
        doc.add_paragraph(f"Сформировано: {now_str}")

        summary_rows = [
            ["Валиден", "Да" if results.get("valid") else "Нет"],
            ["HTTP статус", results.get("status_code", "н/д")],
            ["Sitemap-файлов просканировано", results.get("sitemaps_scanned", 0)],
            ["Валидных sitemap-файлов", results.get("sitemaps_valid", 0)],
            ["Всего URL", results.get("urls_count", 0)],
            ["Уникальных URL", results.get("unique_urls_count", 0)],
            ["Дубли URL", results.get("duplicate_urls_count", 0)],
            ["Оценка качества", f"{results.get('quality_score', 'н/д')} ({results.get('quality_grade', 'н/д')})"],
        ]
        self._add_heading(doc, "1. Сводка", level=1)
        self._add_table(doc, ["Метрика", "Значение"], summary_rows)

        severity = results.get("severity_counts", {}) or {}
        self._add_heading(doc, "2. Распределение по критичности", level=1)
        self._add_table(
            doc,
            ["Критично", "Предупреждение", "Инфо"],
            [[severity.get("critical", 0), severity.get("warning", 0), severity.get("info", 0)]],
        )

        issues = results.get("issues", []) or []
        self._add_heading(doc, "3. Приоритизированные проблемы", level=1)
        if issues:
            rows = []
            for it in issues[:40]:
                rows.append([
                    it.get("severity", ""),
                    it.get("title", ""),
                    it.get("details", ""),
                    it.get("action", ""),
                ])
            self._add_table(doc, ["Критичность", "Проблема", "Детали", "Действие"], rows)
        else:
            doc.add_paragraph("Приоритизированные проблемы отсутствуют.")

        action_plan = results.get("action_plan", []) or []
        self._add_heading(doc, "4. План исправлений", level=1)
        if action_plan:
            rows = []
            for item in action_plan[:30]:
                rows.append([
                    item.get("priority", ""),
                    item.get("owner", ""),
                    item.get("issue", ""),
                    item.get("action", ""),
                    item.get("sla", ""),
                ])
            self._add_table(doc, ["Приоритет", "Ответственный", "Проблема", "Действие", "SLA"], rows)
        else:
            doc.add_paragraph("Пункты плана исправлений отсутствуют.")

        hreflang = results.get("hreflang", {}) or {}
        freshness = results.get("freshness", {}) or {}
        media = results.get("media_extensions", {}) or {}
        self._add_heading(doc, "5. Расширенная валидация", level=1)
        self._add_table(
            doc,
            ["Проверка", "Значение"],
            [
                ["Hreflang обнаружен", "Да" if hreflang.get("detected") else "Нет"],
                ["Hreflang ссылок", hreflang.get("links_count", 0)],
                ["Hreflang: некорректный код", hreflang.get("invalid_code_count", 0)],
                ["Hreflang: некорректный href", hreflang.get("invalid_href_count", 0)],
                ["Hreflang: дубль языка", hreflang.get("duplicate_lang_count", 0)],
                ["Lastmod отсутствует", freshness.get("lastmod_missing_count", 0)],
                ["Lastmod устаревший", freshness.get("stale_lastmod_count", 0)],
                ["Lastmod в будущем", freshness.get("lastmod_future_count", 0)],
                ["Image теги / без loc", f"{media.get('image_tags_count', 0)} / {media.get('image_missing_loc_count', 0)}"],
                ["Video теги / без обязательных", f"{media.get('video_tags_count', 0)} / {media.get('video_missing_required_count', 0)}"],
                ["News теги / без обязательных", f"{media.get('news_tags_count', 0)} / {media.get('news_missing_required_count', 0)}"],
            ],
        )

        sitemap_files = results.get("sitemap_files", []) or []
        self._add_heading(doc, "6. Ошибки и предупреждения по файлам", level=1)
        if sitemap_files:
            rows = []
            for item in sitemap_files[:200]:
                errors_txt = " | ".join((item.get("errors") or [])[:5])
                warnings_txt = " | ".join((item.get("warnings") or [])[:5])
                if not errors_txt and not warnings_txt:
                    continue
                rows.append([
                    item.get("sitemap_url", ""),
                    errors_txt,
                    warnings_txt,
                ])
            if rows:
                self._add_table(doc, ["Sitemap-файл", "Ошибки", "Предупреждения"], rows)
            else:
                doc.add_paragraph("Ошибки или предупреждения по файлам не обнаружены.")
        else:
            doc.add_paragraph("В результате нет sitemap-файлов.")

        tool_notes = results.get("tool_notes", []) or []
        if tool_notes:
            self._add_heading(doc, "7. Служебные заметки (не ошибки sitemap)", level=1)
            for note in tool_notes[:30]:
                doc.add_paragraph(str(note), style='List Bullet')

        checks = results.get("live_indexability_checks", []) or []
        self._add_heading(doc, "8. Live-выборка индексируемости", level=1)
        if checks:
            rows = []
            for item in checks[:20]:
                rows.append([
                    item.get("url", ""),
                    item.get("status_code", ""),
                    "Да" if item.get("indexable") else "Нет",
                    f"{item.get('canonical_status', 'н/д')}: {item.get('canonical_url', '')}",
                    "; ".join(item.get("reasons", [])[:2]),
                ])
            self._add_table(doc, ["URL", "HTTP", "Индексируемость", "Canonical", "Причины"], rows)
        else:
            doc.add_paragraph("Live-выборка пуста.")

        recs = results.get("recommendations", []) or []
        self._add_heading(doc, "9. Рекомендации", level=1)
        if recs:
            for rec in recs[:40]:
                doc.add_paragraph(str(rec), style='List Bullet')
        else:
            doc.add_paragraph("Рекомендации отсутствуют.")

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Сформировано в SEO Tools Platform: {now_str}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    
    def generate_render_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует расширенный клиентский отчет аудита рендеринга."""
        doc = Document()

        title = doc.add_heading('SEO-АУДИТ РЕНДЕРИНГА', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        subtitle = doc.add_paragraph('Анализ контента с JavaScript и без него')
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER

        url = data.get('url', 'н/д')
        results = data.get('results', {}) or {}
        summary = results.get('summary', {}) or {}
        variants = results.get('variants', []) or []
        issues = results.get('issues', []) or []
        recommendations = results.get('recommendations', []) or []
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        doc.add_paragraph(f"Анализируемый URL: {url}")
        doc.add_paragraph(f"Дата аудита: {generated_at}")
        doc.add_paragraph(f"Движок: {results.get('engine', 'legacy')}")
        doc.add_paragraph("Краткое резюме")
        doc.add_paragraph(
            f"Данный отчёт содержит результаты SEO-аудита страницы {url} с фокусом на сравнение контента, "
            "доступного с JavaScript и без него."
        )

        self._add_heading(doc, '1. Ключевые выводы', level=1)
        totals = [
            ['Профилей проверки', summary.get('variants_total', len(variants)), 'Инфо'],
            ['Общая оценка', summary.get('score', 'н/д'), 'Инфо'],
            ['Критичные проблемы', summary.get('critical_issues', 0), 'Важно'],
            ['Предупреждения', summary.get('warning_issues', 0), 'Важно'],
            ['Всего потерянных элементов', summary.get('missing_total', 0), 'Важно'],
            ['Средний процент потерь', f"{summary.get('avg_missing_pct', 0)}%", 'Инфо'],
        ]
        self._add_table(doc, ['Показатель', 'Значение', 'Статус'], totals)
        doc.add_paragraph(f"• SEO-оценка: {summary.get('score', 'н/д')}/100")
        doc.add_paragraph(f"• Критических проблем: {summary.get('critical_issues', 0)}")
        doc.add_paragraph(f"• Предупреждений: {summary.get('warning_issues', 0)}")
        doc.add_paragraph(f"• Добавлено элементов через JS: {summary.get('missing_total', 0)}")

        primary = variants[0] if variants else {}
        raw_p = primary.get('raw', {}) or {}
        js_p = primary.get('rendered', {}) or {}
        meta_p = ((primary.get('meta_non_seo') or {}).get('comparison') or {})
        raw_meta = ((primary.get('meta_non_seo') or {}).get('raw') or {})
        js_meta = ((primary.get('meta_non_seo') or {}).get('rendered') or {})
        raw_schema = ", ".join(raw_p.get('schema_types', []) or []) or "Нет"
        js_schema = ", ".join(js_p.get('schema_types', []) or []) or "Нет"

        def _status(a, b) -> str:
            return "OK" if str(a).strip() == str(b).strip() else "DIFF"

        self._add_heading(doc, '2. Сравнение SEO-элементов', level=1)
        compare_rows = [
            ['Заголовок страницы (title)', raw_p.get('title', ''), js_p.get('title', ''), _status(raw_p.get('title', ''), js_p.get('title', ''))],
            ['Мета-описание (description)', raw_p.get('meta_description', ''), js_p.get('meta_description', ''), _status(raw_p.get('meta_description', ''), js_p.get('meta_description', ''))],
            ['H1 заголовки', f"{raw_p.get('h1_count', 0)} шт", f"{js_p.get('h1_count', 0)} шт", _status(raw_p.get('h1_count', 0), js_p.get('h1_count', 0))],
            ['H2 заголовки', f"{raw_p.get('h2_count', 0)} шт", f"{js_p.get('h2_count', 0)} шт", _status(raw_p.get('h2_count', 0), js_p.get('h2_count', 0))],
            ['Изображения', f"{raw_p.get('images_count', 0)} шт", f"{js_p.get('images_count', 0)} шт", _status(raw_p.get('images_count', 0), js_p.get('images_count', 0))],
            ['Ссылки', f"{raw_p.get('links_count', 0)} шт", f"{js_p.get('links_count', 0)} шт", _status(raw_p.get('links_count', 0), js_p.get('links_count', 0))],
            ['Canonical', raw_p.get('canonical', ''), js_p.get('canonical', ''), _status(raw_p.get('canonical', ''), js_p.get('canonical', ''))],
            ['Schema-разметка', raw_schema, js_schema, _status(raw_schema, js_schema)],
            ['Мета viewport', 'Есть' if raw_meta.get('meta:viewport') else 'Нет', 'Есть' if js_meta.get('meta:viewport') else 'Нет', _status(bool(raw_meta.get('meta:viewport')), bool(js_meta.get('meta:viewport')))],
        ]
        self._add_table(doc, ['Элемент', 'Без JS', 'С JS', 'Статус'], compare_rows)

        self._add_heading(doc, '3. Анализ по устройствам', level=1)
        if variants:
            device_rows = []
            for variant in variants:
                metrics = variant.get('metrics', {}) or {}
                shots = variant.get('screenshots', {}) or {}
                device_rows.append([
                    variant.get('variant_label') or variant.get('variant_id', 'Профиль'),
                    (variant.get('profile_type') or ('mobile' if variant.get('mobile') else 'desktop')),
                    f"{float(metrics.get('score', 0) or 0):.1f}",
                    f"{int(metrics.get('total_missing', 0) or 0)}",
                    ", ".join(list(shots.keys())) if shots else 'нет',
                ])
            self._add_table(doc, ['Устройство', 'Тип профиля', 'Оценка', 'Потери', 'Скриншоты'], device_rows)
        else:
            doc.add_paragraph("Профили устройств отсутствуют.")

        self._add_heading(doc, '4. Что проверялось и зачем', level=1)
        doc.add_paragraph(
            "Система сравнивает две версии страницы: исходный HTML (без JS) и итоговый DOM после JS-рендера. "
            "Если значимый контент появляется только после JS, поисковый робот может увидеть неполную страницу."
        )
        doc.add_paragraph("Проверяются элементы: title, meta description, canonical, H1-H2, ссылки, изображения, schema.org, видимый текст.")
        doc.add_paragraph("Дополнительно сравниваются не-SEO meta-данные между no-JS и JS: viewport, charset, referrer, theme-color, manifest и др.")

        self._add_heading(doc, '5. Результаты по профилям', level=1)
        if variants:
            for variant in variants:
                label = variant.get('variant_label') or variant.get('variant_id', 'Профиль')
                metrics = variant.get('metrics', {}) or {}
                raw = variant.get('raw', {}) or {}
                rendered = variant.get('rendered', {}) or {}
                doc.add_heading(label, level=2)

                rows = [
                    ['Оценка', f"{metrics.get('score', 0):.1f}/100", ''],
                    ['Потерянные элементы', int(metrics.get('total_missing', 0) or 0), ''],
                    ['Потери %', f"{metrics.get('missing_pct', 0):.1f}%", ''],
                    ['H1 (без JS / с JS)', f"{raw.get('h1_count', 0)} / {rendered.get('h1_count', 0)}", ''],
                    ['Ссылки (без JS / с JS)', f"{raw.get('links_count', 0)} / {rendered.get('links_count', 0)}", ''],
                    ['Структурированные данные (без JS / с JS)', f"{raw.get('structured_data_count', 0)} / {rendered.get('structured_data_count', 0)}", ''],
                ]
                self._add_table(doc, ['Параметр', 'Значение', ''], rows)

                seo_required = variant.get('seo_required', {}) or {}
                seo_items = seo_required.get('items', []) or []
                if seo_items:
                    doc.add_paragraph(
                        f"Обязательные SEO-элементы: Пройдено {seo_required.get('pass', 0)}, "
                        f"Предупреждений {seo_required.get('warn', 0)}, Критичных {seo_required.get('fail', 0)}."
                    )
                    seo_rows = []
                    status_map = {'pass': 'Пройдено', 'warn': 'Предупреждение', 'fail': 'Критично'}
                    for item in seo_items:
                        seo_rows.append([
                            item.get('label', ''),
                            item.get('raw', ''),
                            item.get('rendered', ''),
                            status_map.get(item.get('status', ''), item.get('status', '')),
                            item.get('fix', ''),
                        ])
                    self._add_table(doc, ['Элемент', 'Без JS', 'С JS', 'Статус', 'Что исправить'], seo_rows[:80])

                var_issues = variant.get('issues', []) or []
                if var_issues:
                    doc.add_paragraph("Найденные проблемы:", style='List Bullet')
                    for issue in var_issues:
                        sev = str(issue.get('severity', 'info')).upper()
                        title_i = issue.get('title', '')
                        details_i = issue.get('details', '')
                        doc.add_paragraph(f"[{sev}] {title_i}: {details_i}", style='List Bullet')
                        examples = issue.get('examples', []) or []
                        for ex in examples[:5]:
                            doc.add_paragraph(f"Пример: {ex}", style='List Bullet')
                else:
                    doc.add_paragraph("Проблемы не обнаружены.")

                missing = variant.get('missing', {}) or {}
                for key, label in [
                    ('visible_text', 'Текст, который появляется только после JS'),
                    ('headings', 'Заголовки, которые появляются только после JS'),
                    ('links', 'Ссылки, которые появляются только после JS'),
                    ('structured_data', 'Структурированные данные, которые появляются только после JS'),
                ]:
                    values = missing.get(key, []) or []
                    if not values:
                        continue
                    doc.add_paragraph(f"{label}: {len(values)}", style='List Bullet')
                    for item in values[:20]:
                        doc.add_paragraph(str(item), style='List Bullet')

                meta_cmp = ((variant.get('meta_non_seo') or {}).get('comparison') or {})
                meta_items = meta_cmp.get('items', []) or []
                if meta_items:
                    doc.add_paragraph(
                        f"Мета-данные (не SEO): всего {meta_cmp.get('total', 0)}, "
                        f"совпадает {meta_cmp.get('same', 0)}, изменено {meta_cmp.get('changed', 0)}, "
                        f"только JS {meta_cmp.get('only_rendered', 0)}, только без JS {meta_cmp.get('only_raw', 0)}."
                    )
                    meta_rows = []
                    for item in meta_items:
                        status_map = {
                            'same': 'Совпадает',
                            'changed': 'Изменено',
                            'only_rendered': 'Только в JS',
                            'only_raw': 'Только без JS',
                        }
                        meta_rows.append([
                            item.get('key', ''),
                            item.get('raw', ''),
                            item.get('rendered', ''),
                            status_map.get(item.get('status', ''), item.get('status', '')),
                        ])
                    self._add_table(doc, ['Ключ', 'Без JS', 'С JS', 'Статус'], meta_rows[:50])

                var_recs = variant.get('recommendations', []) or []
                if var_recs:
                    doc.add_paragraph("Рекомендации по профилю:", style='List Bullet')
                    for rec in var_recs:
                        doc.add_paragraph(str(rec), style='List Bullet')

                screenshots = variant.get('screenshots', {}) or {}
                for key in ('js', 'nojs', 'js_landscape', 'nojs_landscape'):
                    shot = screenshots.get(key) or {}
                    shot_path = shot.get('path')
                    if shot_path and os.path.exists(shot_path):
                        caption = {
                            'js': 'Скриншот: рендер с JavaScript',
                            'nojs': 'Скриншот: версия без JavaScript',
                            'js_landscape': 'Скриншот: мобильный (горизонтальный), с JavaScript',
                            'nojs_landscape': 'Скриншот: мобильный (горизонтальный), без JavaScript',
                        }.get(key, 'Скриншот')
                        doc.add_paragraph(caption)
                        doc.add_picture(shot_path, width=Inches(6.5))
        else:
            doc.add_paragraph("Данные по профилям отсутствуют.")

        self._add_heading(doc, '6. Общий список ошибок', level=1)
        if issues:
            for issue in issues:
                sev = str(issue.get('severity', 'info')).upper()
                profile = issue.get('variant', 'Профиль')
                title_i = issue.get('title', '')
                details_i = issue.get('details', '')
                doc.add_paragraph(f"[{sev}] {profile}: {title_i} - {details_i}", style='List Bullet')
        else:
            doc.add_paragraph("Ошибок не обнаружено.")

        self._add_heading(doc, '7. Что делать для исправления', level=1)
        if recommendations:
            for rec in recommendations:
                doc.add_paragraph(str(rec), style='List Bullet')
        else:
            doc.add_paragraph("Критичных рекомендаций по итогам проверки нет.")

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    def generate_mobile_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Extended mobile DOCX report with Russian content."""
        doc = Document()

        issue_guides = {
            "viewport_missing": {
                "name": "Отсутствует meta viewport",
                "why": "Страница отображается как десктопная и плохо адаптируется под мобильные экраны.",
                "impact": "Высокое",
                "fix": [
                    "Добавить тег <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"> в <head>.",
                    "Проверить наличие тега на всех шаблонах страниц.",
                ],
            },
            "viewport_invalid": {
                "name": "Некорректный viewport",
                "why": "Неправильные параметры viewport ломают масштаб и адаптивность.",
                "impact": "Высокое",
                "fix": [
                    "Исправить viewport на width=device-width, initial-scale=1.",
                    "Убрать дублирующиеся viewport-теги.",
                ],
            },
            "horizontal_overflow": {
                "name": "Горизонтальная прокрутка",
                "why": "Контент выходит за ширину экрана.",
                "impact": "Высокое",
                "fix": [
                    "Найти элементы с шириной больше viewport.",
                    "Добавить max-width: 100% для медиа-элементов.",
                ],
            },
            "small_touch_targets": {
                "name": "Маленькие интерактивные элементы",
                "why": "Элементы < 44x44px неудобны для тача.",
                "impact": "Среднее",
                "fix": [
                    "Увеличить размеры кнопок/ссылок до 44x44px и больше.",
                    "Добавить отступы между соседними элементами.",
                ],
            },
            "small_fonts": {
                "name": "Слишком мелкий текст",
                "why": "Низкая читаемость ухудшает UX-метрики.",
                "impact": "Среднее",
                "fix": [
                    "Привести базовый размер шрифта к 16px+ на мобильных экранах.",
                    "Проверить читаемость ключевых блоков.",
                ],
            },
            "large_images": {
                "name": "Изображения шире экрана",
                "why": "Широкие изображения ломают сетку и провоцируют скролл.",
                "impact": "Среднее",
                "fix": [
                    "Добавить для изображений max-width: 100%; height: auto;.",
                    "Проверить адаптивность баннеров/слайдеров.",
                ],
            },
            "console_errors": {
                "name": "Ошибки JavaScript в консоли",
                "why": "JS-ошибки могут ломать ключевые UI-сценарии.",
                "impact": "Среднее",
                "fix": [
                    "Разобрать ошибки консоли по приоритету.",
                    "Исправить недоступные ресурсы и исключения.",
                ],
            },
            "runtime_error": {
                "name": "Ошибка выполнения проверки",
                "why": "Часть данных по устройству может быть неполной.",
                "impact": "Высокое",
                "fix": [
                    "Проверить доступность сайта и окружение анализа.",
                    "Повторить аудит после устранения причины.",
                ],
            },
            "playwright_unavailable": {
                "name": "Среда Playwright недоступна",
                "why": "Без браузерного движка нельзя выполнить полный аудит.",
                "impact": "Высокое",
                "fix": [
                    "Установить Playwright и Chromium в окружении сервера.",
                    "Перезапустить сервис и повторить аудит.",
                ],
            },
            "mobile_engine_error": {
                "name": "Сбой движка мобильной проверки",
                "why": "Движок не смог корректно завершить анализ.",
                "impact": "Высокое",
                "fix": [
                    "Проверить логи сервиса и окружение выполнения.",
                    "Устранить причину и повторно запустить аудит.",
                ],
            },
        }

        title = doc.add_heading("Клиентский отчет: мобильная версия сайта", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        url = data.get("url", "н/д")
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        doc.add_paragraph(f"Сайт: {url}")
        doc.add_paragraph(f"Отчет сформирован: {generated_at}")

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        devices = results.get("device_results", []) or []
        all_issues = results.get("issues", []) or []
        artifacts = results.get("artifacts", {}) or {}
        screenshot_dir = str(artifacts.get("screenshot_dir") or "").strip()
        server_base_url = str(data.get("server_base_url") or "").strip()
        actionable_issues = [i for i in all_issues if i.get("severity") in ("critical", "warning")]
        info_issues = [i for i in all_issues if i.get("severity") == "info"]
        temp_screenshots: List[str] = []

        def _resolve_mobile_screenshot_path(device: Dict[str, Any]) -> str:
            candidates: List[str] = []
            raw_path = str(device.get("screenshot_path") or "").strip()
            shot_name = str(device.get("screenshot_name") or "").strip()
            shot_url = str(device.get("screenshot_url") or "").strip()

            if raw_path:
                candidates.append(raw_path)
            if shot_name and screenshot_dir:
                candidates.append(os.path.join(screenshot_dir, shot_name))
            if shot_name:
                candidates.append(os.path.join(self.reports_dir, "mobile", task_id, "screenshots", shot_name))
            if raw_path:
                candidates.append(
                    os.path.join(self.reports_dir, "mobile", task_id, "screenshots", os.path.basename(raw_path))
                )

            for candidate in candidates:
                if candidate and os.path.exists(candidate):
                    return candidate

            if shot_url:
                if shot_url.startswith("/"):
                    if server_base_url:
                        shot_url = urljoin(server_base_url, shot_url)
                    else:
                        shot_url = ""
                if shot_url:
                    try:
                        response = requests.get(shot_url, timeout=25)
                        if response.status_code == 200 and response.content:
                            fd, temp_path = tempfile.mkstemp(prefix=f"mobile_docx_{task_id}_", suffix=".png")
                            os.close(fd)
                            with open(temp_path, "wb") as f:
                                f.write(response.content)
                            temp_screenshots.append(temp_path)
                            return temp_path
                    except Exception:
                        pass
            return ""

        self._add_heading(doc, "1. Сводка по проверке", level=1)
        summary_rows = [
            ["Движок проверки", results.get("engine", "legacy")],
            ["Режим проверки", "Быстрый" if results.get("mode") == "quick" else "Полный"],
            ["Проверено устройств", summary.get("total_devices", len(devices))],
            ["Устройств без критичных проблем", summary.get("mobile_friendly_devices", 0)],
            ["Устройств с проблемами", summary.get("non_friendly_devices", 0)],
            ["Среднее время загрузки, мс", summary.get("avg_load_time_ms", 0)],
            ["Ошибок (critical + warning)", len(actionable_issues)],
            ["Информационных замечаний", len(info_issues)],
            ["Интегральная оценка", results.get("score", "н/д")],
            ["Итог", "Соответствует требованиям" if results.get("mobile_friendly") else "Требуются доработки"],
        ]
        self._add_table(doc, ["Показатель", "Значение"], summary_rows)

        self._add_heading(doc, "2. Технические параметры", level=1)
        tech_rows = [
            ["HTTP", results.get("status_code", "н/д")],
            ["Итоговый URL", results.get("final_url", url)],
            ["Viewport найден", "Да" if results.get("viewport_found") else "Нет"],
            ["Содержимое viewport", results.get("viewport_content") or "-"],
        ]
        self._add_table(doc, ["Параметр", "Значение"], tech_rows)

        self._add_heading(doc, "3. Результаты по устройствам", level=1)
        if devices:
            device_rows = []
            for d in devices:
                category = d.get("category", "")
                if category == "phone":
                    category = "Телефон"
                elif category == "tablet":
                    category = "Планшет"
                device_rows.append([
                    d.get("device_name", ""),
                    category,
                    f"{(d.get('viewport') or {}).get('width', '-')}x{(d.get('viewport') or {}).get('height', '-')}",
                    d.get("status_code", "н/д"),
                    d.get("load_time_ms", 0),
                    d.get("issues_count", 0),
                    "Да" if d.get("mobile_friendly") else "Нет",
                ])
            self._add_table(
                doc,
                ["Устройство", "Тип", "Viewport", "HTTP", "Загрузка, мс", "Ошибок", "ОК для mobile"],
                device_rows,
            )
        else:
            doc.add_paragraph("Данные по устройствам отсутствуют.")

        self._add_heading(doc, "3.1 \u0414\u0435\u0442\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f \u043f\u0440\u043e\u0431\u043b\u0435\u043c \u043f\u043e \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u0430\u043c", level=2)
        detailed_added = 0
        for d in devices:
            device_issues = d.get("issues", []) or []
            if not device_issues:
                continue
            detailed_added += 1
            device_name = d.get("device_name") or "\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e"
            doc.add_paragraph(
                f"{device_name} | "
                f"Viewport {(d.get('viewport') or {}).get('width', '-')}x{(d.get('viewport') or {}).get('height', '-')} | "
                f"\u0417\u0430\u043c\u0435\u0447\u0430\u043d\u0438\u0439: {len(device_issues)}"
            )
            for issue in device_issues[:15]:
                sev = str(issue.get("severity", "info")).upper()
                title_i = str(issue.get("title", issue.get("code", "\u041f\u0440\u043e\u0431\u043b\u0435\u043c\u0430")))
                details_i = str(issue.get("details", "") or "").strip()
                if details_i:
                    doc.add_paragraph(f"[{sev}] {title_i}: {details_i}", style="List Bullet")
                else:
                    doc.add_paragraph(f"[{sev}] {title_i}", style="List Bullet")
        if detailed_added == 0:
            doc.add_paragraph("\u0414\u0435\u0442\u0430\u043b\u0438 \u043f\u043e \u043f\u0440\u043e\u0431\u043b\u0435\u043c\u0430\u043c \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432 \u043e\u0442\u0441\u0443\u0442\u0441\u0442\u0432\u0443\u044e\u0442.")

        self._add_heading(doc, "4. Выявленные ошибки и план исправления", level=1)
        if not actionable_issues:
            doc.add_paragraph("Критические ошибки и предупреждения не обнаружены.")
        else:
            grouped = {}
            for issue in actionable_issues:
                grouped.setdefault(issue.get("code", "unknown"), []).append(issue)

            for idx, (code, items) in enumerate(grouped.items(), start=1):
                guide = issue_guides.get(code, {
                    "name": items[0].get("title", code),
                    "why": "Проблема влияет на качество мобильной версии.",
                    "impact": "Среднее",
                    "fix": ["Проверить верстку и исправить источник ошибки."],
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
                    doc.add_paragraph(step, style="List Number")
                doc.add_paragraph("Технические детали из проверки:")
                for example in items[:5]:
                    detail = str(example.get("details", "") or "").strip()
                    if detail:
                        doc.add_paragraph(f"- {detail}")

        self._add_heading(doc, "5. Информационные наблюдения", level=1)
        if info_issues:
            for issue in info_issues:
                info_title = issue.get("title") or "\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435"
                info_details = issue.get("details", "")
                doc.add_paragraph(
                    f"{info_title}: {info_details}",
                    style="List Bullet",
                )
        else:
            doc.add_paragraph("Дополнительные информационные замечания отсутствуют.")

        self._add_heading(doc, "6. Скриншоты проверенных устройств", level=1)
        added = 0
        missing_files = 0
        for d in devices:
            shot = _resolve_mobile_screenshot_path(d)
            if not shot or not os.path.exists(shot):
                if d.get("screenshot_name") or d.get("screenshot_path"):
                    missing_files += 1
                continue
            device_name = d.get("device_name") or "\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e"
            doc.add_paragraph(
                f"{device_name} | "
                f"Viewport {(d.get('viewport') or {}).get('width', '-')}x{(d.get('viewport') or {}).get('height', '-')} | "
                f"\u0417\u0430\u043c\u0435\u0447\u0430\u043d\u0438\u0439: {d.get('issues_count', 0)}"
            )
            try:
                doc.add_picture(shot, width=Inches(5.8))
                added += 1
            except Exception:
                doc.add_paragraph(f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0432\u0441\u0442\u0440\u043e\u0438\u0442\u044c \u0441\u043a\u0440\u0438\u043d\u0448\u043e\u0442: {shot}")
        if added == 0:
            doc.add_paragraph("Скриншоты отсутствуют.")
            if missing_files > 0:
                doc.add_paragraph(
                    f"\u0424\u0430\u0439\u043b\u044b \u0441\u043a\u0440\u0438\u043d\u0448\u043e\u0442\u043e\u0432 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u044b \u043d\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0435: {missing_files}."
                )

        self._add_heading(doc, "7. Итоги", level=1)
        if actionable_issues:
            doc.add_paragraph(
                "Обнаружены ошибки мобильной версии, которые требуют исправления для повышения "
                "качества UX и стабильности SEO-показателей."
            )
            doc.add_paragraph(
                "Рекомендуется выполнить исправления по приоритету (critical -> warning), затем "
                "повторить аудит и сравнить метрики."
            )
        else:
            doc.add_paragraph(
                "Критичных проблем не обнаружено. Мобильная версия сайта соответствует "
                "базовым требованиям удобства и технической корректности."
            )

        doc.add_paragraph()
        footer = doc.add_paragraph(f"Отчет сформирован SEO Инструменты: {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        try:
            self._save_document(doc, filepath)
            return filepath
        finally:
            for temp_path in temp_screenshots:
                try:
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
    def generate_bot_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate DOCX report for bot accessibility check."""
        doc = Document()
        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        blockers = results.get("priority_blockers", []) or []
        playbooks = results.get("playbooks", []) or []
        baseline_diff = results.get("baseline_diff", {}) or {}
        trend = results.get("trend", {}) or {}
        host_consistency = results.get("host_consistency", {}) or {}
        category_stats = results.get("category_stats", []) or []
        alerts = results.get("alerts", []) or []
        robots_linter = ((results.get("robots_linter", {}) or {}).get("findings") or [])
        allowlist_sim = ((results.get("allowlist_simulator", {}) or {}).get("scenarios") or [])
        action_center = ((results.get("action_center", {}) or {}).get("by_owner") or {})
        evidence_pack = ((results.get("evidence_pack", {}) or {}).get("rows") or [])
        batch_runs = results.get("batch_runs", []) or []

        title = doc.add_heading("Отчет по проверке доступности ботов", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        doc.add_paragraph(f"Сформирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        doc.add_paragraph(f"Профиль ретраев: {results.get('retry_profile', 'standard')}")
        doc.add_paragraph(
            f"Профиль критичности: {results.get('criticality_profile', 'balanced')} | "
            f"Профиль SLA: {results.get('sla_profile', 'standard')}"
        )
        doc.add_paragraph(
            f"Режим AI-политики: {'намеренные блокировки AI' if results.get('ai_block_expected') else 'строгая доступность'} "
            f"(ожидаемо заблокировано: {summary.get('expected_ai_policy_blocked', 0)})"
        )

        self._add_heading(doc, "1. Краткая сводка на одной странице", level=1)
        exec_rows = [
            ["Ботов проверено", len(results.get("bots_checked", []) or [])],
            ["Доступно", summary.get("accessible", 0)],
            ["Сканируемо", summary.get("crawlable", 0)],
            ["Рендерится", summary.get("renderable", 0)],
            ["Индексируемо", summary.get("indexable", 0)],
            ["Не индексируемо", summary.get("non_indexable", 0)],
            ["Сигналы WAF/CDN", summary.get("waf_cdn_detected", 0)],
            ["Средний ответ, мс", summary.get("avg_response_time_ms", 0)],
        ]
        self._add_table(doc, ["Метрика", "Значение"], exec_rows)

        business_risk = "Низкий"
        if (summary.get("non_indexable", 0) or 0) > 0 or (summary.get("waf_cdn_detected", 0) or 0) > 0:
            business_risk = "Средний"
        if (summary.get("non_indexable", 0) or 0) >= max(1, int((summary.get("total", 0) or 0) * 0.3)):
            business_risk = "Высокий"
        doc.add_paragraph(f"Бизнес-риск: {business_risk}")

        self._add_heading(doc, "2. Топ блокеров", level=1)
        if blockers:
            blocker_rows = []
            for item in blockers[:10]:
                blocker_rows.append([
                    item.get("code", ""),
                    item.get("title", ""),
                    item.get("affected_bots", 0),
                    item.get("priority_score", 0),
                    ", ".join(item.get("sample_bots", []) or []),
                ])
            self._add_table(doc, ["Код", "Заголовок", "Затронуто", "Приоритет", "Примеры ботов"], blocker_rows)
        else:
            doc.add_paragraph("Приоритетные блокеры не обнаружены.")

        self._add_heading(doc, "3. План спринта (плейбуки)", level=1)
        if playbooks:
            for idx, item in enumerate(playbooks[:8], start=1):
                doc.add_paragraph(
                    f"{idx}. [{item.get('owner', 'Владелец')}] {item.get('title', '')} "
                    f"(приоритет {item.get('priority_score', 0)})"
                )
                for action in (item.get("actions") or [])[:5]:
                    doc.add_paragraph(str(action), style="List Bullet")
        else:
            doc.add_paragraph("Плейбуки для этого запуска не сформированы.")

        self._add_heading(doc, "4. SLA-матрица категорий", level=1)
        if category_stats:
            rows = []
            for c in category_stats:
                rows.append([
                    c.get("category", ""),
                    c.get("indexable", 0),
                    c.get("total", 0),
                    c.get("indexable_pct", 0),
                    c.get("sla_target_pct", 0),
                    "Да" if c.get("sla_met") else "Нет",
                ])
            self._add_table(doc, ["Категория", "Индексируемо", "Всего", "Индексируемо %", "SLA %", "Выполнено"], rows)
        else:
            doc.add_paragraph("Статистика по категориям недоступна.")

        self._add_heading(doc, "5. Консистентность host", level=1)
        doc.add_paragraph(f"Консистентно: {'Да' if host_consistency.get('consistent', True) else 'Нет'}")
        for note in (host_consistency.get("notes") or []):
            doc.add_paragraph(str(note), style="List Bullet")

        self._add_heading(doc, "6. Сравнение с baseline", level=1)
        if baseline_diff.get("has_baseline"):
            rows = [
                [m.get("metric", ""), m.get("current", ""), m.get("baseline", ""), m.get("delta", "")]
                for m in (baseline_diff.get("metrics") or [])
            ]
            if rows:
                self._add_table(doc, ["Метрика", "Текущее", "Базовое", "Дельта"], rows)
            else:
                doc.add_paragraph("Дельты метрик отсутствуют.")
        else:
            doc.add_paragraph(baseline_diff.get("message", "Baseline не найден."))

        self._add_heading(doc, "7. История тренда", level=1)
        trend_history = trend.get("history", []) or []
        trend_delta = trend.get("delta_vs_previous", {}) or {}
        if trend_history:
            latest = trend.get("latest") or trend_history[0]
            previous = trend.get("previous")
            doc.add_paragraph(
                f"Запусков сохранено для домена: {trend.get('history_count', len(trend_history))}. "
                f"Последний запуск: {latest.get('timestamp', 'н/д')}."
            )
            if previous:
                doc.add_paragraph(f"Предыдущий запуск: {previous.get('timestamp', 'н/д')}.")
                doc.add_paragraph(
                    "Дельта к предыдущему: "
                    f"индексируемо {trend_delta.get('indexable', 0)}, "
                    f"критичных {trend_delta.get('critical_issues', 0)}, "
                    f"средний ответ, мс {trend_delta.get('avg_response_time_ms', 0)}."
                )
            rows = []
            for item in trend_history[:10]:
                rows.append([
                    item.get("timestamp", ""),
                    item.get("indexable", 0),
                    item.get("crawlable", 0),
                    item.get("renderable", 0),
                    item.get("avg_response_time_ms", 0),
                    item.get("critical_issues", 0),
                    item.get("warning_issues", 0),
                ])
            self._add_table(
                doc,
                ["Время запуска", "Индексируемо", "Сканируемо", "Рендерится", "Среднее, мс", "Критично", "Предупреждения"],
                rows,
            )
        else:
            doc.add_paragraph("История тренда недоступна.")

        self._add_heading(doc, "8. Рекомендации", level=1)
        recs = results.get("recommendations", []) or []
        if recs:
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style="List Bullet")
        else:
            doc.add_paragraph("Рекомендации отсутствуют.")

        self._add_heading(doc, "9. Alerts", level=1)
        if alerts:
            for a in alerts[:30]:
                doc.add_paragraph(f"[{str(a.get('severity', 'info')).upper()}] {a.get('code', '')}: {a.get('message', '')}", style="List Bullet")
        else:
            doc.add_paragraph("Активные алерты отсутствуют.")

        self._add_heading(doc, "10. Линтер Robots Policy", level=1)
        if robots_linter:
            rows = [[str(x.get("severity", "")).upper(), x.get("code", ""), x.get("message", "")] for x in robots_linter[:40]]
            self._add_table(doc, ["Критичность", "Код", "Сообщение"], rows)
        else:
            doc.add_paragraph("Линтер robots не выявил замечаний.")

        self._add_heading(doc, "11. Симулятор allowlist", level=1)
        if allowlist_sim:
            rows = []
            for s in allowlist_sim[:20]:
                rows.append([
                    s.get("category", ""),
                    s.get("affected_bots", 0),
                    s.get("delta_renderable", 0),
                    s.get("delta_indexable", 0),
                    s.get("projected_indexable_pct", 0),
                ])
            self._add_table(doc, ["Категория", "Затронуто", "Дельта рендеринга", "Дельта индексации", "Прогноз %"], rows)
        else:
            doc.add_paragraph("Нет данных симуляции.")

        self._add_heading(doc, "12. Центр действий (по владельцам)", level=1)
        if action_center:
            for owner, rows in action_center.items():
                doc.add_paragraph(str(owner))
                for item in (rows or [])[:8]:
                    doc.add_paragraph(f"{item.get('title', '')} (приоритет {item.get('priority_score', 0)})", style="List Bullet")
        else:
            doc.add_paragraph("Группы действий по владельцам отсутствуют.")

        self._add_heading(doc, "13. Пакет доказательств", level=1)
        if evidence_pack:
            rows = []
            for e in evidence_pack[:40]:
                rows.append([
                    e.get("bot", ""),
                    e.get("status", ""),
                    e.get("indexability_reason", ""),
                    f"{'Да' if e.get('waf_detected') else 'Нет'} ({e.get('waf_confidence', 0)})",
                    e.get("waf_reason", ""),
                ])
            self._add_table(doc, ["Бот", "HTTP", "Причина", "WAF", "Причина WAF"], rows)
        else:
            doc.add_paragraph("Строки доказательств отсутствуют.")

        if batch_runs:
            self._add_heading(doc, "14. Пакетные прогоны", level=1)
            rows = []
            for b in batch_runs[:100]:
                rows.append([
                    b.get("url", ""),
                    b.get("indexable", 0),
                    b.get("total", 0),
                    b.get("renderable", 0),
                    b.get("critical_issues", 0),
                    b.get("warning_issues", 0),
                ])
            self._add_table(doc, ["URL", "Индексируемо", "Всего", "Рендерится", "Критично", "Предупреждения"], rows)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath

    def generate_onpage_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate DOCX report for onpage_audit."""
        doc = Document()
        title = doc.add_heading("Отчет OnPage-аудита", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        url = data.get("url", "н/д")
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Сформирован: {generated_at}")
        doc.add_paragraph(f"Engine: {results.get('engine', 'onpage-v1')}")

        self._add_heading(doc, "1. Ключевая сводка", level=1)
        summary_rows = [
            ["Оценка", results.get("score", summary.get("score", 0))],
            ["Spam-оценка", summary.get("spam_score", (results.get("scores", {}) or {}).get("spam_score", 0))],
            ["Оценка покрытия КС", summary.get("keyword_coverage_score", (results.get("scores", {}) or {}).get("keyword_coverage_score", 0))],
            ["Покрытие КС, %", summary.get("keyword_coverage_pct", (results.get("keyword_coverage", {}) or {}).get("coverage_pct", 0))],
            ["AI-риск (композит)", summary.get("ai_risk_composite", (results.get("scores", {}) or {}).get("ai_risk_composite", 0))],
            ["Критические проблемы", summary.get("critical_issues", 0)],
            ["Предупреждения", summary.get("warning_issues", 0)],
            ["Инфо-проблемы", summary.get("info_issues", 0)],
            ["HTTP-статус", results.get("status_code", "н/д")],
            ["Финальный URL", results.get("final_url", url)],
            ["Язык", results.get("language", "auto")],
        ]
        self._add_table(doc, ["Метрика", "Значение"], summary_rows)
        top_risks = sorted((results.get("issues", []) or []), key=lambda x: 0 if x.get("severity") == "critical" else 1)[:5]
        top_actions = (results.get("priority_queue", []) or [])[:5]
        self._add_heading(doc, "1a. Краткая сводка на одной странице", level=2)
        doc.add_paragraph("Топ рисков", style="List Bullet")
        for risk in top_risks:
            doc.add_paragraph(f"{risk.get('severity', '').upper()} | {risk.get('title', '')}", style="List Bullet 2")
        doc.add_paragraph("Топ действий", style="List Bullet")
        for act in top_actions:
                doc.add_paragraph(f"{act.get('bucket', '')}: {act.get('title', '')} (приоритет {act.get('priority_score', 0)})", style="List Bullet 2")

        content = results.get("content", {}) or {}
        content_profile = results.get("content_profile", {}) or {}
        self._add_heading(doc, "2. Метрики контента", level=1)
        content_rows = [
            ["Word count", content.get("word_count", 0)],
            ["Unique words", content.get("unique_word_count", 0)],
            ["Characters", content.get("char_count", 0)],
            ["Clean text length", content_profile.get("clean_text_length", 0)],
            ["Core vocabulary", content_profile.get("core_vocabulary", 0)],
            ["Wateriness %", content_profile.get("wateriness_pct", 0)],
            ["Nausea", content_profile.get("nausea_index", 0)],
            ["Text/HTML %", content_profile.get("text_html_pct", 0)],
        ]
        self._add_table(doc, ["Метрика", "Значение"], content_rows)

        self._add_heading(doc, "3. Meta Tags", level=1)
        title_meta = results.get("title", {}) or {}
        desc_meta = results.get("description", {}) or {}
        h1_meta = results.get("h1", {}) or {}
        meta_rows = [
            ["Title", title_meta.get("text", "")],
            ["Title length", title_meta.get("length", 0)],
            ["Description", desc_meta.get("text", "")],
            ["Description length", desc_meta.get("length", 0)],
            ["H1 count", h1_meta.get("count", 0)],
            ["H1 values", ", ".join(h1_meta.get("values", []) or [])],
        ]
        self._add_table(doc, ["Field", "Value"], meta_rows)

        self._add_heading(doc, "4. Keywords", level=1)
        keyword_rows = []
        for row in (results.get("keywords", []) or [])[:50]:
            keyword_rows.append(
                [
                    row.get("keyword", ""),
                    row.get("occurrences", 0),
                    row.get("density_pct", 0),
                    "Yes" if row.get("in_title") else "No",
                    "Yes" if row.get("in_description") else "No",
                    "Yes" if row.get("in_h1") else "No",
                    str(row.get("status", "ok")).upper(),
                ]
            )
        if keyword_rows:
            self._add_table(doc, ["Ключевое слово", "Частота", "Плотность %", "Title", "Description", "H1", "Статус"], keyword_rows)
        else:
            doc.add_paragraph("Ключевые слова не переданы.")

        self._add_heading(doc, "5. Топ терминов", level=1)
        top_term_rows = []
        for row in (results.get("top_terms", []) or [])[:20]:
            top_term_rows.append([row.get("term", ""), row.get("count", 0), row.get("pct", 0)])
        if top_term_rows:
            self._add_table(doc, ["Термин", "Частота", "Доля %"], top_term_rows)
        else:
            doc.add_paragraph("Топ терминов недоступен.")

        self._add_heading(doc, "6. Technical Signals", level=1)
        technical = results.get("technical", {}) or {}
        technical_rows = [
            ["Canonical href", technical.get("canonical_href", "")],
            ["Canonical self", "Да" if technical.get("canonical_is_self") else "Нет"],
            ["Meta robots", technical.get("robots", "")],
            ["Noindex", "Да" if technical.get("noindex") else "Нет"],
            ["Nofollow", "Да" if technical.get("nofollow") else "Нет"],
            ["Viewport", technical.get("viewport", "")],
            ["HTML lang", technical.get("lang", "")],
            ["Hreflang tags", technical.get("hreflang_count", 0)],
            ["Schema blocks", technical.get("schema_count", 0)],
        ]
        self._add_table(doc, ["Signal", "Value"], technical_rows)

        self._add_heading(doc, "7. Links, Media, Readability", level=1)
        links = results.get("links", {}) or {}
        media = results.get("media", {}) or {}
        readability = results.get("readability", {}) or {}
        quality_rows = [
            ["Total links", links.get("links_total", 0)],
            ["Internal links", links.get("internal_links", 0)],
            ["External links", links.get("external_links", 0)],
            ["Nofollow links", links.get("nofollow_links", 0)],
            ["Empty anchor links", links.get("empty_anchor_links", 0)],
            ["Images total", media.get("images_total", 0)],
            ["Images missing alt", media.get("images_missing_alt", 0)],
            ["Sentences", readability.get("sentences_count", 0)],
            ["Avg sentence len", readability.get("avg_sentence_len", 0)],
            ["Long sentence ratio", readability.get("long_sentence_ratio", 0)],
            ["Lexical diversity", readability.get("lexical_diversity", 0)],
        ]
        spam_metrics = results.get("spam_metrics", {}) or {}
        quality_rows.extend(
            [
                ["Stopword ratio", spam_metrics.get("stopword_ratio", 0)],
                ["Content/HTML ratio", spam_metrics.get("content_html_ratio", 0)],
                ["Uppercase ratio", spam_metrics.get("uppercase_ratio", 0)],
                ["Punctuation ratio", spam_metrics.get("punctuation_ratio", 0)],
                ["Duplicate sentences", spam_metrics.get("duplicate_sentences", 0)],
                ["Duplicate sentence ratio", spam_metrics.get("duplicate_sentence_ratio", 0)],
            ]
        )
        self._add_table(doc, ["Метрика", "Значение"], quality_rows)

        self._add_heading(doc, "8. N-grams", level=1)
        ngrams = results.get("ngrams", {}) or {}
        bigrams = ngrams.get("bigrams", []) or []
        if bigrams:
            bigram_rows = [[row.get("term", ""), row.get("count", 0), row.get("pct", 0)] for row in bigrams[:20]]
            self._add_table(doc, ["Bigram", "Count", "Share %"], bigram_rows)
        else:
            doc.add_paragraph("Биграммы недоступны.")
        trigrams = ngrams.get("trigrams", []) or []
        if trigrams:
            trigram_rows = [[row.get("term", ""), row.get("count", 0), row.get("pct", 0)] for row in trigrams[:20]]
            self._add_table(doc, ["Trigram", "Count", "Share %"], trigram_rows)
        else:
            doc.add_paragraph("Триграммы недоступны.")

        self._add_heading(doc, "9. Schema and OpenGraph", level=1)
        schema = results.get("schema", {}) or {}
        og = results.get("opengraph", {}) or {}
        schema_rows = [
            ["JSON-LD blocks", schema.get("json_ld_blocks", 0)],
            ["Valid JSON-LD", schema.get("json_ld_valid_blocks", 0)],
            ["Microdata items", schema.get("microdata_items", 0)],
            ["RDFa items", schema.get("rdfa_items", 0)],
            ["Schema types", ", ".join([x.get("type", "") for x in (schema.get("types", []) or [])[:10]])],
            ["OpenGraph tags", og.get("tags_count", 0)],
            ["OG required present", og.get("required_present_count", 0)],
            ["OG missing", ", ".join(og.get("required_missing", []) or [])],
        ]
        self._add_table(doc, ["Field", "Value"], schema_rows)

        self._add_heading(doc, "10. AI Signals", level=1)
        ai = results.get("ai_insights", {}) or {}
        ai_rows = [
            ["AI marker density /1k", ai.get("ai_marker_density_1k", 0)],
            ["Hedging ratio", ai.get("hedging_ratio", 0)],
            ["Template repetition /1k", ai.get("template_repetition", 0)],
            ["Burstiness CV", ai.get("burstiness_cv", 0)],
            ["Perplexity proxy", ai.get("perplexity_proxy", 0)],
            ["Entity depth /1k", ai.get("entity_depth_1k", 0)],
            ["Claim specificity score", ai.get("claim_specificity_score", 0)],
            ["Author signal score", ai.get("author_signal_score", 0)],
            ["Source attribution score", ai.get("source_attribution_score", 0)],
            ["AI risk composite", ai.get("ai_risk_composite", 0)],
        ]
        self._add_table(doc, ["Signal", "Value"], ai_rows)

        link_terms = results.get("link_anchor_terms", []) or []
        if link_terms:
            self._add_heading(doc, "11. Топ терминов анкоров", level=1)
            self._add_table(
                doc,
                ["Термин", "Количество"],
                [[row.get("term", ""), row.get("count", 0)] for row in link_terms[:10]],
            )

        self._add_heading(doc, "11a. Тепловая карта критичности", level=2)
        heatmap = results.get("heatmap", {}) or {}
        heatmap_rows = []
        for cat, payload in heatmap.items():
            heatmap_rows.append([cat, payload.get("score", 0), payload.get("issues", 0), payload.get("critical", 0), payload.get("warning", 0)])
        if heatmap_rows:
            self._add_table(doc, ["Категория", "Оценка", "Проблемы", "Критично", "Предупреждение"], heatmap_rows)

        self._add_heading(doc, "11b. Priority Queue", level=2)
        queue = results.get("priority_queue", []) or []
        if queue:
            queue_rows = [[x.get("bucket", ""), x.get("severity", ""), x.get("code", ""), x.get("title", ""), x.get("priority_score", 0), x.get("effort", 0)] for x in queue[:15]]
            self._add_table(doc, ["Этап", "Критичность", "Код", "Проблема", "Приоритет", "Трудозатраты"], queue_rows)

        self._add_heading(doc, "11c. Цели до/после", level=2)
        targets = results.get("targets", []) or []
        if targets:
            target_rows = [[x.get("metric", ""), x.get("current", 0), x.get("target", 0), x.get("delta", 0)] for x in targets]
            self._add_table(doc, ["Метрика", "Текущее", "Цель", "Дельта"], target_rows)

        self._add_heading(doc, "12. Проблемы", level=1)
        issues = results.get("issues", []) or []
        if issues:
            for issue in issues[:80]:
                sev = str(issue.get("severity", "info")).upper()
                title_i = issue.get("title", issue.get("code", "Проблема"))
                details_i = issue.get("details", "")
                doc.add_paragraph(f"[{sev}] {title_i}: {details_i}", style="List Bullet")
        else:
            doc.add_paragraph("Проблемы не обнаружены.")

        self._add_heading(doc, "13. Рекомендации", level=1)
        recs = results.get("recommendations", []) or []
        if recs:
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style="List Bullet")
        else:
            doc.add_paragraph("Рекомендации недоступны.")

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath

    def generate_site_audit_pro_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate compact DOCX report for site_audit_pro."""
        doc = Document()
        title = doc.add_heading("Отчет Site Audit Pro", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        pipeline = results.get("pipeline", {}) or {}
        pipeline_metrics = pipeline.get("metrics", {}) or {}
        pages = results.get("pages", []) or []
        issues = results.get("issues", []) or []
        url = data.get("url", "н/д")

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Режим: {results.get('mode', 'quick')}")
        doc.add_paragraph(f"Сформирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self._add_heading(doc, "1. Ключевая сводка", level=1)
        summary_rows = [
            ["Всего страниц", summary.get("total_pages", 0)],
            ["Всего проблем", summary.get("issues_total", 0)],
            ["Критично", summary.get("critical_issues", 0)],
            ["Предупреждение", summary.get("warning_issues", 0)],
            ["Инфо", summary.get("info_issues", 0)],
            ["Оценка", summary.get("score", "н/д")],
            ["Средний ответ (мс)", pipeline_metrics.get("avg_response_time_ms", 0)],
            ["Средняя читаемость", pipeline_metrics.get("avg_readability_score", 0)],
            ["Среднее качество ссылок", pipeline_metrics.get("avg_link_quality_score", 0)],
            ["Orphan-страницы", pipeline_metrics.get("orphan_pages", 0)],
        ]
        self._add_table(doc, ["Метрика", "Значение"], summary_rows)

        self._add_heading(doc, "2. Топ проблем", level=1)
        top_issues = issues[:20]
        if top_issues:
            issue_rows = []
            for issue in top_issues:
                issue_rows.append([
                    (issue.get("severity") or "info").upper(),
                    issue.get("code", ""),
                    issue.get("url", ""),
                    issue.get("title", ""),
                ])
            self._add_table(doc, ["Критичность", "Код", "URL", "Проблема"], issue_rows)
        else:
            doc.add_paragraph("Проблемы не обнаружены.")

        self._add_heading(doc, "3. Ключевые сигналы ссылочного графа", level=1)
        top_pr = (pipeline.get("pagerank") or [])[:10]
        if top_pr:
            pr_rows = [[row.get("url", ""), row.get("score", 0)] for row in top_pr]
            self._add_table(doc, ["URL", "PageRank"], pr_rows)
        else:
            doc.add_paragraph("Данные PageRank недоступны.")

        self._add_heading(doc, "4. Кластеры тем", level=1)
        clusters = (pipeline.get("topic_clusters") or [])[:20]
        if clusters:
            cluster_rows = [[c.get("topic", "misc"), c.get("count", 0), ", ".join((c.get("urls") or [])[:3])] for c in clusters]
            self._add_table(doc, ["Тема", "Страницы", "Примеры URL"], cluster_rows)
        else:
            doc.add_paragraph("Кластеры тем недоступны.")

        self._add_heading(doc, "5. Рекомендации", level=1)
        recommendations = [p.get("recommendation") for p in pages if p.get("recommendation")]
        if recommendations:
            for rec in recommendations[:20]:
                doc.add_paragraph(str(rec), style="List Bullet")
        else:
            doc.add_paragraph("Рекомендации недоступны.")

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Генерирует отчет в зависимости от типа задачи."""
        generators = {
            'site_analyze': self.generate_site_analyze_report,
            'robots_check': self.generate_robots_report,
            'sitemap_validate': self.generate_sitemap_report,
            'render_audit': self.generate_render_report,
            'mobile_check': self.generate_mobile_report,
            'bot_check': self.generate_bot_report,
            'site_audit_pro': self.generate_site_audit_pro_report,
            'onpage_audit': self.generate_onpage_report,
        }
        
        generator = generators.get(task_type, self.generate_site_analyze_report)
        return generator(task_id, data)


# Singleton
docx_generator = DOCXGenerator()
