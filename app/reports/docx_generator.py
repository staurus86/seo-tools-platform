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
    """Р“РµРЅРµСЂР°С‚ор Word РѕС‚С‡РµС‚ов"""
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _add_heading(self, doc, text: str, level: int = 1):
        """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РіРѕР»овок"""
        heading = doc.add_heading(self._fix_text(text), level=level)
        heading.alignment = WD_ALIGN_PARAGRAPH.LEFT
        return heading
    
    def _add_table(self, doc, headers: List[str], rows: List[List[Any]]):
        """Р”РѕР±Р°РІР»СЏРµС‚ С‚Р°Р±Р»РёС†Сѓ"""
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

        weird_chars = set("ЂЃ‚ѓ„…†‡€‰Љ‹ЊЌЋЏђ‘’“”•–—™љ›њќћџ")

        def _quality(value: str) -> int:
            cyr = sum(1 for ch in value if ("А" <= ch <= "я") or ch in ("Ё", "ё"))
            weird = sum(1 for ch in value if ch in weird_chars)
            return cyr - (weird * 4) - (value.count("�") * 8)

        def _looks_mojibake(value: str) -> bool:
            if any(marker in value for marker in ("вЂ", "в„", "Ѓ", "‚", "€", "™", "љ", "њ", "ћ", "џ")):
                return True
            letters = [ch for ch in value if ch.isalpha()]
            if not letters:
                return False
            rs_count = sum(1 for ch in letters if ch in ("Р", "С"))
            return (rs_count / len(letters)) > 0.28

        repaired = text
        for _ in range(3):
            converted = None
            try:
                converted = repaired.encode("cp1251").decode("utf-8")
            except Exception:
                try:
                    converted = repaired.encode("latin1").decode("utf-8")
                except Exception:
                    break
            if not converted or converted == repaired:
                break
            if _looks_mojibake(repaired) or _quality(converted) > (_quality(repaired) + 1):
                repaired = converted
            else:
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
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РєР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚ Р°РЅР°Р»РёР·Р° СЃР°Р№С‚Р°."""
        doc = Document()

        title = doc.add_heading('РћС‚С‡РµС‚ по SEO-Р°РЅР°Р»РёР·Сѓ СЃР°Р№С‚Р°', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        doc.add_paragraph(f"РџСЂРѕРІРµСЂРµРЅРѕ СЃС‚СЂР°РЅРёС†: {data.get('pages_analyzed', 0)}")
        doc.add_paragraph(f"Completed at: {data.get('completed_at', 'n/a')}")
        doc.add_paragraph(
            "РћРїРёСЃР°РЅРёРµ: РґР°РЅРЅС‹Р№ РѕС‚С‡РµС‚ С„РёРєСЃРёСЂСѓРµС‚ РѕР±С‰РµРµ С‚РµС…РЅРёС‡РµСЃРєРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ СЃР°Р№С‚Р° СЃ С‚РѕС‡РєРё Р·СЂРµРЅРёСЏ SEO, "
            "С‡С‚РѕР±С‹ РѕРїСЂРµРґРµР»РёС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚С‹ РґРѕСЂР°Р±РѕС‚ок Рё СЃРЅРёР·РёС‚СЊ СЂРёСЃРєРё РїРѕС‚РµСЂРё РѕСЂРіР°РЅРёС‡Рµского С‚СЂР°С„РёРєР°."
        )

        self._add_heading(doc, 'РљР»СЋС‡РµРІС‹Рµ СЂРµР·СѓР»СЊС‚Р°С‚С‹', level=1)
        results = data.get('results', {})
        headers = ['РџРѕРєР°Р·Р°С‚РµР»СЊ', 'Р—РЅР°С‡РµРЅРёРµ', 'РЎС‚Р°С‚ус']
        rows = [
            ['Р’СЃРµго СЃС‚СЂР°РЅРёС†', results.get('total_pages', 0), 'OK'],
            ['РЎС‚Р°С‚ус Р°РЅР°Р»РёР·Р°', results.get('status', 'н/д'), 'OK'],
            ['РЎРІРѕРґРєР°', results.get('summary', 'н/д'), 'OK']
        ]
        self._add_table(doc, headers, rows)

        recs = results.get("recommendations", []) or data.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Р РµРєРѕРјРµРЅРґР°С†РёРё', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"РћС‚С‡РµС‚ СЃС„РѕСЂРјРёСЂРѕРІР°РЅ SEO РРЅСЃС‚СЂСѓРјРµРЅС‚С‹: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    
    def generate_robots_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate full robots.txt DOCX report with complete result coverage."""
        doc = Document()

        url = data.get('url', 'n/a')
        results = data.get('results', {}) or {}
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def yes_no(value: Any) -> str:
            return 'Yes' if bool(value) else 'No'

        title = doc.add_heading('Robots.txt Audit Report', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        subtitle = doc.add_paragraph('Technical crawl directives, risk profile, and remediation plan')
        subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Generated: {generated_at}")

        self._add_heading(doc, '1. Executive Summary', level=1)
        summary_rows = [
            ['robots.txt found', yes_no(results.get('robots_txt_found'))],
            ['HTTP status', str(results.get('status_code', 'n/a'))],
            ['Quality score', str(results.get('quality_score', 'n/a'))],
            ['Quality grade', str(results.get('quality_grade', 'n/a'))],
            ['Production ready', yes_no(results.get('production_ready'))],
            ['Quick status', str(results.get('quick_status', 'n/a'))],
            ['File size (bytes)', str(results.get('content_length', 0))],
            ['Lines count', str(results.get('lines_count', 0))],
            ['User-agents', str(results.get('user_agents', 0))],
            ['Disallow rules', str(results.get('disallow_rules', 0))],
            ['Allow rules', str(results.get('allow_rules', 0))],
            ['Sitemaps declared', str(len(results.get('sitemaps', []) or []))],
            ['Hosts declared', str(len(results.get('hosts', []) or []))],
            ['Crawl-delay directives', str(len(results.get('crawl_delays', {}) or {}))],
            ['Clean-param directives', str(len(results.get('clean_params', []) or []))],
        ]
        self._add_table(doc, ['Metric', 'Value'], summary_rows)

        severity = results.get('severity_counts', {}) or {}
        self._add_heading(doc, '2. Severity Overview', level=1)
        self._add_table(
            doc,
            ['Critical', 'Warning', 'Info'],
            [[
                str(severity.get('critical', 0)),
                str(severity.get('warning', 0)),
                str(severity.get('info', 0)),
            ]],
        )

        self._add_heading(doc, '3. Critical Issues', level=1)
        critical = results.get('critical_issues', []) or results.get('issues', []) or []
        if critical:
            for item in critical:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('No critical issues found.')

        self._add_heading(doc, '4. Warnings', level=1)
        warnings = results.get('warning_issues', []) or results.get('warnings', []) or []
        if warnings:
            for item in warnings:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('No warnings found.')

        self._add_heading(doc, '5. Informational Notes', level=1)
        info_issues = results.get('info_issues', []) or []
        if info_issues:
            for item in info_issues:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('No informational notes.')

        self._add_heading(doc, '6. Recommendations', level=1)
        recommendations = results.get('recommendations', []) or []
        if recommendations:
            for item in recommendations:
                doc.add_paragraph(str(item), style='List Bullet')
        else:
            doc.add_paragraph('No recommendations generated.')

        self._add_heading(doc, '7. Top Fixes', level=1)
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
            self._add_table(doc, ['Priority', 'Title', 'Why', 'Action'], rows)
        else:
            doc.add_paragraph('No prioritized fixes generated.')

        self._add_heading(doc, '8. Sitemap Checks', level=1)
        sitemap_checks = results.get('sitemap_checks', []) or []
        if sitemap_checks:
            rows = []
            for check in sitemap_checks:
                ok = check.get('ok')
                if ok is True:
                    status = 'OK'
                elif ok is False:
                    status = 'FAIL'
                else:
                    status = 'SKIPPED'
                rows.append([
                    str(check.get('url', '')),
                    status,
                    str(check.get('status_code', '')),
                    str(check.get('error', '')),
                ])
            self._add_table(doc, ['URL', 'Status', 'HTTP', 'Error'], rows)
        else:
            doc.add_paragraph('No sitemap checks available.')

        self._add_heading(doc, '9. Group Rules Detail', level=1)
        groups = results.get('groups_detail', []) or []
        if groups:
            for idx, group in enumerate(groups, start=1):
                uas = ', '.join(group.get('user_agents', []) or [])
                doc.add_paragraph(f"Group {idx}: {uas}")
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
                    self._add_table(doc, ['Path', 'Line'], disallow_rows)
                if allow_rows:
                    doc.add_paragraph('Allow:')
                    self._add_table(doc, ['Path', 'Line'], allow_rows)
                if (not disallow_rows) and (not allow_rows):
                    doc.add_paragraph('No allow/disallow rules in this group.')
        else:
            doc.add_paragraph('No parsed group details available.')

        self._add_heading(doc, '10. Syntax Errors', level=1)
        syntax_errors = results.get('syntax_errors', []) or []
        if syntax_errors:
            rows = []
            for err in syntax_errors:
                rows.append([
                    str(err.get('line', '')),
                    str(err.get('error', '')),
                    str(err.get('content', '')),
                ])
            self._add_table(doc, ['Line', 'Error', 'Content'], rows)
        else:
            doc.add_paragraph('No syntax errors found.')

        self._add_heading(doc, '11. Extended Analysis', level=1)
        http_status_analysis = results.get('http_status_analysis', {}) or {}
        if http_status_analysis:
            doc.add_paragraph(f"HTTP status context: {http_status_analysis.get('status_code', 'n/a')}")
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
            self._add_table(doc, ['Line', 'Directive', 'Value'], rows)

        host_validation = results.get('host_validation', {}) or {}
        if host_validation:
            hosts = ', '.join(host_validation.get('hosts', []) or []) or 'n/a'
            doc.add_paragraph(f"Host directives: {hosts}")
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
            self._add_table(doc, ['Conflict Type', 'User-agent', 'Path/Value'], rows)

        longest_match = results.get('longest_match_analysis', {}) or {}
        longest_notes = longest_match.get('notes', []) or []
        if longest_notes:
            doc.add_paragraph('Longest-match notes:')
            for note in longest_notes:
                doc.add_paragraph(str(note), style='List Bullet')

        param_recommendations = results.get('param_recommendations', []) or []
        if param_recommendations:
            doc.add_paragraph('Yandex Clean-param recommendations:')
            for rec in param_recommendations:
                doc.add_paragraph(str(rec), style='List Bullet')

        self._add_heading(doc, '12. Bots Coverage', level=1)
        present_agents = results.get('present_agents', []) or []
        missing_bots = results.get('missing_bots', []) or []
        if present_agents:
            doc.add_paragraph('Detected user-agent groups:')
            for ua in present_agents:
                doc.add_paragraph(str(ua), style='List Bullet')
        else:
            doc.add_paragraph('No explicit bot groups detected.')
        if missing_bots:
            doc.add_paragraph('Recommended bots to add explicit rules for:')
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
        footer = doc.add_paragraph(f"Generated by SEO Tools Platform at {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if footer.runs:
            footer.runs[0].font.size = Pt(8)
            footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath

    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РєР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚ по sitemap."""
        doc = Document()

        title = doc.add_heading('РћС‚С‡РµС‚ по РІР°Р»РёРґР°С†РёРё sitemap', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        results = data.get('results', {})
        doc.add_paragraph(f"Р’Р°Р»РёРґРµРЅ: {'Р”Р°' if results.get('valid') else 'РќРµС‚'}")
        doc.add_paragraph(f"РљРѕР»РёС‡РµСЃС‚во URL: {results.get('urls_count', 0)}")
        doc.add_paragraph(
            "РћРїРёСЃР°РЅРёРµ: sitemap РїРѕРјРѕРіР°РµС‚ РїРѕРёСЃРєРѕРІС‹Рј СЃРёСЃС‚РµРјР°Рј Р±С‹СЃС‚СЂРµРµ РЅР°С…РѕРґРёС‚СЊ Рё РїРµСЂРµРѕР±С…РѕРґРёС‚СЊ СЃС‚СЂР°РЅРёС†С‹. "
            "РћС€РёР±РєРё СЃС‚СЂСѓРєС‚СѓСЂС‹ Рё РґСѓР±Р»РµР№ РјРѕРіСѓС‚ СѓС…СѓРґС€Р°С‚СЊ РєР°С‡РµСЃС‚во РёРЅРґРµРєСЃР°С†РёРё."
        )
        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Р РµРєРѕРјРµРЅРґР°С†РёРё', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"РћС‚С‡РµС‚ СЃС„РѕСЂРјРёСЂРѕРІР°РЅ SEO РРЅСЃС‚СЂСѓРјРµРЅС‚С‹: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            return "✅" if str(a).strip() == str(b).strip() else "⚠️"

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
            ['Мета viewport', '✅ Есть' if raw_meta.get('meta:viewport') else '❌ Нет', '✅ Есть' if js_meta.get('meta:viewport') else '❌ Нет', _status(bool(raw_meta.get('meta:viewport')), bool(js_meta.get('meta:viewport')))],
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
                doc.add_paragraph(f"[{sev}] {profile}: {title_i} — {details_i}", style='List Bullet')
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

        url = data.get("url", "n/a")
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
            ["HTTP", results.get("status_code", "n/a")],
            ["Final URL", results.get("final_url", url)],
            ["Viewport найден", "Да" if results.get("viewport_found") else "Нет"],
            ["Viewport content", results.get("viewport_content") or "-"],
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
                    d.get("status_code", "n/a"),
                    d.get("load_time_ms", 0),
                    d.get("issues_count", 0),
                    "Да" if d.get("mobile_friendly") else "Нет",
                ])
            self._add_table(
                doc,
                ["Устройство", "Тип", "Viewport", "HTTP", "Load ms", "Ошибок", "ОК для mobile"],
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
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РєР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚ РїСЂРѕРІРµСЂРєРё Р±РѕС‚ов."""
        doc = Document()

        title = doc.add_heading('РћС‚С‡РµС‚ по РґРѕСЃС‚СѓРїРЅРѕСЃС‚Рё Р±РѕС‚ов', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        results = data.get('results', {})
        bots = results.get('bots_checked', [])
        doc.add_paragraph(
            "РћРїРёСЃР°РЅРёРµ: РѕС‚С‡РµС‚ РїРѕРєР°Р·С‹РІР°РµС‚, РєР°Рє РїРѕРёСЃРєРѕРІС‹Рµ Рё AI-Р±РѕС‚С‹ РІРёРґСЏС‚ СЃР°Р№С‚, "
            "Рё РїРѕРјРѕРіР°РµС‚ РёСЃРєР»СЋС‡РёС‚СЊ РѕРіСЂР°РЅРёС‡РµРЅРёСЏ, РјРµС€Р°СЋС‰РёРµ РёРЅРґРµРєСЃР°С†РёРё."
        )

        self._add_heading(doc, 'РџСЂРѕРІРµСЂРµРЅРЅС‹Рµ Р±РѕС‚С‹', level=1)
        for bot in bots:
            doc.add_paragraph(bot, style='List Bullet')

        recs = results.get("recommendations", [])
        if recs:
            self._add_heading(doc, 'Р РµРєРѕРјРµРЅРґР°С†РёРё', level=1)
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style='List Bullet')

        doc.add_paragraph()
        footer = doc.add_paragraph(f"РћС‚С‡РµС‚ СЃС„РѕСЂРјРёСЂРѕРІР°РЅ SEO РРЅСЃС‚СЂСѓРјРµРЅС‚С‹: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    
    def generate_onpage_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate DOCX report for onpage_audit."""
        doc = Document()
        title = doc.add_heading("OnPage Audit Report", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        url = data.get("url", "n/a")
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Generated: {generated_at}")
        doc.add_paragraph(f"Engine: {results.get('engine', 'onpage-v1')}")

        self._add_heading(doc, "1. Executive Summary", level=1)
        summary_rows = [
            ["Score", results.get("score", summary.get("score", 0))],
            ["Spam score", summary.get("spam_score", (results.get("scores", {}) or {}).get("spam_score", 0))],
            ["Keyword coverage score", summary.get("keyword_coverage_score", (results.get("scores", {}) or {}).get("keyword_coverage_score", 0))],
            ["Keyword coverage %", summary.get("keyword_coverage_pct", (results.get("keyword_coverage", {}) or {}).get("coverage_pct", 0))],
            ["AI risk composite", summary.get("ai_risk_composite", (results.get("scores", {}) or {}).get("ai_risk_composite", 0))],
            ["Critical issues", summary.get("critical_issues", 0)],
            ["Warning issues", summary.get("warning_issues", 0)],
            ["Info issues", summary.get("info_issues", 0)],
            ["HTTP status", results.get("status_code", "n/a")],
            ["Final URL", results.get("final_url", url)],
            ["Language", results.get("language", "auto")],
        ]
        self._add_table(doc, ["Metric", "Value"], summary_rows)
        top_risks = sorted((results.get("issues", []) or []), key=lambda x: 0 if x.get("severity") == "critical" else 1)[:5]
        top_actions = (results.get("priority_queue", []) or [])[:5]
        self._add_heading(doc, "1a. Executive One-Page Summary", level=2)
        doc.add_paragraph("Top Risks", style="List Bullet")
        for risk in top_risks:
            doc.add_paragraph(f"{risk.get('severity', '').upper()} | {risk.get('title', '')}", style="List Bullet 2")
        doc.add_paragraph("Top Actions", style="List Bullet")
        for act in top_actions:
            doc.add_paragraph(f"{act.get('bucket', '')}: {act.get('title', '')} (priority {act.get('priority_score', 0)})", style="List Bullet 2")

        content = results.get("content", {}) or {}
        content_profile = results.get("content_profile", {}) or {}
        self._add_heading(doc, "2. Content Metrics", level=1)
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
        self._add_table(doc, ["Metric", "Value"], content_rows)

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
            self._add_table(doc, ["Keyword", "Count", "Density %", "Title", "Description", "H1", "Status"], keyword_rows)
        else:
            doc.add_paragraph("No keywords provided.")

        self._add_heading(doc, "5. Top Terms", level=1)
        top_term_rows = []
        for row in (results.get("top_terms", []) or [])[:20]:
            top_term_rows.append([row.get("term", ""), row.get("count", 0), row.get("pct", 0)])
        if top_term_rows:
            self._add_table(doc, ["Term", "Count", "Share %"], top_term_rows)
        else:
            doc.add_paragraph("Top terms are not available.")

        self._add_heading(doc, "6. Technical Signals", level=1)
        technical = results.get("technical", {}) or {}
        technical_rows = [
            ["Canonical href", technical.get("canonical_href", "")],
            ["Canonical self", "Yes" if technical.get("canonical_is_self") else "No"],
            ["Meta robots", technical.get("robots", "")],
            ["Noindex", "Yes" if technical.get("noindex") else "No"],
            ["Nofollow", "Yes" if technical.get("nofollow") else "No"],
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
        self._add_table(doc, ["Metric", "Value"], quality_rows)

        self._add_heading(doc, "8. N-grams", level=1)
        ngrams = results.get("ngrams", {}) or {}
        bigrams = ngrams.get("bigrams", []) or []
        if bigrams:
            bigram_rows = [[row.get("term", ""), row.get("count", 0), row.get("pct", 0)] for row in bigrams[:20]]
            self._add_table(doc, ["Bigram", "Count", "Share %"], bigram_rows)
        else:
            doc.add_paragraph("Bigrams are not available.")
        trigrams = ngrams.get("trigrams", []) or []
        if trigrams:
            trigram_rows = [[row.get("term", ""), row.get("count", 0), row.get("pct", 0)] for row in trigrams[:20]]
            self._add_table(doc, ["Trigram", "Count", "Share %"], trigram_rows)
        else:
            doc.add_paragraph("Trigrams are not available.")

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
            self._add_heading(doc, "11. Top Link Terms", level=1)
            self._add_table(
                doc,
                ["Term", "Count"],
                [[row.get("term", ""), row.get("count", 0)] for row in link_terms[:10]],
            )

        self._add_heading(doc, "11a. Severity Heatmap", level=2)
        heatmap = results.get("heatmap", {}) or {}
        heatmap_rows = []
        for cat, payload in heatmap.items():
            heatmap_rows.append([cat, payload.get("score", 0), payload.get("issues", 0), payload.get("critical", 0), payload.get("warning", 0)])
        if heatmap_rows:
            self._add_table(doc, ["Category", "Score", "Issues", "Critical", "Warning"], heatmap_rows)

        self._add_heading(doc, "11b. Priority Queue", level=2)
        queue = results.get("priority_queue", []) or []
        if queue:
            queue_rows = [[x.get("bucket", ""), x.get("severity", ""), x.get("code", ""), x.get("title", ""), x.get("priority_score", 0), x.get("effort", 0)] for x in queue[:15]]
            self._add_table(doc, ["Bucket", "Severity", "Code", "Issue", "Priority", "Effort"], queue_rows)

        self._add_heading(doc, "11c. Before/After Targets", level=2)
        targets = results.get("targets", []) or []
        if targets:
            target_rows = [[x.get("metric", ""), x.get("current", 0), x.get("target", 0), x.get("delta", 0)] for x in targets]
            self._add_table(doc, ["Metric", "Current", "Target", "Delta"], target_rows)

        self._add_heading(doc, "12. Issues", level=1)
        issues = results.get("issues", []) or []
        if issues:
            for issue in issues[:80]:
                sev = str(issue.get("severity", "info")).upper()
                title_i = issue.get("title", issue.get("code", "Issue"))
                details_i = issue.get("details", "")
                doc.add_paragraph(f"[{sev}] {title_i}: {details_i}", style="List Bullet")
        else:
            doc.add_paragraph("Issues not found.")

        self._add_heading(doc, "13. Recommendations", level=1)
        recs = results.get("recommendations", []) or []
        if recs:
            for rec in recs[:30]:
                doc.add_paragraph(str(rec), style="List Bullet")
        else:
            doc.add_paragraph("Recommendations are not available.")

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath

    def generate_site_audit_pro_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate compact DOCX report for site_audit_pro."""
        doc = Document()
        title = doc.add_heading("Site Audit Pro Report", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        pipeline = results.get("pipeline", {}) or {}
        pipeline_metrics = pipeline.get("metrics", {}) or {}
        pages = results.get("pages", []) or []
        issues = results.get("issues", []) or []
        url = data.get("url", "n/a")

        doc.add_paragraph(f"URL: {url}")
        doc.add_paragraph(f"Mode: {results.get('mode', 'quick')}")
        doc.add_paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self._add_heading(doc, "1. Executive Summary", level=1)
        summary_rows = [
            ["Total pages", summary.get("total_pages", 0)],
            ["Issues total", summary.get("issues_total", 0)],
            ["Critical", summary.get("critical_issues", 0)],
            ["Warning", summary.get("warning_issues", 0)],
            ["Info", summary.get("info_issues", 0)],
            ["Score", summary.get("score", "n/a")],
            ["Avg response (ms)", pipeline_metrics.get("avg_response_time_ms", 0)],
            ["Avg readability", pipeline_metrics.get("avg_readability_score", 0)],
            ["Avg link quality", pipeline_metrics.get("avg_link_quality_score", 0)],
            ["Orphan pages", pipeline_metrics.get("orphan_pages", 0)],
        ]
        self._add_table(doc, ["Metric", "Value"], summary_rows)

        self._add_heading(doc, "2. Top Issues", level=1)
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
            self._add_table(doc, ["Severity", "Code", "URL", "Issue"], issue_rows)
        else:
            doc.add_paragraph("Issues not found.")

        self._add_heading(doc, "3. Link Graph Highlights", level=1)
        top_pr = (pipeline.get("pagerank") or [])[:10]
        if top_pr:
            pr_rows = [[row.get("url", ""), row.get("score", 0)] for row in top_pr]
            self._add_table(doc, ["URL", "PageRank"], pr_rows)
        else:
            doc.add_paragraph("PageRank data is not available.")

        self._add_heading(doc, "4. Topic Clusters", level=1)
        clusters = (pipeline.get("topic_clusters") or [])[:20]
        if clusters:
            cluster_rows = [[c.get("topic", "misc"), c.get("count", 0), ", ".join((c.get("urls") or [])[:3])] for c in clusters]
            self._add_table(doc, ["Topic", "Pages", "Sample URLs"], cluster_rows)
        else:
            doc.add_paragraph("Topic clusters are not available.")

        self._add_heading(doc, "5. Recommendations", level=1)
        recommendations = [p.get("recommendation") for p in pages if p.get("recommendation")]
        if recommendations:
            for rec in recommendations[:20]:
                doc.add_paragraph(str(rec), style="List Bullet")
        else:
            doc.add_paragraph("Recommendations are not available.")

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        self._save_document(doc, filepath)
        return filepath
    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РѕС‚С‡РµС‚ РІ Р·Р°РІРёСЃРёРјРѕСЃС‚Рё РѕС‚ С‚РёРїР° Р·Р°РґР°С‡Рё"""
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
