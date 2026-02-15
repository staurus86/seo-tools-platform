"""
Excel РіРµРЅРµСЂР°С‚ор РѕС‚С‡РµС‚ов
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from typing import Dict, Any, List
from datetime import datetime
import os

from app.config import settings


class XLSXGenerator:
    """Р В Р’В Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’ВµР В Р’В Р В РІР‚В¦Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р В РІР‚С™Р В Р’В Р вЂ™Р’В°Р В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚СћР В Р Р‹Р В РІР‚С™ Excel Р В Р’В Р РЋРІР‚СћР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚СћР В Р’В Р В РІР‚В """
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _create_header_style(self):
        """Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СћР В Р’В Р вЂ™Р’В·Р В Р’В Р СћРІР‚ВР В Р’В Р вЂ™Р’В°Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р Р‹Р В РЎвЂњР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚ВР В Р’В Р вЂ™Р’В»Р В Р Р‹Р В Р вЂ° Р В Р’В Р СћРІР‚ВР В Р’В Р вЂ™Р’В»Р В Р Р‹Р В Р РЏ Р В Р’В Р вЂ™Р’В·Р В Р’В Р вЂ™Р’В°Р В Р’В Р РЋРІР‚вЂњР В Р’В Р РЋРІР‚СћР В Р’В Р вЂ™Р’В»Р В Р’В Р РЋРІР‚СћР В Р’В Р В РІР‚В Р В Р’В Р РЋРІР‚СњР В Р’В Р РЋРІР‚СћР В Р’В Р В РІР‚В """
        return {
            'font': Font(bold=True, color='FFFFFF'),
            'fill': PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid'),
            'alignment': Alignment(horizontal='center', vertical='center', wrap_text=True),
            'border': Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        }
    
    def _create_cell_style(self):
        """Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СћР В Р’В Р вЂ™Р’В·Р В Р’В Р СћРІР‚ВР В Р’В Р вЂ™Р’В°Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р Р‹Р В РЎвЂњР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚ВР В Р’В Р вЂ™Р’В»Р В Р Р‹Р В Р вЂ° Р В Р’В Р СћРІР‚ВР В Р’В Р вЂ™Р’В»Р В Р Р‹Р В Р РЏ Р В Р Р‹Р В Р РЏР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р’В Р вЂ™Р’ВµР В Р’В Р РЋРІР‚Сњ"""
        return {
            'border': Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        }
    
    def _apply_style(self, cell, style):
        """Р В Р’В Р РЋРЎСџР В Р Р‹Р В РІР‚С™Р В Р’В Р РЋРІР‚ВР В Р’В Р РЋР’ВР В Р’В Р вЂ™Р’ВµР В Р’В Р В РІР‚В¦Р В Р Р‹Р В Р РЏР В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р Р‹Р В РЎвЂњР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚ВР В Р’В Р вЂ™Р’В»Р В Р Р‹Р В Р вЂ° Р В Р’В Р РЋРІР‚Сњ Р В Р Р‹Р В Р РЏР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р’В Р Р†РІР‚С›РІР‚вЂњР В Р’В Р РЋРІР‚СњР В Р’В Р вЂ™Р’Вµ"""
        if 'font' in style:
            cell.font = style['font']
        if 'fill' in style:
            cell.fill = style['fill']
        if 'alignment' in style:
            cell.alignment = style['alignment']
        if 'border' in style:
            cell.border = style['border']

    def _severity_style(self, severity: str) -> Dict[str, Any]:
        """Return fill/font style for a severity level."""
        sev = (severity or "info").lower()
        styles = {
            "critical": {
                "fill": PatternFill(start_color='F8D7DA', end_color='F8D7DA', fill_type='solid'),
                "font": Font(color='842029', bold=True),
            },
            "warning": {
                "fill": PatternFill(start_color='FFF3CD', end_color='FFF3CD', fill_type='solid'),
                "font": Font(color='664D03', bold=True),
            },
            "info": {
                "fill": PatternFill(start_color='D1ECF1', end_color='D1ECF1', fill_type='solid'),
                "font": Font(color='0C5460'),
            },
            "ok": {
                "fill": PatternFill(start_color='D1E7DD', end_color='D1E7DD', fill_type='solid'),
                "font": Font(color='0F5132', bold=True),
            },
        }
        return styles.get(sev, styles["info"])

    def _apply_severity_cell_style(self, cell, severity: str):
        sev_style = self._severity_style(severity)
        cell.fill = sev_style["fill"]
        cell.font = sev_style["font"]
        cell.alignment = Alignment(horizontal='center', vertical='center')

    def _apply_row_severity_fill(self, ws, row_idx: int, start_col: int, end_col: int, severity: str):
        """Apply severity background to all row cells, preserving existing font/border/alignment."""
        sev_fill = self._severity_style(severity)["fill"]
        for col in range(start_col, end_col + 1):
            ws.cell(row=row_idx, column=col).fill = sev_fill

    def _sitemap_issue_severity(self, item: Dict[str, Any]) -> str:
        """Infer severity for sitemap file-level row."""
        if not item.get("ok", False):
            return "critical"
        if (item.get("status_code") or 0) >= 400:
            return "critical"
        if (item.get("duplicate_count") or 0) > 0:
            return "critical"
        if item.get("errors"):
            return "critical"
        if item.get("warnings"):
            return "warning"
        return "ok"
    
    def generate_site_analyze_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р В Р’В Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’ВµР В Р’В Р В РІР‚В¦Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р В РІР‚С™Р В Р’В Р РЋРІР‚ВР В Р Р‹Р В РІР‚С™Р В Р Р‹Р РЋРІР‚СљР В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р’В Р РЋРІР‚СћР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р’В Р вЂ™Р’В°Р В Р’В Р В РІР‚В¦Р В Р’В Р вЂ™Р’В°Р В Р’В Р вЂ™Р’В»Р В Р’В Р РЋРІР‚ВР В Р’В Р вЂ™Р’В·Р В Р’В Р вЂ™Р’В° Р В Р Р‹Р В РЎвЂњР В Р’В Р вЂ™Р’В°Р В Р’В Р Р†РІР‚С›РІР‚вЂњР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р вЂ™Р’В°"""
        wb = Workbook()
        ws = wb.active
        ws.title = "РђРЅР°Р»РёР· СЃР°Р№С‚Р°"
        
        # Header
        ws['A1'] = 'РћС‚С‡РµС‚ по SEO-Р°РЅР°Р»РёР·Сѓ СЃР°Р№С‚Р°'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        # Basic info
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'н/д')
        ws['A4'] = 'РџСЂРѕРІРµСЂРµРЅРѕ СЃС‚СЂР°РЅРёС†:'
        ws['B4'] = data.get('pages_analyzed', 0)
        ws['A5'] = 'Р”Р°С‚Р° Р·Р°РІРµСЂС€РµРЅРёСЏ:'
        ws['B5'] = data.get('completed_at', 'н/д')
        
        # Results section
        ws['A7'] = 'Р РµР·СѓР»СЊС‚Р°С‚С‹'
        ws['A7'].font = Font(bold=True, size=14)
        
        results = data.get('results', {})
        row = 8
        
        # Headers
        headers = ['РџРѕРєР°Р·Р°С‚РµР»СЊ', 'Р—РЅР°С‡РµРЅРёРµ', 'РЎС‚Р°С‚ус']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=row, column=col, value=header)
            self._apply_style(cell, self._create_header_style())
        
        # Sample data (will be replaced with real data from tools)
        sample_data = [
            ['Р’СЃРµго СЃС‚СЂР°РЅРёС†', results.get('total_pages', 0), 'OK'],
            ['РЎС‚Р°С‚ус', results.get('status', 'н/д'), 'OK'],
            ['РЎРІРѕРґРєР°', results.get('summary', 'н/д'), 'OK']
        ]
        
        for data_row in sample_data:
            row += 1
            for col, value in enumerate(data_row, 1):
                cell = ws.cell(row=row, column=col, value=value)
                self._apply_style(cell, self._create_cell_style())
        
        # Auto-adjust column widths
        for col in range(1, 4):
            ws.column_dimensions[get_column_letter(col)].width = 25
        
        # Save
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_robots_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р В Р’В Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’ВµР В Р’В Р В РІР‚В¦Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р В РІР‚С™Р В Р’В Р РЋРІР‚ВР В Р Р‹Р В РІР‚С™Р В Р Р‹Р РЋРІР‚СљР В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р’В Р РЋРІР‚СћР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў robots.txt"""
        wb = Workbook()
        ws = wb.active
        ws.title = "РџСЂРѕРІРµСЂРєР° Robots"
        
        ws['A1'] = 'РћС‚С‡РµС‚ по robots.txt'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'н/д')
        
        results = data.get('results', {})
        ws['A5'] = 'Р¤Р°Р№Р» robots.txt РЅР°Р№РґРµРЅ:'
        ws['B5'] = 'Р”Р°' if results.get('robots_txt_found') else 'РќРµС‚'
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate a detailed sitemap validation XLSX report."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()
        results = data.get('results', {}) or {}
        report_url = data.get('url', 'н/д')

        ws = wb.active
        ws.title = "РЎРІРѕРґРєР°"
        ws['A1'] = 'РћС‚С‡РµС‚ по РІР°Р»РёРґР°С†РёРё sitemap'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:E1')

        summary_rows = [
            ("URL", report_url),
            ("Р’Р°Р»РёРґРµРЅ", "Р”Р°" if results.get("valid") else "РќРµС‚"),
            ("HTTP СЃС‚Р°С‚ус", results.get("status_code", "н/д")),
            ("РџСЂРѕРІРµСЂРµРЅРѕ sitemap", results.get("sitemaps_scanned", 0)),
            ("Р’Р°Р»РёРґРЅС‹С… sitemap", results.get("sitemaps_valid", 0)),
            ("Р’СЃРµго URL", results.get("urls_count", 0)),
            ("РЈРЅРёРєР°Р»СЊРЅС‹С… URL", results.get("unique_urls_count", 0)),
            ("Р”СѓР±Р»Рё URL", results.get("duplicate_urls_count", 0)),
            ("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Рµ URL", results.get("invalid_urls_count", 0)),
            ("РћС€РёР±РєРё lastmod", results.get("invalid_lastmod_count", 0)),
            ("РћС€РёР±РєРё changefreq", results.get("invalid_changefreq_count", 0)),
            ("РћС€РёР±РєРё priority", results.get("invalid_priority_count", 0)),
            ("Р Р°Р·РјРµСЂ РґР°РЅРЅС‹С… (Р±Р°Р№С‚)", results.get("size", 0)),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws.column_dimensions['A'].width = 28
        ws.column_dimensions['B'].width = 80

        files_ws = wb.create_sheet("Р¤Р°Р№Р»С‹ Sitemap")
        files_headers = [
            "Sitemap URL", "РўРёРї", "HTTP", "OK", "URL",
            "Р”СѓР±Р»Рё", "Р Р°Р·РјРµСЂ (Р±Р°Р№С‚)", "РћС€РёР±РєРё", "РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ", "РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ"
        ]
        for col, header in enumerate(files_headers, 1):
            cell = files_ws.cell(row=1, column=col, value=header)
            self._apply_style(cell, header_style)

        for row_idx, item in enumerate((results.get("sitemap_files", []) or []), start=2):
            severity = self._sitemap_issue_severity(item)
            values = [
                item.get("sitemap_url", ""),
                item.get("type", ""),
                item.get("status_code", ""),
                "Р”Р°" if item.get("ok") else "РќРµС‚",
                item.get("urls_count", 0),
                item.get("duplicate_count", 0),
                item.get("size_bytes", 0),
                " | ".join(item.get("errors", [])[:5]),
                " | ".join(item.get("warnings", [])[:5]),
                severity.capitalize(),
            ]
            for col, value in enumerate(values, 1):
                cell = files_ws.cell(row=row_idx, column=col, value=value)
                self._apply_style(cell, cell_style)
            self._apply_row_severity_fill(files_ws, row_idx, 1, len(files_headers), severity)
            self._apply_severity_cell_style(files_ws.cell(row=row_idx, column=len(files_headers)), severity)
        files_ws.freeze_panes = "A2"
        files_ws.auto_filter.ref = f"A1:{get_column_letter(len(files_headers))}1"
        for col, width in enumerate([72, 14, 10, 8, 12, 12, 14, 60, 60, 14], 1):
            files_ws.column_dimensions[get_column_letter(col)].width = width

        errors_ws = wb.create_sheet("РћС€РёР±РєРё")
        errors_ws.cell(row=1, column=1, value="РћС€РёР±РєР°")
        errors_ws.cell(row=1, column=2, value="РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ")
        self._apply_style(errors_ws.cell(row=1, column=1), header_style)
        self._apply_style(errors_ws.cell(row=1, column=2), header_style)
        for idx, err in enumerate((results.get("errors", []) or []), start=2):
            err_cell = errors_ws.cell(row=idx, column=1, value=err)
            sev_cell = errors_ws.cell(row=idx, column=2, value="РљСЂРёС‚РёС‡РЅРѕ")
            self._apply_style(err_cell, cell_style)
            self._apply_style(sev_cell, cell_style)
            self._apply_row_severity_fill(errors_ws, idx, 1, 2, "critical")
            self._apply_severity_cell_style(sev_cell, "critical")
        errors_ws.column_dimensions['A'].width = 140
        errors_ws.column_dimensions['B'].width = 14
        errors_ws.freeze_panes = "A2"
        errors_ws.auto_filter.ref = "A1:B1"

        warnings_ws = wb.create_sheet("РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ")
        warnings_ws.cell(row=1, column=1, value="РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёРµ")
        warnings_ws.cell(row=1, column=2, value="РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ")
        self._apply_style(warnings_ws.cell(row=1, column=1), header_style)
        self._apply_style(warnings_ws.cell(row=1, column=2), header_style)
        for idx, warn in enumerate((results.get("warnings", []) or []), start=2):
            warn_cell = warnings_ws.cell(row=idx, column=1, value=warn)
            sev_cell = warnings_ws.cell(row=idx, column=2, value="РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёРµ")
            self._apply_style(warn_cell, cell_style)
            self._apply_style(sev_cell, cell_style)
            self._apply_row_severity_fill(warnings_ws, idx, 1, 2, "warning")
            self._apply_severity_cell_style(sev_cell, "warning")
        warnings_ws.column_dimensions['A'].width = 140
        warnings_ws.column_dimensions['B'].width = 14
        warnings_ws.freeze_panes = "A2"
        warnings_ws.auto_filter.ref = "A1:B1"

        dup_ws = wb.create_sheet("Duplicates")
        dup_headers = ["URL", "РџРµСЂРІС‹Р№ sitemap", "Р”СѓР±Р»РёРєР°С‚ РІ sitemap", "РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ"]
        for col, header in enumerate(dup_headers, 1):
            cell = dup_ws.cell(row=1, column=col, value=header)
            self._apply_style(cell, header_style)
        for row_idx, item in enumerate((results.get("duplicate_details", []) or []), start=2):
            dup_ws.cell(row=row_idx, column=1, value=item.get("url", ""))
            dup_ws.cell(row=row_idx, column=2, value=item.get("first_sitemap", ""))
            dup_ws.cell(row=row_idx, column=3, value=item.get("duplicate_sitemap", ""))
            dup_ws.cell(row=row_idx, column=4, value="РљСЂРёС‚РёС‡РЅРѕ")
            for col in range(1, 5):
                self._apply_style(dup_ws.cell(row=row_idx, column=col), cell_style)
            self._apply_row_severity_fill(dup_ws, row_idx, 1, 4, "critical")
            self._apply_severity_cell_style(dup_ws.cell(row=row_idx, column=4), "critical")
        dup_ws.freeze_panes = "A2"
        dup_ws.auto_filter.ref = "A1:D1"
        dup_ws.column_dimensions['A'].width = 80
        dup_ws.column_dimensions['B'].width = 60
        dup_ws.column_dimensions['C'].width = 60
        dup_ws.column_dimensions['D'].width = 14

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_render_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует детальный XLSX-отчет по аудиту рендеринга (фокус на проблемах)."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()

        results = data.get('results', {}) or {}
        summary = results.get('summary', {}) or {}
        variants = results.get('variants', []) or []
        issues = results.get('issues', []) or []
        recommendations = results.get('recommendations', []) or []

        ws = wb.active
        ws.title = 'Сводка'
        ws['A1'] = 'Отчет аудита рендеринга (JS и без JS)'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:E1')

        rows = [
            ('Адрес URL', data.get('url', 'н/д')),
            ('Движок', results.get('engine', 'legacy')),
            ('Профилей', summary.get('variants_total', len(variants))),
            ('Оценка', summary.get('score', 'н/д')),
            ('Критичные', summary.get('critical_issues', 0)),
            ('Предупреждения', summary.get('warning_issues', 0)),
            ('Потерянных элементов всего', summary.get('missing_total', 0)),
            ('Потери средний %', summary.get('avg_missing_pct', 0)),
            ('Ср. загрузка без JS (мс)', summary.get('avg_raw_load_ms', 0)),
            ('Ср. загрузка JS (мс)', summary.get('avg_js_load_ms', 0)),
        ]
        row_num = 3
        for key, value in rows:
            ws.cell(row=row_num, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row_num, column=2, value=value)
            row_num += 1
        ws.column_dimensions['A'].width = 32
        ws.column_dimensions['B'].width = 80

        variant_ws = wb.create_sheet('Профили')
        headers = ['Профиль', 'Оценка', 'Потери', 'Потери %', 'H1 без JS', 'H1 с JS', 'Ссылки без JS', 'Ссылки с JS', 'Структурированные данные без JS', 'Структурированные данные с JS']
        for col, header in enumerate(headers, 1):
            self._apply_style(variant_ws.cell(row=1, column=col, value=header), header_style)

        for row_idx, variant in enumerate(variants, start=2):
            metrics = variant.get('metrics', {}) or {}
            raw = variant.get('raw', {}) or {}
            rendered = variant.get('rendered', {}) or {}
            values = [
                variant.get('variant_label') or variant.get('variant_id', ''),
                float(metrics.get('score', 0.0) or 0.0),
                int(metrics.get('total_missing', 0) or 0),
                float(metrics.get('missing_pct', 0.0) or 0.0),
                int(raw.get('h1_count', 0) or 0),
                int(rendered.get('h1_count', 0) or 0),
                int(raw.get('links_count', 0) or 0),
                int(rendered.get('links_count', 0) or 0),
                int(raw.get('structured_data_count', 0) or 0),
                int(rendered.get('structured_data_count', 0) or 0),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(variant_ws.cell(row=row_idx, column=col, value=value), cell_style)

            score = float(metrics.get('score', 0.0) or 0.0)
            severity = 'ok' if score >= 80 else ('warning' if score >= 60 else 'critical')
            self._apply_row_severity_fill(variant_ws, row_idx, 1, len(headers), severity)
            self._apply_severity_cell_style(variant_ws.cell(row=row_idx, column=2), severity)

        variant_ws.freeze_panes = 'A2'
        variant_ws.auto_filter.ref = f"A1:J{max(2, len(variants)+1)}"
        for col, width in enumerate([28, 12, 12, 12, 12, 10, 14, 10, 14, 10], 1):
            variant_ws.column_dimensions[get_column_letter(col)].width = width

        issues_ws = wb.create_sheet('Ошибки')
        issue_headers = ['Серьезность', 'Профиль', 'Код', 'Заголовок', 'Детали']
        for col, header in enumerate(issue_headers, 1):
            self._apply_style(issues_ws.cell(row=1, column=col, value=header), header_style)

        for row_idx, issue in enumerate(issues, start=2):
            severity = (issue.get('severity') or 'info').lower()
            values = [
                severity.upper(),
                issue.get('variant', ''),
                issue.get('code', ''),
                issue.get('title', ''),
                issue.get('details', ''),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(issues_ws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(issues_ws, row_idx, 1, len(issue_headers), severity)
            self._apply_severity_cell_style(issues_ws.cell(row=row_idx, column=1), severity)

        issues_ws.freeze_panes = 'A2'
        issues_ws.auto_filter.ref = f"A1:E{max(2, len(issues)+1)}"
        for col, width in enumerate([12, 24, 24, 34, 96], 1):
            issues_ws.column_dimensions[get_column_letter(col)].width = width

        rec_ws = wb.create_sheet('Рекомендации')
        self._apply_style(rec_ws.cell(row=1, column=1, value='Рекомендация'), header_style)
        for idx, text in enumerate(recommendations, start=2):
            self._apply_style(rec_ws.cell(row=idx, column=1, value=text), cell_style)
        rec_ws.column_dimensions['A'].width = 160
        rec_ws.freeze_panes = 'A2'
        rec_ws.auto_filter.ref = 'A1:A1'

        missing_ws = wb.create_sheet('Потерянные элементы')
        missing_headers = ['Профиль', 'Категория', 'Элемент']
        for col, header in enumerate(missing_headers, 1):
            self._apply_style(missing_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        for variant in variants:
            profile = variant.get('variant_label') or variant.get('variant_id', '')
            missing = variant.get('missing', {}) or {}
            for key, label in [
                ('visible_text', 'Текст только в JS'),
                ('headings', 'Заголовки только в JS'),
                ('links', 'Ссылки только в JS'),
                ('structured_data', 'Структурированные данные только в JS'),
            ]:
                values = missing.get(key, []) or []
                for value in values:
                    self._apply_style(missing_ws.cell(row=row_idx, column=1, value=profile), cell_style)
                    self._apply_style(missing_ws.cell(row=row_idx, column=2, value=label), cell_style)
                    self._apply_style(missing_ws.cell(row=row_idx, column=3, value=str(value)), cell_style)
                    row_idx += 1
        missing_ws.freeze_panes = 'A2'
        missing_ws.auto_filter.ref = f"A1:C{max(2, row_idx-1)}"
        missing_ws.column_dimensions['A'].width = 24
        missing_ws.column_dimensions['B'].width = 28
        missing_ws.column_dimensions['C'].width = 140

        meta_ws = wb.create_sheet('Мета (не SEO)')
        meta_headers = ['Профиль', 'Ключ', 'Без JS', 'С JS', 'Статус']
        for col, header in enumerate(meta_headers, 1):
            self._apply_style(meta_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        status_map = {
            'same': 'Совпадает',
            'changed': 'Изменено',
            'only_rendered': 'Только в JS',
            'only_raw': 'Только без JS',
        }
        for variant in variants:
            profile = variant.get('variant_label') or variant.get('variant_id', '')
            comparison = ((variant.get('meta_non_seo') or {}).get('comparison') or {})
            for item in (comparison.get('items') or []):
                self._apply_style(meta_ws.cell(row=row_idx, column=1, value=profile), cell_style)
                self._apply_style(meta_ws.cell(row=row_idx, column=2, value=item.get('key', '')), cell_style)
                self._apply_style(meta_ws.cell(row=row_idx, column=3, value=item.get('raw', '')), cell_style)
                self._apply_style(meta_ws.cell(row=row_idx, column=4, value=item.get('rendered', '')), cell_style)
                self._apply_style(
                    meta_ws.cell(row=row_idx, column=5, value=status_map.get(item.get('status', ''), item.get('status', ''))),
                    cell_style,
                )
                row_idx += 1
        meta_ws.freeze_panes = 'A2'
        meta_ws.auto_filter.ref = f"A1:E{max(2, row_idx-1)}"
        meta_ws.column_dimensions['A'].width = 24
        meta_ws.column_dimensions['B'].width = 36
        meta_ws.column_dimensions['C'].width = 80
        meta_ws.column_dimensions['D'].width = 80
        meta_ws.column_dimensions['E'].width = 18

        seo_ws = wb.create_sheet('SEO обязательные')
        seo_headers = ['Профиль', 'Элемент', 'Без JS', 'С JS', 'Статус', 'Что исправить']
        for col, header in enumerate(seo_headers, 1):
            self._apply_style(seo_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        status_map = {'pass': 'Пройдено', 'warn': 'Предупреждение', 'fail': 'Критично'}
        for variant in variants:
            profile = variant.get('variant_label') or variant.get('variant_id', '')
            for item in ((variant.get('seo_required') or {}).get('items') or []):
                self._apply_style(seo_ws.cell(row=row_idx, column=1, value=profile), cell_style)
                self._apply_style(seo_ws.cell(row=row_idx, column=2, value=item.get('label', '')), cell_style)
                self._apply_style(seo_ws.cell(row=row_idx, column=3, value=str(item.get('raw', ''))), cell_style)
                self._apply_style(seo_ws.cell(row=row_idx, column=4, value=str(item.get('rendered', ''))), cell_style)
                status = status_map.get(item.get('status', ''), item.get('status', ''))
                self._apply_style(seo_ws.cell(row=row_idx, column=5, value=status), cell_style)
                self._apply_style(seo_ws.cell(row=row_idx, column=6, value=item.get('fix', '')), cell_style)
                sev = 'critical' if item.get('status') == 'fail' else ('warning' if item.get('status') == 'warn' else 'ok')
                self._apply_row_severity_fill(seo_ws, row_idx, 1, len(seo_headers), sev)
                row_idx += 1
        seo_ws.freeze_panes = 'A2'
        seo_ws.auto_filter.ref = f"A1:F{max(2, row_idx-1)}"
        seo_ws.column_dimensions['A'].width = 24
        seo_ws.column_dimensions['B'].width = 44
        seo_ws.column_dimensions['C'].width = 40
        seo_ws.column_dimensions['D'].width = 40
        seo_ws.column_dimensions['E'].width = 12
        seo_ws.column_dimensions['F'].width = 70

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    def generate_mobile_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate detailed mobile XLSX report."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        devices = results.get("device_results", []) or []
        issues = results.get("issues", []) or []
        recommendations = results.get("recommendations", []) or []

        ws = wb.active
        ws.title = "РЎРІРѕРґРєР°"
        ws["A1"] = "РћС‚С‡РµС‚ РјРѕР±РёР»СЊРЅРѕР№ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё"
        ws["A1"].font = Font(bold=True, size=16)
        ws.merge_cells("A1:E1")

        rows = [
            ("URL", data.get("url", "н/д")),
            ("Р”РІРёР¶ок", results.get("engine", "legacy")),
            ("Р РµР¶РёРј", results.get("mode", "full")),
            ("РћС†РµРЅРєР°", results.get("score", 0)),
            ("Мобильная совместимость", "Р”Р°" if results.get("mobile_friendly") else "РќРµС‚"),
            ("РЈСЃС‚СЂРѕР№СЃС‚РІ", summary.get("total_devices", len(results.get("devices_tested", [])))),
            ("Р‘РµР· РєСЂРёС‚РёС‡РЅС‹С… РїСЂРѕР±Р»РµРј", summary.get("mobile_friendly_devices", 0)),
            ("РЎ РїСЂРѕР±Р»РµРјР°РјРё", summary.get("non_friendly_devices", 0)),
            ("РЎСЂРµРґРЅяя Р·Р°РіСЂСѓР·РєР° (мс)", summary.get("avg_load_time_ms", 0)),
            ("РљРѕР»РёС‡РµСЃС‚во РїСЂРѕР±Р»РµРј", results.get("issues_count", 0)),
            ("РљСЂРёС‚РёС‡РЅС‹С…", summary.get("critical_issues", 0)),
            ("РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёР№", summary.get("warning_issues", 0)),
            ("РРЅС„Рѕ", summary.get("info_issues", 0)),
        ]
        r = 3
        for key, val in rows:
            ws.cell(row=r, column=1, value=key).font = Font(bold=True)
            ws.cell(row=r, column=2, value=val)
            r += 1
        ws.column_dimensions["A"].width = 28
        ws.column_dimensions["B"].width = 80

        dws = wb.create_sheet("РЈСЃС‚СЂРѕР№СЃС‚РІР°")
        headers = ["РЈСЃС‚СЂРѕР№СЃС‚во", "РўРёРї", "HTTP", "РњРѕР±РёР»СЊРЅРѕ-РґСЂСѓР¶РµР»СЋР±РЅРѕ", "РџСЂРѕР±Р»РµРј", "Р—Р°РіСЂСѓР·РєР° (мс)", "РЎРєСЂРёРЅС€РѕС‚", "РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ"]
        for col, header in enumerate(headers, 1):
            self._apply_style(dws.cell(row=1, column=col, value=header), header_style)

        for row_idx, d in enumerate(devices, start=2):
            if d.get("issues_count", 0) > 0 and not d.get("mobile_friendly"):
                severity = "warning"
            elif d.get("issues_count", 0) > 0:
                severity = "info"
            else:
                severity = "ok"

            values = [
                d.get("device_name", ""),
                d.get("category", ""),
                d.get("status_code", "н/д"),
                "Р”Р°" if d.get("mobile_friendly") else "РќРµС‚",
                d.get("issues_count", 0),
                d.get("load_time_ms", 0),
                d.get("screenshot_name", ""),
                severity.capitalize(),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(dws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(dws, row_idx, 1, len(headers), severity)
            self._apply_severity_cell_style(dws.cell(row=row_idx, column=len(headers)), severity)
        dws.freeze_panes = "A2"
        dws.auto_filter.ref = "A1:H1"
        for col, width in enumerate([28, 12, 10, 14, 10, 12, 40, 12], 1):
            dws.column_dimensions[get_column_letter(col)].width = width

        iws = wb.create_sheet("РџСЂРѕР±Р»РµРјС‹")
        issue_headers = ["РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ", "РЈСЃС‚СЂРѕР№СЃС‚во", "Код", "РџСЂРѕР±Р»РµРјР°", "Р”РµС‚Р°Р»Рё"]
        for col, header in enumerate(issue_headers, 1):
            self._apply_style(iws.cell(row=1, column=col, value=header), header_style)
        for row_idx, issue in enumerate(issues, start=2):
            severity = (issue.get("severity") or "info").lower()
            values = [
                severity.capitalize(),
                issue.get("device", ""),
                issue.get("code", ""),
                issue.get("title", ""),
                issue.get("details", ""),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(iws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(iws, row_idx, 1, len(issue_headers), severity)
            self._apply_severity_cell_style(iws.cell(row=row_idx, column=1), severity)
        iws.freeze_panes = "A2"
        iws.auto_filter.ref = "A1:E1"
        for col, width in enumerate([12, 24, 20, 30, 80], 1):
            iws.column_dimensions[get_column_letter(col)].width = width

        rws = wb.create_sheet("Р РµРєРѕРјРµРЅРґР°С†РёРё")
        self._apply_style(rws.cell(row=1, column=1, value="Р РµРєРѕРјРµРЅРґР°С†РёСЏ"), header_style)
        for idx, rec in enumerate(recommendations, start=2):
            self._apply_style(rws.cell(row=idx, column=1, value=rec), cell_style)
        rws.column_dimensions["A"].width = 160
        rws.freeze_panes = "A2"

        sws = wb.create_sheet("РЎРєСЂРёРЅС€РѕС‚С‹")
        shot_headers = ["РЈСЃС‚СЂРѕР№СЃС‚во", "Рмя СЃРєСЂРёРЅС€РѕС‚Р°", "РџСѓС‚СЊ", "URL"]
        for col, header in enumerate(shot_headers, 1):
            self._apply_style(sws.cell(row=1, column=col, value=header), header_style)
        for row_idx, d in enumerate(devices, start=2):
            vals = [d.get("device_name", ""), d.get("screenshot_name", ""), d.get("screenshot_path", ""), d.get("screenshot_url", "")]
            for col, value in enumerate(vals, 1):
                self._apply_style(sws.cell(row=row_idx, column=col, value=value), cell_style)
        sws.freeze_panes = "A2"
        sws.auto_filter.ref = "A1:D1"
        for col, width in enumerate([26, 40, 80, 48], 1):
            sws.column_dimensions[get_column_letter(col)].width = width

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_bot_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate detailed bot accessibility report with severity styling."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()
        results = data.get("results", {}) or {}
        report_url = data.get("url", "н/д")
        summary = results.get("summary", {}) or {}
        bot_rows = results.get("bot_rows", []) or []
        bot_results = results.get("bot_results", {}) or {}
        category_stats = results.get("category_stats", []) or []
        issues = results.get("issues", []) or []
        recommendations = results.get("recommendations", []) or []

        ws = wb.active
        ws.title = "РЎРІРѕРґРєР°"
        ws["A1"] = "РћС‚С‡РµС‚ по РґРѕСЃС‚СѓРїРЅРѕСЃС‚Рё Р±РѕС‚ов"
        ws["A1"].font = Font(bold=True, size=16)
        ws.merge_cells("A1:E1")

        summary_rows = [
            ("URL", report_url),
            ("Р”РІРёР¶ок", results.get("engine", "legacy")),
            ("Р”РѕРјРµРЅ", results.get("domain", "")),
            ("РџСЂРѕРІРµСЂРµРЅРѕ Р±РѕС‚ов", len(results.get("bots_checked", []) or list(bot_results.keys()))),
            ("Р”РѕСЃС‚упно", summary.get("accessible", 0)),
            ("РќРµРґРѕСЃС‚упно", summary.get("unavailable", 0)),
            ("РЎ РєРѕРЅС‚РµРЅС‚ом", summary.get("with_content", 0)),
            ("Р‘РµР· РєРѕРЅС‚РµРЅС‚Р°", summary.get("without_content", 0)),
            ("Р—Р°РїСЂРµС‰РµРЅРѕ robots", summary.get("robots_disallowed", 0)),
            ("Р—Р°РїСЂРµС‚ X-Robots", summary.get("x_robots_forbidden", 0)),
            ("Р—Р°РїСЂРµС‚ Meta Robots", summary.get("meta_forbidden", 0)),
            ("РЎСЂРµРґРЅРёР№ РѕС‚РІРµС‚ (мс)", summary.get("avg_response_time_ms", "")),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 90

        results_ws = wb.create_sheet("Р РµР·СѓР»СЊС‚Р°С‚С‹ Р±РѕС‚ов")
        result_headers = [
            "Р‘РѕС‚",
            "РљР°С‚РµРіРѕСЂРёСЏ",
            "HTTP",
            "Р”РѕСЃС‚СѓРїРµРЅ",
            "Р•СЃС‚СЊ РєРѕРЅС‚РµРЅС‚",
            "Р Р°Р·СЂРµС€РµРЅ robots",
            "X-Robots-Tag",
            "Р—Р°РїСЂРµС‚ X-Robots",
            "Meta Robots",
            "Р—Р°РїСЂРµС‚ Meta",
            "РћС‚РІРµС‚ (мс)",
            "Р¤РёРЅР°Р»СЊРЅС‹Р№ URL",
            "РћС€РёР±РєР°",
            "РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ",
        ]
        for col, header in enumerate(result_headers, 1):
            self._apply_style(results_ws.cell(row=1, column=col, value=header), header_style)

        if not bot_rows and bot_results:
            for bot, item in bot_results.items():
                bot_rows.append({
                    "bot_name": bot,
                    "category": item.get("category", ""),
                    "status": item.get("status"),
                    "accessible": item.get("accessible"),
                    "has_content": item.get("has_content"),
                    "robots_allowed": item.get("robots_allowed"),
                    "x_robots_tag": item.get("x_robots_tag"),
                    "x_robots_forbidden": item.get("x_robots_forbidden"),
                    "meta_robots": item.get("meta_robots"),
                    "meta_forbidden": item.get("meta_forbidden"),
                    "response_time_ms": item.get("response_time_ms"),
                    "final_url": item.get("final_url"),
                    "error": item.get("error"),
                })

        for row_idx, item in enumerate(bot_rows, start=2):
            if item.get("error") or not item.get("accessible"):
                severity = "critical"
            elif not item.get("has_content"):
                severity = "warning"
            elif item.get("x_robots_forbidden") or item.get("meta_forbidden"):
                severity = "info"
            else:
                severity = "ok"

            values = [
                item.get("bot_name", ""),
                item.get("category", ""),
                item.get("status", ""),
                "Р”Р°" if item.get("accessible") else "РќРµС‚",
                "Р”Р°" if item.get("has_content") else "РќРµС‚",
                "Р”Р°" if item.get("robots_allowed") is True else ("РќРµС‚" if item.get("robots_allowed") is False else "н/д"),
                item.get("x_robots_tag", ""),
                "Р”Р°" if item.get("x_robots_forbidden") else "РќРµС‚",
                item.get("meta_robots", ""),
                "Р”Р°" if item.get("meta_forbidden") else "РќРµС‚",
                item.get("response_time_ms", ""),
                item.get("final_url", ""),
                item.get("error", ""),
                severity.capitalize(),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(results_ws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(results_ws, row_idx, 1, len(result_headers), severity)
            self._apply_severity_cell_style(results_ws.cell(row=row_idx, column=len(result_headers)), severity)

        results_ws.freeze_panes = "A2"
        results_ws.auto_filter.ref = f"A1:{get_column_letter(len(result_headers))}1"
        for col, width in enumerate([26, 16, 10, 10, 12, 14, 28, 16, 28, 14, 14, 40, 38, 12], 1):
            results_ws.column_dimensions[get_column_letter(col)].width = width

        categories_ws = wb.create_sheet("Categories")
        category_headers = ["РљР°С‚РµРіРѕСЂРёСЏ", "Р’СЃРµго", "Р”РѕСЃС‚упно", "РЎ РєРѕРЅС‚РµРЅС‚ом", "РћРіСЂР°РЅРёС‡РёРІР°СЋС‰РёРµ РґРёСЂРµРєС‚РёРІС‹", "РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ"]
        for col, header in enumerate(category_headers, 1):
            self._apply_style(categories_ws.cell(row=1, column=col, value=header), header_style)
        for row_idx, item in enumerate(category_stats, start=2):
            total = item.get("total", 0) or 0
            accessible = item.get("accessible", 0) or 0
            ratio = (accessible / total) if total else 0
            severity = "ok" if ratio >= 0.9 else ("warning" if ratio >= 0.6 else "critical")
            values = [
                item.get("category", ""),
                total,
                accessible,
                item.get("with_content", 0),
                item.get("restrictive_directives", 0),
                severity.capitalize(),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(categories_ws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(categories_ws, row_idx, 1, len(category_headers), severity)
            self._apply_severity_cell_style(categories_ws.cell(row=row_idx, column=len(category_headers)), severity)
        categories_ws.freeze_panes = "A2"
        categories_ws.auto_filter.ref = "A1:F1"
        for col, width in enumerate([24, 10, 12, 14, 22, 12], 1):
            categories_ws.column_dimensions[get_column_letter(col)].width = width

        issues_ws = wb.create_sheet("РџСЂРѕР±Р»РµРјС‹")
        issue_headers = ["РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ", "Р‘РѕС‚", "РљР°С‚РµРіРѕСЂРёСЏ", "Р—Р°РіРѕР»овок", "Р”РµС‚Р°Р»Рё"]
        for col, header in enumerate(issue_headers, 1):
            self._apply_style(issues_ws.cell(row=1, column=col, value=header), header_style)
        for row_idx, item in enumerate(issues, start=2):
            severity = (item.get("severity") or "info").lower()
            values = [
                severity.capitalize(),
                item.get("bot", ""),
                item.get("category", ""),
                item.get("title", ""),
                item.get("details", ""),
            ]
            for col, value in enumerate(values, 1):
                self._apply_style(issues_ws.cell(row=row_idx, column=col, value=value), cell_style)
            self._apply_row_severity_fill(issues_ws, row_idx, 1, len(issue_headers), severity)
            self._apply_severity_cell_style(issues_ws.cell(row=row_idx, column=1), severity)
        issues_ws.freeze_panes = "A2"
        issues_ws.auto_filter.ref = "A1:E1"
        for col, width in enumerate([12, 24, 16, 28, 80], 1):
            issues_ws.column_dimensions[get_column_letter(col)].width = width

        rec_ws = wb.create_sheet("Р РµРєРѕРјРµРЅРґР°С†РёРё")
        self._apply_style(rec_ws.cell(row=1, column=1, value="Р РµРєРѕРјРµРЅРґР°С†РёСЏ"), header_style)
        for idx, text in enumerate(recommendations, start=2):
            cell = rec_ws.cell(row=idx, column=1, value=text)
            self._apply_style(cell, cell_style)
        rec_ws.column_dimensions["A"].width = 160
        rec_ws.freeze_panes = "A2"
        rec_ws.auto_filter.ref = "A1:A1"

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_site_audit_pro_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate 8-sheet Site Audit Pro XLSX with deduplicated metric ownership."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()

        results = data.get("results", {}) or {}
        summary = results.get("summary", {}) or {}
        pages = results.get("pages", []) or []
        issues = results.get("issues", []) or []
        pipeline = results.get("pipeline", {}) or {}
        pipeline_metrics = pipeline.get("metrics", {}) or {}
        report_url = data.get("url", "n/a")
        mode = results.get("mode", "quick")

        tfidf_by_url = {row.get("url"): row.get("top_terms", []) for row in (pipeline.get("tf_idf") or [])}
        issues_by_url: Dict[str, List[Dict[str, Any]]] = {}
        for issue in issues:
            issues_by_url.setdefault(issue.get("url", ""), []).append(issue)

        def bool_icon(value: Any, positive: str = "✅", negative: str = "❌") -> str:
            return positive if bool(value) else negative

        def icon_score(value: Any, low: float = 60.0, high: float = 80.0) -> str:
            v = to_float(value, 0.0)
            if v >= high:
                return f"✅ {v:.0f}"
            if v >= low:
                return f"⚠️ {v:.0f}"
            return f"❌ {v:.0f}"

        def icon_count(count: int, low: float = 20.0, high: float = 50.0) -> str:
            # Smaller count is better; use inverted score scale.
            score = 100.0 - min(100.0, float(count) * 5.0)
            if score >= high:
                return f"✅ {count}"
            if score >= low:
                return f"⚠️ {count}"
            return f"❌ {count}"

        def as_percent(value: Any) -> str:
            try:
                return f"{float(value):.1f}%"
            except Exception:
                return "0.0%"

        def to_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except Exception:
                return default

        def ok_if_empty(recs: List[str]) -> str:
            return "OK" if not recs else "; ".join(recs[:2])

        def page_solution(tab: str, page: Dict[str, Any], page_issues: List[Dict[str, Any]] | None = None) -> str:
            recs: List[str] = []
            page_issues = page_issues or []
            if tab == "main":
                if not page.get("indexable"):
                    recs.append("Убрать noindex/исправить статус/robots")
                if to_float(page.get("health_score"), 100.0) < 60:
                    recs.append("Поднять тех.качество и контент до 60+")
                if not page_issues and not recs:
                    return "OK"
                if recs:
                    return ok_if_empty(recs)
                issue_titles = [str(i.get("title") or i.get("code") or "") for i in page_issues if (i.get("title") or i.get("code"))]
                return ok_if_empty(issue_titles)

            if tab == "hierarchy":
                status = str(page.get("h_hierarchy") or "").lower()
                if "wrong start" in status:
                    return "Добавьте <h1> в начало основного контента"
                if "level skip" in status:
                    return "Исправьте пропуски уровней (H1->H2->H3)"
                if "multiple h1" in status:
                    return "Оставьте только 1 H1, остальные замените на H2"
                if "missing h1" in status:
                    return "Добавьте один описательный H1"
                return "OK"

            if tab == "onpage":
                title_len = int(page.get("title_len") or len(str(page.get("title") or "")))
                desc_len = int(page.get("description_len") or len(str(page.get("meta_description") or "")))
                if title_len < 30 or title_len > 60:
                    recs.append("Title 30-60 символов, уникальный")
                if desc_len < 50 or desc_len > 160:
                    recs.append("Meta 100-160 символов с CTA")
                if int(page.get("h1_count") or 0) != 1:
                    recs.append("Оставить 1 H1")
                if (page.get("canonical_status") or "") in ("missing", "external", "invalid"):
                    recs.append("Поставить корректный canonical")
                if int(page.get("duplicate_title_count") or 0) > 1:
                    recs.append("Уникализировать Title")
                if int(page.get("duplicate_description_count") or 0) > 1:
                    recs.append("Уникализировать Meta")
                return ok_if_empty(recs)

            if tab == "content":
                unique_percent = to_float(page.get("unique_percent"), 0.0)
                words = int(page.get("word_count") or 0)
                if unique_percent < 30:
                    recs.append("Добавить новые текстовые блоки (уникальность <30%)")
                elif unique_percent < 50:
                    recs.append("Повысить уникальность >50%")
                if words < 300:
                    recs.append("Увеличить объем до 300+ слов")
                if to_float(page.get("toxicity_score"), 0.0) > 40:
                    recs.append("Снизить keyword-stuffing/спам")
                return ok_if_empty(recs)

            if tab == "technical":
                if not page.get("indexable"):
                    recs.append("Сделать страницу индексируемой")
                if not page.get("is_https"):
                    recs.append("Включить HTTPS")
                if not page.get("compression_enabled"):
                    recs.append("Включить gzip/br")
                if not page.get("cache_enabled"):
                    recs.append("Настроить Cache-Control")
                if (page.get("canonical_status") or "") in ("missing", "external", "invalid"):
                    recs.append("Проверить canonical")
                if len(page.get("deprecated_tags") or []) > 0:
                    recs.append("Удалить устаревшие теги")
                rt = page.get("response_time_ms")
                if rt is not None and int(rt) > 2000:
                    recs.append("Ускорить ответ сервера")
                return ok_if_empty(recs)

            if tab == "eeat":
                eeat = to_float(page.get("eeat_score"), 0.0)
                if eeat < 50:
                    recs.append("Добавить автора, био, источники, кейсы")
                elif eeat < 70:
                    recs.append("Усилить авторство и доверие")
                if not page.get("has_author_info"):
                    recs.append("Блок автора + контакт/соцсети")
                if not page.get("has_reviews"):
                    recs.append("Добавить отзывы/кейсы")
                return ok_if_empty(recs)

            if tab == "trust":
                if not page.get("has_contact_info"):
                    recs.append("Добавить контакты/адрес/телефон")
                if not page.get("has_legal_docs"):
                    recs.append("Добавить юридические документы")
                if not page.get("has_reviews"):
                    recs.append("Добавить отзывы")
                if not page.get("trust_badges"):
                    recs.append("Добавить trust-бейджи/сертификаты")
                return ok_if_empty(recs)

            if tab == "health":
                if to_float(page.get("health_score"), 100.0) < 60:
                    recs.append("Поднять общий Health до 60+")
                if not page.get("indexable"):
                    recs.append("Индексируемость")
                if int(page.get("duplicate_title_count") or 0) > 1:
                    recs.append("Уникализировать Title")
                if int(page.get("duplicate_description_count") or 0) > 1:
                    recs.append("Уникализировать Meta")
                return ok_if_empty(recs)

            if tab == "links":
                if page.get("orphan_page"):
                    recs.append("Добавить внутренние ссылки на страницу")
                if int(page.get("outgoing_internal_links") or 0) == 0:
                    recs.append("Добавить ссылки на релевантные страницы")
                return ok_if_empty(recs)

            if tab == "images":
                img_opt = page.get("images_optimization") or {}
                if int(img_opt.get("no_alt") or page.get("images_without_alt") or 0) > 0:
                    recs.append("Добавить ALT для изображений")
                if int(img_opt.get("no_width_height") or 0) > 0:
                    recs.append("Добавить width/height")
                if int(img_opt.get("no_lazy_load") or 0) > 0:
                    recs.append("Включить lazy-loading")
                return ok_if_empty(recs)

            if tab == "external":
                total = int(page.get("outgoing_external_links") or 0)
                if total == 0:
                    recs.append("Добавить релевантные внешние источники")
                return ok_if_empty(recs)

            if tab == "structured":
                if int(page.get("structured_data") or 0) == 0:
                    recs.append("Внедрить schema.org (JSON-LD)")
                if int(page.get("hreflang_count") or 0) == 0:
                    recs.append("Добавить hreflang для языковых версий")
                return ok_if_empty(recs)

            if tab == "keywords":
                if not (page.get("top_keywords") or page.get("top_terms") or page.get("tf_idf_keywords")):
                    recs.append("Уточнить семантику и ключи")
                if to_float(page.get("toxicity_score"), 0.0) > 40:
                    recs.append("Снизить переспам ключами")
                return ok_if_empty(recs)

            if tab == "topics":
                if not page.get("topic_hub"):
                    recs.append("Связать с релевантным хабом")
                if not page.get("topic_label"):
                    recs.append("Определить кластер темы")
                return ok_if_empty(recs)

            if tab == "advanced":
                freshness = page.get("content_freshness_days")
                if freshness is not None and int(freshness) > 365:
                    recs.append("Обновить контент")
                if bool(page.get("hidden_content")):
                    recs.append("Убрать скрытый контент")
                if bool(page.get("cloaking_detected")):
                    recs.append("Исключить клоакинг")
                return ok_if_empty(recs)

            if tab == "link_quality":
                if to_float(page.get("link_quality_score"), 0.0) < 60:
                    recs.append("Усилить внутреннюю перелинковку")
                if page.get("orphan_page"):
                    recs.append("Добавить входящие ссылки")
                return ok_if_empty(recs)

            return "OK"

        def hierarchy_problem(page: Dict[str, Any]) -> str:
            errors = page.get("h_errors") or []
            if errors:
                mapping = {
                    "wrong_start": "Hierarchy starts from H2+ instead of H1",
                    "missing_h1": "Missing H1",
                    "multiple_h1": "Multiple H1 tags",
                    "heading_level_skip": "Heading level skip detected",
                }
                return "; ".join(mapping.get(str(e), str(e)) for e in errors)
            return "No hierarchy issues"

        def ai_found_list(page: Dict[str, Any]) -> str:
            markers = page.get("ai_markers_list") or []
            return ", ".join(markers[:10]) if markers else "No AI markers detected"

        def infer_page_severity(page: Dict[str, Any]) -> str:
            severity = "ok"
            for issue in (page.get("issues") or []):
                sev = (issue.get("severity") or "info").lower()
                if sev == "critical":
                    return "critical"
                if sev == "warning":
                    severity = "warning"
                elif sev == "info" and severity == "ok":
                    severity = "info"
            return severity

        def fill_sheet(
            sheet_name: str,
            headers: List[str],
            rows: List[List[Any]],
            severity_idx: int = -1,
            widths: List[int] = None,
        ):
            wsx = wb.create_sheet(sheet_name)
            for col, header in enumerate(headers, 1):
                self._apply_style(wsx.cell(row=1, column=col, value=header), header_style)
            for row_idx, row_data in enumerate(rows, start=2):
                for col, value in enumerate(row_data, 1):
                    self._apply_style(wsx.cell(row=row_idx, column=col, value=value), cell_style)
                if severity_idx >= 0:
                    sev_value = str(row_data[severity_idx]).lower()
                    self._apply_row_severity_fill(wsx, row_idx, 1, len(headers), sev_value)
                    self._apply_severity_cell_style(wsx.cell(row=row_idx, column=severity_idx + 1), sev_value)
            wsx.freeze_panes = "A2"
            wsx.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
            if widths:
                for col, width in enumerate(widths, 1):
                    wsx.column_dimensions[get_column_letter(col)].width = width

        # Sheet 1: Executive summary + formula-based issue rates
        ws = wb.active
        ws.title = "1_Executive"
        ws["A1"] = "Site Audit Pro Report"
        ws["A1"].font = Font(bold=True, size=16)
        ws.merge_cells("A1:D1")

        summary_rows = [
            ("URL", report_url),
            ("Mode", mode),
            ("Total pages", summary.get("total_pages", len(pages))),
            ("Internal pages", summary.get("internal_pages", "")),
            ("Issues total", summary.get("issues_total", len(issues))),
            ("Critical", summary.get("critical_issues", 0)),
            ("Warning", summary.get("warning_issues", 0)),
            ("Info", summary.get("info_issues", 0)),
            ("Score", summary.get("score", "")),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws["A13"] = "Critical Rate"
        ws["B13"] = "=IF(B5=0,0,B8/B5)"
        ws["A14"] = "Warning Rate"
        ws["B14"] = "=IF(B5=0,0,B9/B5)"
        ws["A15"] = "Info Rate"
        ws["B15"] = "=IF(B5=0,0,B10/B5)"
        ws["B13"].number_format = "0.00%"
        ws["B14"].number_format = "0.00%"
        ws["B15"].number_format = "0.00%"

        ws["A17"] = "Avg Response (ms)"
        ws["B17"] = pipeline_metrics.get("avg_response_time_ms", "")
        ws["A18"] = "Avg Readability"
        ws["B18"] = pipeline_metrics.get("avg_readability_score", "")
        ws["A19"] = "Avg Link Quality"
        ws["B19"] = pipeline_metrics.get("avg_link_quality_score", "")
        ws["A20"] = "Orphan Pages"
        ws["B20"] = pipeline_metrics.get("orphan_pages", "")
        ws["A21"] = "Topic Hubs"
        ws["B21"] = pipeline_metrics.get("topic_hubs", "")

        ws.column_dimensions["A"].width = 26
        ws.column_dimensions["B"].width = 80

        # Sheet 2: OnPage + Structured
        onpage_headers = [
            "URL", "Title", "Meta description", "Canonical", "Meta robots", "H1",
            "Schema count", "Hreflang count", "Mobile hint", "Indexable",
            "Title dup", "Desc dup", "Severity",
        ]
        onpage_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            onpage_rows.append([
                page.get("url", ""),
                page.get("title", ""),
                page.get("meta_description", ""),
                page.get("canonical", ""),
                page.get("meta_robots", ""),
                page.get("h1_count", ""),
                page.get("schema_count", 0),
                page.get("hreflang_count", 0),
                page.get("mobile_friendly_hint", ""),
                page.get("indexable", ""),
                page.get("duplicate_title_count", 0),
                page.get("duplicate_description_count", 0),
                sev,
            ])
        fill_sheet(
            "2_OnPage+Structured",
            onpage_headers,
            onpage_rows,
            severity_idx=12,
            widths=[60, 30, 36, 36, 20, 8, 12, 12, 12, 10, 10, 10, 10],
        )

        # Sheet 3: Technical
        tech_headers = [
            "URL", "Final URL", "Status", "Response ms", "HTML bytes", "DOM nodes", "Redirects",
            "HTTPS", "Compression", "Cache hints", "Health", "Severity",
        ]
        tech_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            tech_rows.append([
                page.get("url", ""),
                page.get("final_url", ""),
                page.get("status_code", ""),
                page.get("response_time_ms", ""),
                page.get("html_size_bytes", ""),
                page.get("dom_nodes_count", ""),
                page.get("redirect_count", 0),
                page.get("is_https", ""),
                page.get("compression_enabled", ""),
                page.get("cache_enabled", ""),
                page.get("health_score", ""),
                sev,
            ])
        fill_sheet(
            "3_Technical",
            tech_headers,
            tech_rows,
            severity_idx=11,
            widths=[50, 50, 10, 12, 12, 12, 10, 8, 12, 12, 10, 10],
        )

        # Sheet 4: Content + AI
        content_headers = [
            "URL", "Word count", "Unique words", "Lexical diversity", "Readability",
            "Toxicity", "Filler ratio", "AI markers", "Recommendation", "Severity",
        ]
        content_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            content_rows.append([
                page.get("url", ""),
                page.get("word_count", 0),
                page.get("unique_word_count", 0),
                page.get("lexical_diversity", 0),
                page.get("readability_score", 0),
                page.get("toxicity_score", 0),
                page.get("filler_ratio", 0),
                page.get("ai_markers_count", 0),
                page.get("recommendation", ""),
                sev,
            ])
        fill_sheet(
            "4_Content+AI",
            content_headers,
            content_rows,
            severity_idx=9,
            widths=[55, 10, 12, 14, 12, 10, 10, 10, 62, 10],
        )

        # Sheet 5: Link Graph
        link_headers = [
            "URL", "Incoming int", "Outgoing int", "Outgoing ext", "Orphan",
            "Topic hub", "PageRank", "Weak anchor ratio", "Link quality", "Severity",
        ]
        link_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            link_rows.append([
                page.get("url", ""),
                page.get("incoming_internal_links", 0),
                page.get("outgoing_internal_links", 0),
                page.get("outgoing_external_links", 0),
                page.get("orphan_page", ""),
                page.get("topic_hub", ""),
                page.get("pagerank", 0),
                page.get("weak_anchor_ratio", 0),
                page.get("link_quality_score", 0),
                sev,
            ])
        fill_sheet(
            "5_LinkGraph",
            link_headers,
            link_rows,
            severity_idx=9,
            widths=[55, 12, 12, 12, 10, 10, 10, 16, 12, 10],
        )

        # Sheet 6: Images + External
        img_headers = [
            "URL", "Images", "Without alt", "External follow", "External nofollow", "Severity",
        ]
        img_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            img_rows.append([
                page.get("url", ""),
                page.get("images_count", 0),
                page.get("images_without_alt", 0),
                page.get("external_follow_links", 0),
                page.get("external_nofollow_links", 0),
                sev,
            ])
        fill_sheet(
            "6_Images+External",
            img_headers,
            img_rows,
            severity_idx=5,
            widths=[64, 10, 12, 14, 16, 10],
        )

        # Sheet 7: Hierarchy + Errors
        issue_headers = ["URL", "Code", "Issue title", "Issue details", "Severity"]
        issue_rows = []
        for issue in issues:
            issue_rows.append([
                issue.get("url", ""),
                issue.get("code", ""),
                issue.get("title", ""),
                issue.get("details", ""),
                (issue.get("severity") or "info").lower(),
            ])
        fill_sheet(
            "7_HierarchyErrors",
            issue_headers,
            issue_rows,
            severity_idx=4,
            widths=[56, 22, 32, 72, 10],
        )

        # Sheet 8: Keywords
        keyword_headers = ["URL", "Topic", "Top terms (TF-IDF)", "Severity"]
        keyword_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            url = page.get("url", "")
            top_terms = tfidf_by_url.get(url, page.get("top_terms", []))
            keyword_rows.append([
                url,
                page.get("topic_label", ""),
                ", ".join(top_terms[:10]),
                sev,
            ])
        fill_sheet("8_Keywords", keyword_headers, keyword_rows, severity_idx=3, widths=[64, 18, 76, 10])

        # Optional full-mode deep sheets.
        if str(mode).lower() == "full":
            semantic_headers = ["Source URL", "Target URL", "Topic", "Reason"]
            semantic_rows = []
            for row in (pipeline.get("semantic_linking_map") or []):
                semantic_rows.append([
                    row.get("source_url", ""),
                    row.get("target_url", ""),
                    row.get("topic", ""),
                    row.get("reason", ""),
                ])
            fill_sheet("9_SemanticMap", semantic_headers, semantic_rows, widths=[60, 60, 18, 40])

            dup_headers = ["Type", "Value", "URLs count", "Sample URLs"]
            dup_rows = []
            duplicates = pipeline.get("duplicates") or {}
            for row in duplicates.get("title_groups") or []:
                urls = row.get("urls") or []
                dup_rows.append(["title", row.get("value", ""), len(urls), ", ".join(urls[:5])])
            for row in duplicates.get("description_groups") or []:
                urls = row.get("urls") or []
                dup_rows.append(["description", row.get("value", ""), len(urls), ", ".join(urls[:5])])
            fill_sheet("10_DuplicatesDeep", dup_headers, dup_rows, widths=[14, 60, 12, 80])

            raw_issue_headers = ["Severity", "URL", "Code", "Title", "Details"]
            raw_issue_rows = []
            for issue in issues:
                raw_issue_rows.append([
                    (issue.get("severity") or "info").lower(),
                    issue.get("url", ""),
                    issue.get("code", ""),
                    issue.get("title", ""),
                    issue.get("details", ""),
                ])
            fill_sheet("11_IssuesRaw", raw_issue_headers, raw_issue_rows, severity_idx=0, widths=[12, 62, 18, 28, 80])

            advanced_headers = [
                "URL",
                "Canonical status",
                "X-Robots-Tag",
                "Indexability reason",
                "Last-Modified",
                "Freshness days",
                "Structured total",
                "Breadcrumbs",
                "HTML quality",
                "Trust score",
                "EEAT score",
                "CTA quality",
                "JS dependence",
                "Has main tag",
                "OG tags",
                "Compression",
            ]
            advanced_rows = []
            for page in pages:
                advanced_rows.append([
                    page.get("url", ""),
                    page.get("canonical_status", ""),
                    page.get("x_robots_tag", ""),
                    page.get("indexability_reason", ""),
                    page.get("last_modified", ""),
                    page.get("content_freshness_days", ""),
                    page.get("structured_data", 0),
                    page.get("breadcrumbs", ""),
                    page.get("html_quality_score", ""),
                    page.get("trust_score", ""),
                    page.get("eeat_score", ""),
                    page.get("cta_text_quality", ""),
                    page.get("js_dependence", ""),
                    page.get("has_main_tag", ""),
                    page.get("og_tags", ""),
                    page.get("compression", ""),
                ])
            fill_sheet("12_AdvancedDeep", advanced_headers, advanced_rows, widths=[52, 16, 22, 20, 22, 14, 14, 12, 12, 10, 10, 10, 12, 12, 10, 10])

            # Compatibility pack: legacy seopro workbook-equivalent views.
            main_compat_headers = [
                "URL", "Title", "Meta", "H1", "Токсичность", "Иерархия", "Health", "HTTP",
                "Indexable", "Canonical", "Resp ms", "Проблемы", "Solution", "Severity",
            ]
            main_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                page_issues = issues_by_url.get(page.get("url", ""), [])
                problem_text = "; ".join((issue.get("title") or issue.get("code") or "") for issue in page_issues[:6])
                if not problem_text:
                    problem_text = hierarchy_problem(page)
                main_compat_rows.append([
                    page.get("url", ""),
                    page.get("title", "") or "-",
                    page.get("meta_description", "") or "-",
                    page.get("h1_text", "") or "-",
                    icon_score(100.0 - to_float(page.get("toxicity_score"), 0.0), low=30.0, high=70.0),
                    page.get("h_hierarchy", "") or "-",
                    icon_score(page.get("health_score"), low=60.0, high=80.0),
                    page.get("status_code", ""),
                    f"{bool_icon(page.get('indexable'))} {'Yes' if page.get('indexable') else 'No'}",
                    page.get("canonical_status", "") or "-",
                    f"{page.get('response_time_ms', '')}" if page.get("response_time_ms") is not None else "-",
                    problem_text,
                    page_solution("main", page, page_issues),
                    sev,
                ])
            fill_sheet("13_MainReport_Compat", main_compat_headers, main_compat_rows, severity_idx=13, widths=[52, 24, 24, 20, 10, 16, 10, 8, 10, 12, 10, 52, 62, 10])

            hierarchy_compat_headers = ["URL", "Статус", "Проблема", "Всего заголовков", "H1 Count", "Решение", "Severity"]
            hierarchy_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                total_headers = int((page.get("h_details") or {}).get("total_headers") or sum((page.get("heading_distribution") or {}).values()))
                hierarchy_compat_rows.append([
                    page.get("url", ""),
                    page.get("h_hierarchy", "") or "unknown",
                    hierarchy_problem(page),
                    total_headers,
                    int(page.get("h1_count") or 0),
                    page_solution("hierarchy", page, issues_by_url.get(page.get("url", ""), [])),
                    sev,
                ])
            fill_sheet("14_Hierarchy_Compat", hierarchy_compat_headers, hierarchy_compat_rows, severity_idx=6, widths=[52, 16, 48, 12, 10, 58, 10])

            onpage_compat_headers = [
                "URL", "Title Len", "Meta Len", "H1", "Canonical", "Canonical Status", "Mobile",
                "Schema", "Breadcrumbs", "Title Dup", "Meta Dup", "Solution", "Severity",
            ]
            onpage_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                onpage_compat_rows.append([
                    page.get("url", ""),
                    f"{int(page.get('title_len') or len(str(page.get('title') or '')))} ch",
                    f"{int(page.get('description_len') or len(str(page.get('meta_description') or '')))} ch",
                    int(page.get("h1_count") or 0),
                    bool_icon(bool(page.get("canonical"))),
                    page.get("canonical_status", "") or "-",
                    bool_icon(page.get("mobile_friendly_hint")),
                    int(page.get("schema_count") or 0),
                    bool_icon(page.get("breadcrumbs")),
                    int(page.get("duplicate_title_count") or 0),
                    int(page.get("duplicate_description_count") or 0),
                    page_solution("onpage", page),
                    sev,
                ])
            fill_sheet("15_OnPage_Compat", onpage_compat_headers, onpage_compat_rows, severity_idx=12, widths=[52, 10, 10, 8, 10, 16, 8, 8, 10, 10, 10, 58, 10])

            content_compat_headers = [
                "URL", "Words", "Unique %", "Readability", "Toxicity", "AI Markers",
                "AI Markers List", "Filler", "Solution", "Severity",
            ]
            content_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                content_compat_rows.append([
                    page.get("url", ""),
                    int(page.get("word_count") or 0),
                    as_percent(page.get("unique_percent")),
                    icon_score(page.get("readability_score"), low=60.0, high=80.0),
                    icon_score(100.0 - to_float(page.get("toxicity_score"), 0.0), low=30.0, high=70.0),
                    icon_count(int(page.get("ai_markers_count") or 0), low=20.0, high=50.0),
                    ai_found_list(page),
                    int(len(page.get("filler_phrases") or [])),
                    page_solution("content", page),
                    sev,
                ])
            fill_sheet("16_Content_Compat", content_compat_headers, content_compat_rows, severity_idx=9, widths=[52, 10, 10, 12, 10, 10, 36, 10, 58, 10])

            technical_compat_headers = [
                "URL", "Status", "Indexable", "Resp ms", "Size KB", "DOM", "HTML Score", "HTTPS",
                "Compression", "Cache", "Canonical", "Robots", "Deprecated", "Solution", "Severity",
            ]
            technical_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                technical_compat_rows.append([
                    page.get("url", ""),
                    page.get("status_code", ""),
                    f"{bool_icon(page.get('indexable'))} {'Yes' if page.get('indexable') else 'No'}",
                    f"{page.get('response_time_ms', '')}" if page.get("response_time_ms") is not None else "-",
                    round((to_float(page.get("html_size_bytes"), 0.0) / 1024.0), 1),
                    int(page.get("dom_nodes_count") or 0),
                    icon_score(page.get("html_quality_score"), low=60.0, high=80.0),
                    bool_icon(page.get("is_https")),
                    "Yes" if page.get("compression_enabled") else "No",
                    "Set" if page.get("cache_enabled") else "No",
                    page.get("canonical_status", ""),
                    page.get("meta_robots", "") or page.get("x_robots_tag", ""),
                    len(page.get("deprecated_tags") or []),
                    page_solution("technical", page),
                    sev,
                ])
            fill_sheet("17_Technical_Compat", technical_compat_headers, technical_compat_rows, severity_idx=14, widths=[52, 8, 10, 10, 10, 10, 10, 8, 10, 8, 12, 22, 10, 58, 10])

            eeat_headers = ["URL", "Score", "Expertise", "Authority", "Trust", "Experience", "Solution", "Severity"]
            eeat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                comp = page.get("eeat_components") or {}
                eeat_rows.append([
                    page.get("url", ""),
                    icon_score(page.get("eeat_score"), low=60.0, high=80.0),
                    to_float(comp.get("expertise"), 0.0),
                    to_float(comp.get("authoritativeness"), 0.0),
                    to_float(comp.get("trustworthiness"), 0.0),
                    to_float(comp.get("experience"), 0.0),
                    page_solution("eeat", page),
                    sev,
                ])
            fill_sheet("18_EEAT_Compat", eeat_headers, eeat_rows, severity_idx=7, widths=[52, 10, 10, 10, 10, 10, 58, 10])

            trust_headers = ["URL", "Trust Score", "Contact", "Legal", "Reviews", "Badges", "Solution", "Severity"]
            trust_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                trust_rows.append([
                    page.get("url", ""),
                    icon_score(page.get("trust_score"), low=60.0, high=80.0),
                    bool_icon(page.get("has_contact_info")),
                    bool_icon(page.get("has_legal_docs")),
                    bool_icon(page.get("has_reviews")),
                    bool_icon(page.get("trust_badges")),
                    page_solution("trust", page),
                    sev,
                ])
            fill_sheet("19_Trust_Compat", trust_headers, trust_rows, severity_idx=7, widths=[52, 10, 8, 8, 8, 8, 58, 10])

            health_headers = [
                "URL", "Health Score", "Indexable", "Words", "Unique %", "Readability",
                "Title Dup", "Meta Dup", "Resp ms", "Solution", "Severity",
            ]
            health_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                health_rows.append([
                    page.get("url", ""),
                    icon_score(page.get("health_score"), low=60.0, high=80.0),
                    f"{bool_icon(page.get('indexable'))} {'Yes' if page.get('indexable') else 'No'}",
                    int(page.get("word_count") or 0),
                    as_percent(page.get("unique_percent")),
                    f"{to_float(page.get('readability_score'), 0.0):.0f}",
                    int(page.get("duplicate_title_count") or 0),
                    int(page.get("duplicate_description_count") or 0),
                    f"{page.get('response_time_ms', '')}" if page.get("response_time_ms") is not None else "-",
                    page_solution("health", page),
                    sev,
                ])
            fill_sheet("20_Health_Compat", health_headers, health_rows, severity_idx=10, widths=[52, 12, 10, 10, 10, 12, 10, 10, 10, 58, 10])

            links_headers = ["URL", "Authority", "Incoming", "Outgoing", "Is Orphan", "Solution", "Severity"]
            links_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                links_rows.append([
                    page.get("url", ""),
                    to_float(page.get("pagerank"), 0.0),
                    int(page.get("incoming_internal_links") or 0),
                    int(page.get("outgoing_internal_links") or 0),
                    "❌ ORPHAN" if page.get("orphan_page") else "✅",
                    page_solution("links", page),
                    sev,
                ])
            fill_sheet("21_InternalLinks_Compat", links_headers, links_rows, severity_idx=6, widths=[52, 10, 10, 10, 10, 58, 10])

            images_headers = ["URL", "Total", "No Alt", "No Width", "No Lazy", "Issues", "Solution", "Severity"]
            images_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                img_opt = page.get("images_optimization") or {}
                no_alt = int(page.get("images_without_alt") or img_opt.get("no_alt") or 0)
                no_width = int(img_opt.get("no_width_height") or 0)
                no_lazy = int(img_opt.get("no_lazy_load") or 0)
                images_rows.append([
                    page.get("url", ""),
                    int(page.get("images_count") or img_opt.get("total") or 0),
                    f"❌ {no_alt}" if no_alt > 0 else "✅",
                    f"⚠️ {no_width}" if no_width > 0 else "✅",
                    f"⚠️ {no_lazy}" if no_lazy > 0 else "✅",
                    (no_alt + no_width + no_lazy),
                    page_solution("images", page),
                    sev,
                ])
            fill_sheet("22_Images_Compat", images_headers, images_rows, severity_idx=7, widths=[52, 10, 10, 10, 10, 10, 58, 10])

            external_headers = ["URL", "Total External", "Follow", "NoFollow", "Follow %", "Solution", "Severity"]
            external_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                follow = int(page.get("external_follow_links") or 0)
                nofollow = int(page.get("external_nofollow_links") or 0)
                total = int(page.get("outgoing_external_links") or (follow + nofollow))
                follow_pct = (follow / total * 100.0) if total > 0 else 0.0
                external_rows.append([
                    page.get("url", ""),
                    total,
                    follow,
                    nofollow,
                    f"{follow_pct:.0f}%",
                    page_solution("external", page),
                    sev,
                ])
            fill_sheet("23_ExternalLinks_Compat", external_headers, external_rows, severity_idx=6, widths=[52, 14, 10, 10, 10, 58, 10])

            structured_headers = ["URL", "Total", "JSON-LD", "Microdata", "RDFa", "Hreflang", "Meta Robots", "Solution", "Severity"]
            structured_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                detail = page.get("structured_data_detail") or {}
                structured_rows.append([
                    page.get("url", ""),
                    int(page.get("structured_data") or 0),
                    int(detail.get("json_ld") or 0),
                    int(detail.get("microdata") or 0),
                    int(detail.get("rdfa") or 0),
                    int(page.get("hreflang_count") or 0),
                    page.get("meta_robots", "") or page.get("x_robots_tag", ""),
                    page_solution("structured", page),
                    sev,
                ])
            fill_sheet("24_Structured_Compat", structured_headers, structured_rows, severity_idx=8, widths=[52, 8, 8, 10, 8, 10, 24, 58, 10])

            kw_headers = ["URL", "Top Keywords", "TF-IDF 1", "TF-IDF 2", "TF-IDF 3", "Solution", "Severity"]
            kw_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                tfidf_terms = list((page.get("tf_idf_keywords") or {}).keys())
                if not tfidf_terms:
                    tfidf_terms = tfidf_by_url.get(page.get("url", ""), []) or page.get("top_terms", [])
                top_keywords = page.get("top_keywords") or tfidf_terms
                kw_rows.append([
                    page.get("url", ""),
                    ", ".join(top_keywords[:5]),
                    tfidf_terms[0] if len(tfidf_terms) > 0 else "",
                    tfidf_terms[1] if len(tfidf_terms) > 1 else "",
                    tfidf_terms[2] if len(tfidf_terms) > 2 else "",
                    page_solution("keywords", page),
                    sev,
                ])
            fill_sheet("25_KeywordsTFIDF_Compat", kw_headers, kw_rows, severity_idx=6, widths=[52, 36, 16, 16, 16, 58, 10])

            topics_headers = ["URL", "Is Hub", "Cluster", "Incoming Links", "Semantic Links", "Solution", "Severity"]
            topics_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                semantic_links = page.get("semantic_links") or []
                summary_links = []
                for item in semantic_links[:3]:
                    target = item.get("target_url") or item.get("target") or ""
                    anchor = item.get("suggested_anchor") or item.get("topic") or ""
                    summary_links.append(f"[{anchor}] -> {target}")
                topics_rows.append([
                    page.get("url", ""),
                    "⭐ HUB" if page.get("topic_hub") else "-",
                    page.get("topic_label", ""),
                    int(page.get("incoming_internal_links") or 0),
                    "\n".join(summary_links),
                    page_solution("topics", page),
                    sev,
                ])
            fill_sheet("26_Topics_Compat", topics_headers, topics_rows, severity_idx=6, widths=[52, 10, 16, 12, 62, 58, 10])

            advanced_compat_headers = [
                "URL", "Freshness Days", "Last Modified", "Status", "Indexable", "Resp ms", "Size KB",
                "Redirects", "Final URL", "Hidden Content", "Cloaking", "CTA Count", "List/Tables", "Solution", "Severity",
            ]
            advanced_compat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                advanced_compat_rows.append([
                    page.get("url", ""),
                    page.get("content_freshness_days", "Unknown") if page.get("content_freshness_days") is not None else "Unknown",
                    page.get("last_modified", "not set"),
                    page.get("status_code", ""),
                    f"{bool_icon(page.get('indexable'))} {'Yes' if page.get('indexable') else 'No'}",
                    f"{page.get('response_time_ms', '')}" if page.get("response_time_ms") is not None else "-",
                    round((to_float(page.get("html_size_bytes"), 0.0) / 1024.0), 1),
                    int(page.get("redirect_count") or 0),
                    page.get("final_url", ""),
                    f"❌ {int(page.get('hidden_content') or 0)}" if page.get("hidden_content") else "✅",
                    "❌" if page.get("cloaking_detected") else "✅",
                    int(page.get("cta_count") or 0),
                    f"{int(page.get('lists_count') or 0)}/{int(page.get('tables_count') or 0)}",
                    page_solution("advanced", page),
                    sev,
                ])
            fill_sheet("27_Advanced_Compat", advanced_compat_headers, advanced_compat_rows, severity_idx=14, widths=[52, 12, 22, 8, 10, 10, 10, 10, 52, 12, 10, 10, 12, 58, 10])

            link_quality_headers = [
                "URL", "Linking Score", "Page Authority", "Anchor Score", "Incoming Links",
                "Outgoing Internal", "Orphan", "Topic Hub", "Solution", "Severity",
            ]
            link_quality_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                link_quality_rows.append([
                    page.get("url", ""),
                    icon_score(page.get("link_quality_score"), low=60.0, high=80.0),
                    to_float(page.get("pagerank"), 0.0),
                    to_float(page.get("anchor_text_quality_score"), 0.0),
                    int(page.get("incoming_internal_links") or 0),
                    int(page.get("outgoing_internal_links") or 0),
                    "✅" if not page.get("orphan_page") else "❌",
                    "✅" if page.get("topic_hub") else "-",
                    page_solution("link_quality", page),
                    sev,
                ])
            fill_sheet("28_LinkQuality_Compat", link_quality_headers, link_quality_rows, severity_idx=9, widths=[52, 12, 12, 12, 12, 14, 10, 10, 58, 10])

            ai_headers = ["URL", "AI Markers Count", "AI Markers Found", "Text Sample with Markers", "Recommendation", "Severity"]
            ai_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                ai_count = int(page.get("ai_markers_count") or 0)
                ai_rows.append([
                    page.get("url", ""),
                    icon_count(ai_count, low=20.0, high=50.0),
                    ai_found_list(page),
                    page.get("ai_marker_sample", "") or ("Marker snippet not available in compact payload" if ai_count > 0 else "No text sample available"),
                    page_solution("content", page) if ai_count > 0 else "No AI markers detected",
                    sev,
                ])
            fill_sheet("29_AIMarkers_Compat", ai_headers, ai_rows, severity_idx=5, widths=[52, 14, 40, 44, 58, 10])

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Р В Р’В Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’ВµР В Р’В Р В РІР‚В¦Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р В РІР‚С™Р В Р’В Р РЋРІР‚ВР В Р Р‹Р В РІР‚С™Р В Р Р‹Р РЋРІР‚СљР В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р’В Р РЋРІР‚СћР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р вЂ™Р’ВµР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р’В Р В РІР‚В  Р В Р’В Р вЂ™Р’В·Р В Р’В Р вЂ™Р’В°Р В Р’В Р В РІР‚В Р В Р’В Р РЋРІР‚ВР В Р Р‹Р В РЎвЂњР В Р’В Р РЋРІР‚ВР В Р’В Р РЋР’ВР В Р’В Р РЋРІР‚СћР В Р Р‹Р В РЎвЂњР В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚В Р В Р’В Р РЋРІР‚СћР В Р Р‹Р Р†Р вЂљРЎв„ў Р В Р Р‹Р Р†Р вЂљРЎв„ўР В Р’В Р РЋРІР‚ВР В Р’В Р РЋРІР‚вЂќР В Р’В Р вЂ™Р’В° Р В Р’В Р вЂ™Р’В·Р В Р’В Р вЂ™Р’В°Р В Р’В Р СћРІР‚ВР В Р’В Р вЂ™Р’В°Р В Р Р‹Р Р†Р вЂљР Р‹Р В Р’В Р РЋРІР‚В"""
        generators = {
            'site_analyze': self.generate_site_analyze_report,
            'robots_check': self.generate_robots_report,
            'sitemap_validate': self.generate_sitemap_report,
            'render_audit': self.generate_render_report,
            'mobile_check': self.generate_mobile_report,
            'bot_check': self.generate_bot_report,
            'site_audit_pro': self.generate_site_audit_pro_report,
        }
        
        generator = generators.get(task_type, self.generate_site_analyze_report)
        return generator(task_id, data)


# Singleton
xlsx_generator = XLSXGenerator()
