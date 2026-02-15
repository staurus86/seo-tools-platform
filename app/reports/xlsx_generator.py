"""
Excel Р С–Р ВµР Р…Р ВµРЎР‚Р В°РЎвЂљРѕСЂ Р С•РЎвЂљРЎвЂЎР ВµРЎвЂљРѕРІ
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from typing import Dict, Any, List
from datetime import datetime
import os

from app.config import settings


class XLSXGenerator:
    """Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРЎв„ўР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ў Excel Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р В Р вЂ№Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В """
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _create_header_style(self):
        """Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В·Р В Р’В Р вЂ™Р’В Р В РЎС›Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СљР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р В Р вЂ№Р В Р’В Р В РІР‚В° Р В Р’В Р вЂ™Р’В Р В РЎС›Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р В Р вЂ№Р В Р’В Р В Р РЏ Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В·Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎСљР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В """
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
        """Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В·Р В Р’В Р вЂ™Р’В Р В РЎС›Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СљР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р В Р вЂ№Р В Р’В Р В РІР‚В° Р В Р’В Р вЂ™Р’В Р В РЎС›Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р В Р вЂ№Р В Р’В Р В Р РЏ Р В Р’В Р В Р вЂ№Р В Р’В Р В Р РЏР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р В Р вЂ№Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎСљ"""
        return {
            'border': Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        }
    
    def _apply_style(self, cell, style):
        """Р В Р’В Р вЂ™Р’В Р В Р Р‹Р РЋРЎСџР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В Р Р‹Р вЂ™Р’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В¦Р В Р’В Р В Р вЂ№Р В Р’В Р В Р РЏР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СљР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р В Р вЂ№Р В Р’В Р В РІР‚В° Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎСљ Р В Р’В Р В Р вЂ№Р В Р’В Р В Р РЏР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р В Р вЂ№Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎСљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’Вµ"""
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
        """Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРЎв„ўР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р В Р вЂ№Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В·Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В° Р В Р’В Р В Р вЂ№Р В Р’В Р РЋРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В°"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Р С’Р Р…Р В°Р В»Р С‘Р В· РЎРѓР В°Р в„–РЎвЂљР В°"
        
        # Header
        ws['A1'] = 'Р С›РЎвЂљРЎвЂЎР ВµРЎвЂљ по SEO-Р В°Р Р…Р В°Р В»Р С‘Р В·РЎС“ РЎРѓР В°Р в„–РЎвЂљР В°'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        # Basic info
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'РЅ/Рґ')
        ws['A4'] = 'Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚Р ВµР Р…Р С• РЎРѓРЎвЂљРЎР‚Р В°Р Р…Р С‘РЎвЂ :'
        ws['B4'] = data.get('pages_analyzed', 0)
        ws['A5'] = 'Р вЂќР В°РЎвЂљР В° Р В·Р В°Р Р†Р ВµРЎР‚РЎв‚¬Р ВµР Р…Р С‘РЎРЏ:'
        ws['B5'] = data.get('completed_at', 'РЅ/Рґ')
        
        # Results section
        ws['A7'] = 'Р В Р ВµР В·РЎС“Р В»РЎРЉРЎвЂљР В°РЎвЂљРЎвЂ№'
        ws['A7'].font = Font(bold=True, size=14)
        
        results = data.get('results', {})
        row = 8
        
        # Headers
        headers = ['Р СџР С•Р С”Р В°Р В·Р В°РЎвЂљР ВµР В»РЎРЉ', 'Р вЂ”Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘Р Вµ', 'Р РЋРЎвЂљР В°РЎвЂљСѓСЃ']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=row, column=col, value=header)
            self._apply_style(cell, self._create_header_style())
        
        # Sample data (will be replaced with real data from tools)
        sample_data = [
            ['Р вЂ™РЎРѓР Вµго РЎРѓРЎвЂљРЎР‚Р В°Р Р…Р С‘РЎвЂ ', results.get('total_pages', 0), 'OK'],
            ['Р РЋРЎвЂљР В°РЎвЂљСѓСЃ', results.get('status', 'РЅ/Рґ'), 'OK'],
            ['Р РЋР Р†Р С•Р Т‘Р С”Р В°', results.get('summary', 'РЅ/Рґ'), 'OK']
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
        """Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРЎв„ўР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљР’ВР В Р’В Р В Р вЂ№Р В Р’В Р Р†Р вЂљРЎв„ўР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎС™Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ Р В Р’В Р вЂ™Р’В Р В Р Р‹Р Р†Р вЂљРЎС›Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р В Р вЂ№Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћ robots.txt"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚Р С”Р В° Robots"
        
        ws['A1'] = 'Р С›РЎвЂљРЎвЂЎР ВµРЎвЂљ по robots.txt'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'РЅ/Рґ')
        
        results = data.get('results', {})
        ws['A5'] = 'Р В¤Р В°Р в„–Р В» robots.txt Р Р…Р В°Р в„–Р Т‘Р ВµР Р…:'
        ws['B5'] = 'Р вЂќР В°' if results.get('robots_txt_found') else 'Р СњР ВµРЎвЂљ'
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate a detailed sitemap validation XLSX report."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()
        results = data.get('results', {}) or {}
        report_url = data.get('url', 'РЅ/Рґ')

        ws = wb.active
        ws.title = "Р РЋР Р†Р С•Р Т‘Р С”Р В°"
        ws['A1'] = 'Р С›РЎвЂљРЎвЂЎР ВµРЎвЂљ по Р Р†Р В°Р В»Р С‘Р Т‘Р В°РЎвЂ Р С‘Р С‘ sitemap'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:E1')

        summary_rows = [
            ("URL", report_url),
            ("Р вЂ™Р В°Р В»Р С‘Р Т‘Р ВµР Р…", "Р вЂќР В°" if results.get("valid") else "Р СњР ВµРЎвЂљ"),
            ("HTTP РЎРѓРЎвЂљР В°РЎвЂљСѓСЃ", results.get("status_code", "РЅ/Рґ")),
            ("Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚Р ВµР Р…Р С• sitemap", results.get("sitemaps_scanned", 0)),
            ("Р вЂ™Р В°Р В»Р С‘Р Т‘Р Р…РЎвЂ№РЎвЂ¦ sitemap", results.get("sitemaps_valid", 0)),
            ("Р вЂ™РЎРѓР Вµго URL", results.get("urls_count", 0)),
            ("Р Р€Р Р…Р С‘Р С”Р В°Р В»РЎРЉР Р…РЎвЂ№РЎвЂ¦ URL", results.get("unique_urls_count", 0)),
            ("Р вЂќРЎС“Р В±Р В»Р С‘ URL", results.get("duplicate_urls_count", 0)),
            ("Р СњР ВµР С”Р С•РЎР‚РЎР‚Р ВµР С”РЎвЂљР Р…РЎвЂ№Р Вµ URL", results.get("invalid_urls_count", 0)),
            ("Р С›РЎв‚¬Р С‘Р В±Р С”Р С‘ lastmod", results.get("invalid_lastmod_count", 0)),
            ("Р С›РЎв‚¬Р С‘Р В±Р С”Р С‘ changefreq", results.get("invalid_changefreq_count", 0)),
            ("Р С›РЎв‚¬Р С‘Р В±Р С”Р С‘ priority", results.get("invalid_priority_count", 0)),
            ("Р В Р В°Р В·Р СР ВµРЎР‚ Р Т‘Р В°Р Р…Р Р…РЎвЂ№РЎвЂ¦ (Р В±Р В°Р в„–РЎвЂљ)", results.get("size", 0)),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws.column_dimensions['A'].width = 28
        ws.column_dimensions['B'].width = 80

        files_ws = wb.create_sheet("Р В¤Р В°Р в„–Р В»РЎвЂ№ Sitemap")
        files_headers = [
            "Sitemap URL", "Р СћР С‘Р С—", "HTTP", "OK", "URL",
            "Р вЂќРЎС“Р В±Р В»Р С‘", "Р В Р В°Р В·Р СР ВµРЎР‚ (Р В±Р В°Р в„–РЎвЂљ)", "Р С›РЎв‚¬Р С‘Р В±Р С”Р С‘", "Р СџРЎР‚Р ВµР Т‘РЎС“Р С—РЎР‚Р ВµР В¶Р Т‘Р ВµР Р…Р С‘РЎРЏ", "Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ"
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
                "Р вЂќР В°" if item.get("ok") else "Р СњР ВµРЎвЂљ",
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

        errors_ws = wb.create_sheet("Р С›РЎв‚¬Р С‘Р В±Р С”Р С‘")
        errors_ws.cell(row=1, column=1, value="Р С›РЎв‚¬Р С‘Р В±Р С”Р В°")
        errors_ws.cell(row=1, column=2, value="Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ")
        self._apply_style(errors_ws.cell(row=1, column=1), header_style)
        self._apply_style(errors_ws.cell(row=1, column=2), header_style)
        for idx, err in enumerate((results.get("errors", []) or []), start=2):
            err_cell = errors_ws.cell(row=idx, column=1, value=err)
            sev_cell = errors_ws.cell(row=idx, column=2, value="Р С™РЎР‚Р С‘РЎвЂљР С‘РЎвЂЎР Р…Р С•")
            self._apply_style(err_cell, cell_style)
            self._apply_style(sev_cell, cell_style)
            self._apply_row_severity_fill(errors_ws, idx, 1, 2, "critical")
            self._apply_severity_cell_style(sev_cell, "critical")
        errors_ws.column_dimensions['A'].width = 140
        errors_ws.column_dimensions['B'].width = 14
        errors_ws.freeze_panes = "A2"
        errors_ws.auto_filter.ref = "A1:B1"

        warnings_ws = wb.create_sheet("Р СџРЎР‚Р ВµР Т‘РЎС“Р С—РЎР‚Р ВµР В¶Р Т‘Р ВµР Р…Р С‘РЎРЏ")
        warnings_ws.cell(row=1, column=1, value="Р СџРЎР‚Р ВµР Т‘РЎС“Р С—РЎР‚Р ВµР В¶Р Т‘Р ВµР Р…Р С‘Р Вµ")
        warnings_ws.cell(row=1, column=2, value="Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ")
        self._apply_style(warnings_ws.cell(row=1, column=1), header_style)
        self._apply_style(warnings_ws.cell(row=1, column=2), header_style)
        for idx, warn in enumerate((results.get("warnings", []) or []), start=2):
            warn_cell = warnings_ws.cell(row=idx, column=1, value=warn)
            sev_cell = warnings_ws.cell(row=idx, column=2, value="Р СџРЎР‚Р ВµР Т‘РЎС“Р С—РЎР‚Р ВµР В¶Р Т‘Р ВµР Р…Р С‘Р Вµ")
            self._apply_style(warn_cell, cell_style)
            self._apply_style(sev_cell, cell_style)
            self._apply_row_severity_fill(warnings_ws, idx, 1, 2, "warning")
            self._apply_severity_cell_style(sev_cell, "warning")
        warnings_ws.column_dimensions['A'].width = 140
        warnings_ws.column_dimensions['B'].width = 14
        warnings_ws.freeze_panes = "A2"
        warnings_ws.auto_filter.ref = "A1:B1"

        dup_ws = wb.create_sheet("Duplicates")
        dup_headers = ["URL", "Р СџР ВµРЎР‚Р Р†РЎвЂ№Р в„– sitemap", "Р вЂќРЎС“Р В±Р В»Р С‘Р С”Р В°РЎвЂљ Р Р† sitemap", "Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ"]
        for col, header in enumerate(dup_headers, 1):
            cell = dup_ws.cell(row=1, column=col, value=header)
            self._apply_style(cell, header_style)
        for row_idx, item in enumerate((results.get("duplicate_details", []) or []), start=2):
            dup_ws.cell(row=row_idx, column=1, value=item.get("url", ""))
            dup_ws.cell(row=row_idx, column=2, value=item.get("first_sitemap", ""))
            dup_ws.cell(row=row_idx, column=3, value=item.get("duplicate_sitemap", ""))
            dup_ws.cell(row=row_idx, column=4, value="Р С™РЎР‚Р С‘РЎвЂљР С‘РЎвЂЎР Р…Р С•")
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
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РґРµС‚Р°Р»СЊРЅС‹Р№ XLSX-РѕС‚С‡РµС‚ по Р°СѓРґРёС‚Сѓ СЂРµРЅРґРµСЂРёРЅРіР° (С„окус РЅР° РїСЂРѕР±Р»РµРјР°С…)."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()

        results = data.get('results', {}) or {}
        summary = results.get('summary', {}) or {}
        variants = results.get('variants', []) or []
        issues = results.get('issues', []) or []
        recommendations = results.get('recommendations', []) or []

        ws = wb.active
        ws.title = 'РЎРІРѕРґРєР°'
        ws['A1'] = 'РћС‚С‡РµС‚ Р°СѓРґРёС‚Р° СЂРµРЅРґРµСЂРёРЅРіР° (JS Рё Р±РµР· JS)'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:E1')

        rows = [
            ('РђРґСЂРµСЃ URL', data.get('url', 'РЅ/Рґ')),
            ('Р”РІРёР¶ок', results.get('engine', 'legacy')),
            ('РџСЂРѕС„РёР»РµР№', summary.get('variants_total', len(variants))),
            ('РћС†РµРЅРєР°', summary.get('score', 'РЅ/Рґ')),
            ('РљСЂРёС‚РёС‡РЅС‹Рµ', summary.get('critical_issues', 0)),
            ('РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ', summary.get('warning_issues', 0)),
            ('РџРѕС‚РµСЂСЏРЅРЅС‹С… СЌР»РµРјРµРЅС‚ов РІСЃРµго', summary.get('missing_total', 0)),
            ('РџРѕС‚РµСЂРё СЃСЂРµРґРЅРёР№ %', summary.get('avg_missing_pct', 0)),
            ('Ср. Р·Р°РіСЂСѓР·РєР° Р±РµР· JS (мс)', summary.get('avg_raw_load_ms', 0)),
            ('Ср. Р·Р°РіСЂСѓР·РєР° JS (мс)', summary.get('avg_js_load_ms', 0)),
        ]
        row_num = 3
        for key, value in rows:
            ws.cell(row=row_num, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row_num, column=2, value=value)
            row_num += 1
        ws.column_dimensions['A'].width = 32
        ws.column_dimensions['B'].width = 80

        variant_ws = wb.create_sheet('РџСЂРѕС„РёР»Рё')
        headers = ['РџСЂРѕС„РёР»СЊ', 'РћС†РµРЅРєР°', 'РџРѕС‚РµСЂРё', 'РџРѕС‚РµСЂРё %', 'H1 Р±РµР· JS', 'H1 СЃ JS', 'РЎСЃС‹Р»РєРё Р±РµР· JS', 'РЎСЃС‹Р»РєРё СЃ JS', 'РЎС‚СЂСѓРєС‚СѓСЂРёСЂРѕРІР°РЅРЅС‹Рµ РґР°РЅРЅС‹Рµ Р±РµР· JS', 'РЎС‚СЂСѓРєС‚СѓСЂРёСЂРѕРІР°РЅРЅС‹Рµ РґР°РЅРЅС‹Рµ СЃ JS']
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

        issues_ws = wb.create_sheet('РћС€РёР±РєРё')
        issue_headers = ['РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ', 'РџСЂРѕС„РёР»СЊ', 'Код', 'Р—Р°РіРѕР»овок', 'Р”РµС‚Р°Р»Рё']
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

        rec_ws = wb.create_sheet('Р РµРєРѕРјРµРЅРґР°С†РёРё')
        self._apply_style(rec_ws.cell(row=1, column=1, value='Р РµРєРѕРјРµРЅРґР°С†РёСЏ'), header_style)
        for idx, text in enumerate(recommendations, start=2):
            self._apply_style(rec_ws.cell(row=idx, column=1, value=text), cell_style)
        rec_ws.column_dimensions['A'].width = 160
        rec_ws.freeze_panes = 'A2'
        rec_ws.auto_filter.ref = 'A1:A1'

        missing_ws = wb.create_sheet('РџРѕС‚РµСЂСЏРЅРЅС‹Рµ СЌР»РµРјРµРЅС‚С‹')
        missing_headers = ['РџСЂРѕС„РёР»СЊ', 'РљР°С‚РµРіРѕСЂРёСЏ', 'Р­Р»РµРјРµРЅС‚']
        for col, header in enumerate(missing_headers, 1):
            self._apply_style(missing_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        for variant in variants:
            profile = variant.get('variant_label') or variant.get('variant_id', '')
            missing = variant.get('missing', {}) or {}
            for key, label in [
                ('visible_text', 'РўРµРєСЃС‚ С‚РѕР»ько РІ JS'),
                ('headings', 'Р—Р°РіРѕР»РѕРІРєРё С‚РѕР»ько РІ JS'),
                ('links', 'РЎСЃС‹Р»РєРё С‚РѕР»ько РІ JS'),
                ('structured_data', 'РЎС‚СЂСѓРєС‚СѓСЂРёСЂРѕРІР°РЅРЅС‹Рµ РґР°РЅРЅС‹Рµ С‚РѕР»ько РІ JS'),
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

        meta_ws = wb.create_sheet('РњРµС‚Р° (РЅРµ SEO)')
        meta_headers = ['РџСЂРѕС„РёР»СЊ', 'РљР»СЋС‡', 'Р‘РµР· JS', 'РЎ JS', 'РЎС‚Р°С‚ус']
        for col, header in enumerate(meta_headers, 1):
            self._apply_style(meta_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        status_map = {
            'same': 'РЎРѕРІРїР°РґР°РµС‚',
            'changed': 'РР·РјРµРЅРµРЅРѕ',
            'only_rendered': 'РўРѕР»ько РІ JS',
            'only_raw': 'РўРѕР»ько Р±РµР· JS',
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

        seo_ws = wb.create_sheet('SEO РѕР±СЏР·Р°С‚РµР»СЊРЅС‹Рµ')
        seo_headers = ['РџСЂРѕС„РёР»СЊ', 'Р­Р»РµРјРµРЅС‚', 'Р‘РµР· JS', 'РЎ JS', 'РЎС‚Р°С‚ус', 'Р§С‚Рѕ РёСЃРїСЂР°РІРёС‚СЊ']
        for col, header in enumerate(seo_headers, 1):
            self._apply_style(seo_ws.cell(row=1, column=col, value=header), header_style)
        row_idx = 2
        status_map = {'pass': 'РџСЂРѕР№РґРµРЅРѕ', 'warn': 'РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёРµ', 'fail': 'РљСЂРёС‚РёС‡РЅРѕ'}
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
        ws.title = "Р РЋР Р†Р С•Р Т‘Р С”Р В°"
        ws["A1"] = "Р С›РЎвЂљРЎвЂЎР ВµРЎвЂљ Р СР С•Р В±Р С‘Р В»РЎРЉР Р…Р С•Р в„– РЎРѓР С•Р Р†Р СР ВµРЎРѓРЎвЂљР С‘Р СР С•РЎРѓРЎвЂљР С‘"
        ws["A1"].font = Font(bold=True, size=16)
        ws.merge_cells("A1:E1")

        rows = [
            ("URL", data.get("url", "РЅ/Рґ")),
            ("Р вЂќР Р†Р С‘Р В¶ок", results.get("engine", "legacy")),
            ("Р В Р ВµР В¶Р С‘Р С", results.get("mode", "full")),
            ("Р С›РЎвЂ Р ВµР Р…Р С”Р В°", results.get("score", 0)),
            ("РњРѕР±РёР»СЊРЅР°СЏ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚СЊ", "Р вЂќР В°" if results.get("mobile_friendly") else "Р СњР ВµРЎвЂљ"),
            ("Р Р€РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎРѓРЎвЂљР Р†", summary.get("total_devices", len(results.get("devices_tested", [])))),
            ("Р вЂР ВµР В· Р С”РЎР‚Р С‘РЎвЂљР С‘РЎвЂЎР Р…РЎвЂ№РЎвЂ¦ Р С—РЎР‚Р С•Р В±Р В»Р ВµР С", summary.get("mobile_friendly_devices", 0)),
            ("Р РЋ Р С—РЎР‚Р С•Р В±Р В»Р ВµР СР В°Р СР С‘", summary.get("non_friendly_devices", 0)),
            ("Р РЋРЎР‚Р ВµР Т‘Р Р…яя Р В·Р В°Р С–РЎР‚РЎС“Р В·Р С”Р В° (мс)", summary.get("avg_load_time_ms", 0)),
            ("Р С™Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљРІРѕ Р С—РЎР‚Р С•Р В±Р В»Р ВµР С", results.get("issues_count", 0)),
            ("Р С™РЎР‚Р С‘РЎвЂљР С‘РЎвЂЎР Р…РЎвЂ№РЎвЂ¦", summary.get("critical_issues", 0)),
            ("Р СџРЎР‚Р ВµР Т‘РЎС“Р С—РЎР‚Р ВµР В¶Р Т‘Р ВµР Р…Р С‘Р в„–", summary.get("warning_issues", 0)),
            ("Р ВР Р…РЎвЂћР С•", summary.get("info_issues", 0)),
        ]
        r = 3
        for key, val in rows:
            ws.cell(row=r, column=1, value=key).font = Font(bold=True)
            ws.cell(row=r, column=2, value=val)
            r += 1
        ws.column_dimensions["A"].width = 28
        ws.column_dimensions["B"].width = 80

        dws = wb.create_sheet("Р Р€РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎРѓРЎвЂљР Р†Р В°")
        headers = ["Р Р€РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎРѓРЎвЂљРІРѕ", "Р СћР С‘Р С—", "HTTP", "Р СљР С•Р В±Р С‘Р В»РЎРЉР Р…Р С•-Р Т‘РЎР‚РЎС“Р В¶Р ВµР В»РЎР‹Р В±Р Р…Р С•", "Р СџРЎР‚Р С•Р В±Р В»Р ВµР С", "Р вЂ”Р В°Р С–РЎР‚РЎС“Р В·Р С”Р В° (мс)", "Р РЋР С”РЎР‚Р С‘Р Р…РЎв‚¬Р С•РЎвЂљ", "Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ"]
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
                d.get("status_code", "РЅ/Рґ"),
                "Р вЂќР В°" if d.get("mobile_friendly") else "Р СњР ВµРЎвЂљ",
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

        iws = wb.create_sheet("Р СџРЎР‚Р С•Р В±Р В»Р ВµР СРЎвЂ№")
        issue_headers = ["Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ", "Р Р€РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎРѓРЎвЂљРІРѕ", "Код", "Р СџРЎР‚Р С•Р В±Р В»Р ВµР СР В°", "Р вЂќР ВµРЎвЂљР В°Р В»Р С‘"]
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

        rws = wb.create_sheet("Р В Р ВµР С”Р С•Р СР ВµР Р…Р Т‘Р В°РЎвЂ Р С‘Р С‘")
        self._apply_style(rws.cell(row=1, column=1, value="Р В Р ВµР С”Р С•Р СР ВµР Р…Р Т‘Р В°РЎвЂ Р С‘РЎРЏ"), header_style)
        for idx, rec in enumerate(recommendations, start=2):
            self._apply_style(rws.cell(row=idx, column=1, value=rec), cell_style)
        rws.column_dimensions["A"].width = 160
        rws.freeze_panes = "A2"

        sws = wb.create_sheet("Р РЋР С”РЎР‚Р С‘Р Р…РЎв‚¬Р С•РЎвЂљРЎвЂ№")
        shot_headers = ["Р Р€РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎРѓРЎвЂљРІРѕ", "Р Вмя РЎРѓР С”РЎР‚Р С‘Р Р…РЎв‚¬Р С•РЎвЂљР В°", "Р СџРЎС“РЎвЂљРЎРЉ", "URL"]
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
        report_url = data.get("url", "РЅ/Рґ")
        summary = results.get("summary", {}) or {}
        bot_rows = results.get("bot_rows", []) or []
        bot_results = results.get("bot_results", {}) or {}
        category_stats = results.get("category_stats", []) or []
        issues = results.get("issues", []) or []
        recommendations = results.get("recommendations", []) or []

        ws = wb.active
        ws.title = "Р РЋР Р†Р С•Р Т‘Р С”Р В°"
        ws["A1"] = "Р С›РЎвЂљРЎвЂЎР ВµРЎвЂљ по Р Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р Р…Р С•РЎРѓРЎвЂљР С‘ Р В±Р С•РЎвЂљРѕРІ"
        ws["A1"].font = Font(bold=True, size=16)
        ws.merge_cells("A1:E1")

        summary_rows = [
            ("URL", report_url),
            ("Р вЂќР Р†Р С‘Р В¶ок", results.get("engine", "legacy")),
            ("Р вЂќР С•Р СР ВµР Р…", results.get("domain", "")),
            ("Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚Р ВµР Р…Р С• Р В±Р С•РЎвЂљРѕРІ", len(results.get("bots_checked", []) or list(bot_results.keys()))),
            ("Р вЂќР С•РЎРѓРЎвЂљСѓРїРЅРѕ", summary.get("accessible", 0)),
            ("Р СњР ВµР Т‘Р С•РЎРѓРЎвЂљСѓРїРЅРѕ", summary.get("unavailable", 0)),
            ("Р РЋ Р С”Р С•Р Р…РЎвЂљР ВµР Р…РЎвЂљРѕРј", summary.get("with_content", 0)),
            ("Р вЂР ВµР В· Р С”Р С•Р Р…РЎвЂљР ВµР Р…РЎвЂљР В°", summary.get("without_content", 0)),
            ("Р вЂ”Р В°Р С—РЎР‚Р ВµРЎвЂ°Р ВµР Р…Р С• robots", summary.get("robots_disallowed", 0)),
            ("Р вЂ”Р В°Р С—РЎР‚Р ВµРЎвЂљ X-Robots", summary.get("x_robots_forbidden", 0)),
            ("Р вЂ”Р В°Р С—РЎР‚Р ВµРЎвЂљ Meta Robots", summary.get("meta_forbidden", 0)),
            ("Р РЋРЎР‚Р ВµР Т‘Р Р…Р С‘Р в„– Р С•РЎвЂљР Р†Р ВµРЎвЂљ (мс)", summary.get("avg_response_time_ms", "")),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 90

        results_ws = wb.create_sheet("Р В Р ВµР В·РЎС“Р В»РЎРЉРЎвЂљР В°РЎвЂљРЎвЂ№ Р В±Р С•РЎвЂљРѕРІ")
        result_headers = [
            "Р вЂР С•РЎвЂљ",
            "Р С™Р В°РЎвЂљР ВµР С–Р С•РЎР‚Р С‘РЎРЏ",
            "HTTP",
            "Р вЂќР С•РЎРѓРЎвЂљРЎС“Р С—Р ВµР Р…",
            "Р вЂўРЎРѓРЎвЂљРЎРЉ Р С”Р С•Р Р…РЎвЂљР ВµР Р…РЎвЂљ",
            "Р В Р В°Р В·РЎР‚Р ВµРЎв‚¬Р ВµР Р… robots",
            "X-Robots-Tag",
            "Р вЂ”Р В°Р С—РЎР‚Р ВµРЎвЂљ X-Robots",
            "Meta Robots",
            "Р вЂ”Р В°Р С—РЎР‚Р ВµРЎвЂљ Meta",
            "Р С›РЎвЂљР Р†Р ВµРЎвЂљ (мс)",
            "Р В¤Р С‘Р Р…Р В°Р В»РЎРЉР Р…РЎвЂ№Р в„– URL",
            "Р С›РЎв‚¬Р С‘Р В±Р С”Р В°",
            "Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ",
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
                "Р вЂќР В°" if item.get("accessible") else "Р СњР ВµРЎвЂљ",
                "Р вЂќР В°" if item.get("has_content") else "Р СњР ВµРЎвЂљ",
                "Р вЂќР В°" if item.get("robots_allowed") is True else ("Р СњР ВµРЎвЂљ" if item.get("robots_allowed") is False else "РЅ/Рґ"),
                item.get("x_robots_tag", ""),
                "Р вЂќР В°" if item.get("x_robots_forbidden") else "Р СњР ВµРЎвЂљ",
                item.get("meta_robots", ""),
                "Р вЂќР В°" if item.get("meta_forbidden") else "Р СњР ВµРЎвЂљ",
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
        category_headers = ["Р С™Р В°РЎвЂљР ВµР С–Р С•РЎР‚Р С‘РЎРЏ", "Р вЂ™РЎРѓР Вµго", "Р вЂќР С•РЎРѓРЎвЂљСѓРїРЅРѕ", "Р РЋ Р С”Р С•Р Р…РЎвЂљР ВµР Р…РЎвЂљРѕРј", "Р С›Р С–РЎР‚Р В°Р Р…Р С‘РЎвЂЎР С‘Р Р†Р В°РЎР‹РЎвЂ°Р С‘Р Вµ Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С‘Р Р†РЎвЂ№", "Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ"]
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

        issues_ws = wb.create_sheet("Р СџРЎР‚Р С•Р В±Р В»Р ВµР СРЎвЂ№")
        issue_headers = ["Р РЋР ВµРЎР‚РЎРЉР ВµР В·Р Р…Р С•РЎРѓРЎвЂљРЎРЉ", "Р вЂР С•РЎвЂљ", "Р С™Р В°РЎвЂљР ВµР С–Р С•РЎР‚Р С‘РЎРЏ", "Р вЂ”Р В°Р С–Р С•Р В»овок", "Р вЂќР ВµРЎвЂљР В°Р В»Р С‘"]
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

        rec_ws = wb.create_sheet("Р В Р ВµР С”Р С•Р СР ВµР Р…Р Т‘Р В°РЎвЂ Р С‘Р С‘")
        self._apply_style(rec_ws.cell(row=1, column=1, value="Р В Р ВµР С”Р С•Р СР ВµР Р…Р Т‘Р В°РЎвЂ Р С‘РЎРЏ"), header_style)
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

        def bool_icon(value: Any, positive: str = "вњ…", negative: str = "вќЊ") -> str:
            return positive if bool(value) else negative

        def icon_score(value: Any, low: float = 60.0, high: float = 80.0) -> str:
            v = to_float(value, 0.0)
            if v >= high:
                return f"вњ… {v:.0f}"
            if v >= low:
                return f"вљ пёЏ {v:.0f}"
            return f"вќЊ {v:.0f}"

        def icon_count(count: int, low: float = 20.0, high: float = 50.0) -> str:
            # Smaller count is better; use inverted score scale.
            score = 100.0 - min(100.0, float(count) * 5.0)
            if score >= high:
                return f"вњ… {count}"
            if score >= low:
                return f"вљ пёЏ {count}"
            return f"вќЊ {count}"

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
                    recs.append("РЈР±СЂР°С‚СЊ noindex/РёСЃРїСЂР°РІРёС‚СЊ СЃС‚Р°С‚ус/robots")
                if to_float(page.get("health_score"), 100.0) < 60:
                    recs.append("РџРѕРґРЅСЏС‚СЊ С‚РµС….РєР°С‡РµСЃС‚во Рё РєРѕРЅС‚РµРЅС‚ РґРѕ 60+")
                if not page_issues and not recs:
                    return "OK"
                if recs:
                    return ok_if_empty(recs)
                issue_titles = [str(i.get("title") or i.get("code") or "") for i in page_issues if (i.get("title") or i.get("code"))]
                return ok_if_empty(issue_titles)

            if tab == "hierarchy":
                status = str(page.get("h_hierarchy") or "").lower()
                if "wrong start" in status:
                    return "Р”РѕР±Р°РІСЊС‚Рµ <h1> РІ РЅР°С‡Р°Р»Рѕ основного РєРѕРЅС‚РµРЅС‚Р°"
                if "level skip" in status:
                    return "РСЃРїСЂР°РІСЊС‚Рµ РїСЂРѕРїСѓСЃРєРё СѓСЂРѕРІРЅРµР№ (H1->H2->H3)"
                if "multiple h1" in status:
                    return "РћСЃС‚Р°РІСЊС‚Рµ С‚РѕР»ько 1 H1, РѕСЃС‚Р°Р»СЊРЅС‹Рµ Р·Р°РјРµРЅРёС‚Рµ РЅР° H2"
                if "missing h1" in status:
                    return "Р”РѕР±Р°РІСЊС‚Рµ РѕРґРёРЅ РѕРїРёСЃР°С‚РµР»СЊРЅС‹Р№ H1"
                return "OK"

            if tab == "onpage":
                title_len = int(page.get("title_len") or len(str(page.get("title") or "")))
                desc_len = int(page.get("description_len") or len(str(page.get("meta_description") or "")))
                if title_len < 30 or title_len > 60:
                    recs.append("Title 30-60 СЃРёРјРІРѕР»ов, СѓРЅРёРєР°Р»СЊРЅС‹Р№")
                if desc_len < 50 or desc_len > 160:
                    recs.append("Meta 100-160 СЃРёРјРІРѕР»ов СЃ CTA")
                if int(page.get("h1_count") or 0) != 1:
                    recs.append("РћСЃС‚Р°РІРёС‚СЊ 1 H1")
                if (page.get("canonical_status") or "") in ("missing", "external", "invalid"):
                    recs.append("РџРѕСЃС‚Р°РІРёС‚СЊ РєРѕСЂСЂРµРєС‚РЅС‹Р№ canonical")
                if int(page.get("duplicate_title_count") or 0) > 1:
                    recs.append("РЈРЅРёРєР°Р»РёР·РёСЂРѕРІР°С‚СЊ Title")
                if int(page.get("duplicate_description_count") or 0) > 1:
                    recs.append("РЈРЅРёРєР°Р»РёР·РёСЂРѕРІР°С‚СЊ Meta")
                return ok_if_empty(recs)

            if tab == "content":
                unique_percent = to_float(page.get("unique_percent"), 0.0)
                words = int(page.get("word_count") or 0)
                if unique_percent < 30:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РЅРѕРІС‹Рµ С‚РµРєСЃС‚РѕРІС‹Рµ Р±Р»РѕРєРё (СѓРЅРёРєР°Р»СЊРЅРѕСЃС‚СЊ <30%)")
                elif unique_percent < 50:
                    recs.append("РџРѕРІС‹СЃРёС‚СЊ СѓРЅРёРєР°Р»СЊРЅРѕСЃС‚СЊ >50%")
                if words < 300:
                    recs.append("РЈРІРµР»РёС‡РёС‚СЊ РѕР±СЉРµРј РґРѕ 300+ СЃР»ов")
                if to_float(page.get("toxicity_score"), 0.0) > 40:
                    recs.append("РЎРЅРёР·РёС‚СЊ keyword-stuffing/СЃРїР°Рј")
                return ok_if_empty(recs)

            if tab == "technical":
                if not page.get("indexable"):
                    recs.append("РЎРґРµР»Р°С‚СЊ СЃС‚СЂР°РЅРёС†Сѓ РёРЅРґРµРєСЃРёСЂСѓРµРјРѕР№")
                if not page.get("is_https"):
                    recs.append("Р’РєР»СЋС‡РёС‚СЊ HTTPS")
                if not page.get("compression_enabled"):
                    recs.append("Р’РєР»СЋС‡РёС‚СЊ gzip/br")
                if not page.get("cache_enabled"):
                    recs.append("РќР°СЃС‚СЂРѕРёС‚СЊ Cache-Control")
                if (page.get("canonical_status") or "") in ("missing", "external", "invalid"):
                    recs.append("РџСЂРѕРІРµСЂРёС‚СЊ canonical")
                if len(page.get("deprecated_tags") or []) > 0:
                    recs.append("РЈРґР°Р»РёС‚СЊ СѓСЃС‚Р°СЂРµРІС€РёРµ С‚РµРіРё")
                rt = page.get("response_time_ms")
                if rt is not None and int(rt) > 2000:
                    recs.append("РЈСЃРєРѕСЂРёС‚СЊ РѕС‚РІРµС‚ СЃРµСЂРІРµСЂР°")
                return ok_if_empty(recs)

            if tab == "eeat":
                eeat = to_float(page.get("eeat_score"), 0.0)
                if eeat < 50:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ Р°РІС‚РѕСЂР°, Р±РёРѕ, РёСЃС‚РѕС‡РЅРёРєРё, РєРµР№СЃС‹")
                elif eeat < 70:
                    recs.append("РЈСЃРёР»РёС‚СЊ Р°РІС‚РѕСЂСЃС‚во Рё РґРѕРІРµСЂРёРµ")
                if not page.get("has_author_info"):
                    recs.append("Р‘Р»ок Р°РІС‚РѕСЂР° + РєРѕРЅС‚Р°РєС‚/СЃРѕС†СЃРµС‚Рё")
                if not page.get("has_reviews"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РѕС‚Р·С‹РІС‹/РєРµР№СЃС‹")
                return ok_if_empty(recs)

            if tab == "trust":
                if not page.get("has_contact_info"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РєРѕРЅС‚Р°РєС‚С‹/Р°РґСЂРµСЃ/С‚РµР»РµС„РѕРЅ")
                if not page.get("has_legal_docs"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ СЋСЂРёРґРёС‡РµСЃРєРёРµ РґРѕРєСѓРјРµРЅС‚С‹")
                if not page.get("has_reviews"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РѕС‚Р·С‹РІС‹")
                if not page.get("trust_badges"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ trust-Р±РµР№РґР¶Рё/СЃРµСЂС‚РёС„РёРєР°С‚С‹")
                return ok_if_empty(recs)

            if tab == "health":
                if to_float(page.get("health_score"), 100.0) < 60:
                    recs.append("РџРѕРґРЅСЏС‚СЊ РѕР±С‰РёР№ Health РґРѕ 60+")
                if not page.get("indexable"):
                    recs.append("РРЅРґРµРєСЃРёСЂСѓРµРјРѕСЃС‚СЊ")
                if int(page.get("duplicate_title_count") or 0) > 1:
                    recs.append("РЈРЅРёРєР°Р»РёР·РёСЂРѕРІР°С‚СЊ Title")
                if int(page.get("duplicate_description_count") or 0) > 1:
                    recs.append("РЈРЅРёРєР°Р»РёР·РёСЂРѕРІР°С‚СЊ Meta")
                return ok_if_empty(recs)

            if tab == "links":
                if page.get("orphan_page"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РІРЅСѓС‚СЂРµРЅРЅРёРµ СЃСЃС‹Р»РєРё РЅР° СЃС‚СЂР°РЅРёС†Сѓ")
                if int(page.get("outgoing_internal_links") or 0) == 0:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ СЃСЃС‹Р»РєРё РЅР° СЂРµР»РµРІР°РЅС‚РЅС‹Рµ СЃС‚СЂР°РЅРёС†С‹")
                return ok_if_empty(recs)

            if tab == "images":
                img_opt = page.get("images_optimization") or {}
                if int(img_opt.get("no_alt") or page.get("images_without_alt") or 0) > 0:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ ALT РґР»СЏ РёР·РѕР±СЂР°Р¶РµРЅРёР№")
                if int(img_opt.get("no_width_height") or 0) > 0:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ width/height")
                if int(img_opt.get("no_lazy_load") or 0) > 0:
                    recs.append("Р’РєР»СЋС‡РёС‚СЊ lazy-loading")
                return ok_if_empty(recs)

            if tab == "external":
                total = int(page.get("outgoing_external_links") or 0)
                if total == 0:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ СЂРµР»РµРІР°РЅС‚РЅС‹Рµ РІРЅРµС€РЅРёРµ РёСЃС‚РѕС‡РЅРёРєРё")
                return ok_if_empty(recs)

            if tab == "structured":
                if int(page.get("structured_data") or 0) == 0:
                    recs.append("Р’РЅРµРґСЂРёС‚СЊ schema.org (JSON-LD)")
                if int(page.get("hreflang_count") or 0) == 0:
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ hreflang РґР»СЏ СЏР·С‹РєРѕРІС‹С… РІРµСЂСЃРёР№")
                return ok_if_empty(recs)

            if tab == "keywords":
                if not (page.get("top_keywords") or page.get("top_terms") or page.get("tf_idf_keywords")):
                    recs.append("РЈС‚РѕС‡РЅРёС‚СЊ СЃРµРјР°РЅС‚РёРєСѓ Рё РєР»СЋС‡Рё")
                if to_float(page.get("toxicity_score"), 0.0) > 40:
                    recs.append("РЎРЅРёР·РёС‚СЊ РїРµСЂРµСЃРїР°Рј РєР»СЋС‡Р°РјРё")
                return ok_if_empty(recs)

            if tab == "topics":
                if not page.get("topic_hub"):
                    recs.append("РЎРІСЏР·Р°С‚СЊ СЃ СЂРµР»РµРІР°РЅС‚РЅС‹Рј С…Р°Р±ом")
                if not page.get("topic_label"):
                    recs.append("РћРїСЂРµРґРµР»РёС‚СЊ РєР»Р°СЃС‚РµСЂ С‚РµРјС‹")
                return ok_if_empty(recs)

            if tab == "advanced":
                freshness = page.get("content_freshness_days")
                if freshness is not None and int(freshness) > 365:
                    recs.append("РћР±РЅРѕРІРёС‚СЊ РєРѕРЅС‚РµРЅС‚")
                if bool(page.get("hidden_content")):
                    recs.append("РЈР±СЂР°С‚СЊ СЃРєСЂС‹С‚С‹Р№ РєРѕРЅС‚РµРЅС‚")
                if bool(page.get("cloaking_detected")):
                    recs.append("РСЃРєР»СЋС‡РёС‚СЊ РєР»РѕР°РєРёРЅРі")
                return ok_if_empty(recs)

            if tab == "link_quality":
                if to_float(page.get("link_quality_score"), 0.0) < 60:
                    recs.append("РЈСЃРёР»РёС‚СЊ РІРЅСѓС‚СЂРµннюю РїРµСЂРµР»РёРЅРєРѕРІРєСѓ")
                if page.get("orphan_page"):
                    recs.append("Р”РѕР±Р°РІРёС‚СЊ РІС…РѕРґСЏС‰РёРµ СЃСЃС‹Р»РєРё")
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
            "URL", "Title", "Title len", "Meta description", "Meta len", "H1 count", "H1 text",
            "Canonical URL", "Canonical status", "Meta robots", "X-Robots", "Schema count",
            "JSON-LD", "Microdata", "RDFa", "Structured types", "Hreflang count",
            "Breadcrumbs", "Mobile hint", "Title dup", "Desc dup", "OnPage solution", "Severity",
        ]
        onpage_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            sdetail = page.get("structured_data_detail") or {}
            onpage_rows.append([
                page.get("url", ""),
                page.get("title", ""),
                int(page.get("title_len") or len(str(page.get("title") or ""))),
                page.get("meta_description", ""),
                int(page.get("description_len") or len(str(page.get("meta_description") or ""))),
                page.get("h1_count", ""),
                page.get("h1_text", ""),
                page.get("canonical", ""),
                page.get("canonical_status", ""),
                page.get("meta_robots", ""),
                page.get("x_robots_tag", ""),
                page.get("structured_data", 0),
                sdetail.get("json_ld", 0),
                sdetail.get("microdata", 0),
                sdetail.get("rdfa", 0),
                ", ".join((page.get("structured_types") or [])[:6]),
                page.get("hreflang_count", 0),
                page.get("breadcrumbs", ""),
                page.get("mobile_friendly_hint", ""),
                page.get("duplicate_title_count", 0),
                page.get("duplicate_description_count", 0),
                page_solution("onpage", page),
                sev,
            ])
        fill_sheet(
            "2_OnPage+Structured",
            onpage_headers,
            onpage_rows,
            severity_idx=22,
            widths=[56, 28, 10, 32, 10, 10, 22, 32, 16, 20, 20, 12, 8, 10, 8, 36, 10, 10, 10, 10, 10, 48, 10],
        )

        # Sheet 3: Technical
        tech_headers = [
            "URL", "Final URL", "Status", "Response ms", "Size KB", "HTML bytes", "DOM nodes", "Redirects",
            "HTTPS", "Compression", "Compression algo", "Cache enabled", "Cache-Control",
            "Last-Modified", "Freshness days", "HTML quality score", "Deprecated tags count",
            "Indexability reason", "Technical solution", "Severity",
        ]
        tech_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            tech_rows.append([
                page.get("url", ""),
                page.get("final_url", ""),
                page.get("status_code", ""),
                page.get("response_time_ms", ""),
                page.get("content_kb", round(to_float(page.get("html_size_bytes"), 0.0) / 1024.0, 1)),
                page.get("html_size_bytes", ""),
                page.get("dom_nodes_count", ""),
                page.get("redirect_count", 0),
                page.get("is_https", ""),
                page.get("compression_enabled", ""),
                page.get("compression_algorithm", ""),
                page.get("cache_enabled", ""),
                page.get("cache_control", ""),
                page.get("last_modified", ""),
                page.get("content_freshness_days", ""),
                page.get("html_quality_score", ""),
                len(page.get("deprecated_tags") or []),
                page.get("indexability_reason", ""),
                page_solution("technical", page),
                sev,
            ])
        fill_sheet(
            "3_Technical",
            tech_headers,
            tech_rows,
            severity_idx=19,
            widths=[50, 50, 10, 12, 10, 12, 10, 10, 8, 10, 14, 10, 22, 24, 14, 14, 12, 18, 46, 10],
        )

        # Sheet 4: Content + AI
        content_headers = [
            "URL", "Word count", "Unique words", "Unique %", "Lexical diversity", "Readability score",
            "Avg sentence len", "Avg word len", "Complex words %", "Keyword stuffing score",
            "Content density %", "Boilerplate %", "Toxicity score", "Filler ratio",
            "Filler phrases", "AI markers count", "AI markers list", "AI marker sample",
            "Content solution", "Severity",
        ]
        content_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            content_rows.append([
                page.get("url", ""),
                page.get("word_count", 0),
                page.get("unique_word_count", 0),
                page.get("unique_percent", 0),
                page.get("lexical_diversity", 0),
                page.get("readability_score", 0),
                page.get("avg_sentence_length", 0),
                page.get("avg_word_length", 0),
                page.get("complex_words_percent", 0),
                page.get("keyword_stuffing_score", 0),
                page.get("content_density", 0),
                page.get("boilerplate_percent", 0),
                page.get("toxicity_score", 0),
                page.get("filler_ratio", 0),
                len(page.get("filler_phrases") or []),
                page.get("ai_markers_count", 0),
                ", ".join((page.get("ai_markers_list") or [])[:10]),
                page.get("ai_marker_sample", "") or "No sample",
                page_solution("content", page),
                sev,
            ])
        fill_sheet(
            "4_Content+AI",
            content_headers,
            content_rows,
            severity_idx=19,
            widths=[52, 10, 12, 10, 12, 12, 12, 10, 12, 12, 12, 12, 10, 10, 12, 12, 50, 62, 46, 10],
        )

        # Sheet 5: Link Graph
        link_headers = [
            "URL", "Incoming int", "Outgoing int", "Outgoing ext", "Orphan",
            "Topic hub", "PageRank", "Weak anchor ratio", "Anchor quality", "Link quality",
            "Follow links total", "Nofollow links total", "Semantic links count", "Linking solution", "Severity",
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
                page.get("anchor_text_quality_score", 0),
                page.get("link_quality_score", 0),
                page.get("follow_links_total", 0),
                page.get("nofollow_links_total", 0),
                len(page.get("semantic_links") or []),
                page_solution("link_quality", page),
                sev,
            ])
        fill_sheet(
            "5_LinkGraph",
            link_headers,
            link_rows,
            severity_idx=14,
            widths=[50, 12, 12, 12, 10, 10, 10, 14, 12, 12, 14, 16, 14, 46, 10],
        )

        # Sheet 6: Images + External
        img_headers = [
            "URL", "Images total", "Without alt", "No width/height", "No lazy-load", "Image issues total",
            "External total", "External follow", "External nofollow", "Follow ratio %",
            "Images+External solution", "Severity",
        ]
        img_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            img_opt = page.get("images_optimization") or {}
            ext_follow = int(page.get("external_follow_links", 0) or 0)
            ext_nofollow = int(page.get("external_nofollow_links", 0) or 0)
            ext_total = int(page.get("outgoing_external_links", ext_follow + ext_nofollow) or 0)
            follow_ratio = (ext_follow / ext_total * 100.0) if ext_total > 0 else 0.0
            no_alt = int(page.get("images_without_alt", img_opt.get("no_alt", 0)) or 0)
            no_wh = int(img_opt.get("no_width_height", 0) or 0)
            no_lazy = int(img_opt.get("no_lazy_load", 0) or 0)
            img_rows.append([
                page.get("url", ""),
                page.get("images_count", img_opt.get("total", 0)),
                no_alt,
                no_wh,
                no_lazy,
                (no_alt + no_wh + no_lazy),
                ext_total,
                ext_follow,
                ext_nofollow,
                round(follow_ratio, 1),
                f"{page_solution('images', page)}; {page_solution('external', page)}",
                sev,
            ])
        fill_sheet(
            "6_Images+External",
            img_headers,
            img_rows,
            severity_idx=11,
            widths=[52, 10, 10, 12, 12, 12, 12, 12, 14, 12, 58, 10],
        )

        # Sheet 7: Hierarchy + Errors
        issue_headers = [
            "URL", "Hierarchy status", "Hierarchy problems", "Total headers", "Hierarchy H1 count",
            "Heading outline", "Code", "Issue title", "Issue details", "Hierarchy solution", "Severity",
        ]
        issue_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            h_details = page.get("h_details") or {}
            h_outline = h_details.get("heading_outline") or []
            outline_text = " | ".join(f"H{item.get('level')}:{item.get('text')}" for item in h_outline[:8])
            page_issues = issues_by_url.get(page.get("url", ""), [])
            hierarchy_issue = next(
                (i for i in page_issues if "h1" in str(i.get("code", "")).lower() or "hierarchy" in str(i.get("code", "")).lower()),
                page_issues[0] if page_issues else {},
            )
            issue_rows.append([
                page.get("url", ""),
                page.get("h_hierarchy", ""),
                hierarchy_problem(page),
                h_details.get("total_headers", sum((page.get("heading_distribution") or {}).values())),
                page.get("h1_count", 0),
                outline_text,
                hierarchy_issue.get("code", ""),
                hierarchy_issue.get("title", ""),
                hierarchy_issue.get("details", ""),
                page_solution("hierarchy", page, page_issues),
                sev,
            ])
        fill_sheet(
            "7_HierarchyErrors",
            issue_headers,
            issue_rows,
            severity_idx=10,
            widths=[50, 18, 32, 12, 10, 72, 20, 28, 40, 48, 10],
        )

        # Sheet 8: Keywords
        keyword_headers = [
            "URL", "Topic", "Top terms (TF-IDF)", "Top keywords", "TF-IDF #1", "TF-IDF #2", "TF-IDF #3",
            "Keyword density profile", "Keyword solution", "Severity",
        ]
        keyword_rows = []
        for page in pages:
            sev = infer_page_severity(page)
            url = page.get("url", "")
            top_terms = tfidf_by_url.get(url, page.get("top_terms", [])) or list((page.get("tf_idf_keywords") or {}).keys())
            kw_profile = page.get("keyword_density_profile") or {}
            kw_profile_text = ", ".join(f"{k}:{v}%" for k, v in list(kw_profile.items())[:6])
            keyword_rows.append([
                url,
                page.get("topic_label", ""),
                ", ".join(top_terms[:10]),
                ", ".join((page.get("top_keywords") or [])[:8]),
                top_terms[0] if len(top_terms) > 0 else "",
                top_terms[1] if len(top_terms) > 1 else "",
                top_terms[2] if len(top_terms) > 2 else "",
                kw_profile_text,
                page_solution("keywords", page),
                sev,
            ])
        fill_sheet("8_Keywords", keyword_headers, keyword_rows, severity_idx=9, widths=[48, 16, 42, 36, 14, 14, 14, 42, 46, 10])

        # Optional full-mode optimized deep sheets (no compatibility duplication).
        if str(mode).lower() == "full":
            indexability_headers = [
                "URL", "Status", "Indexable", "Noindex", "Blocked by robots",
                "Indexability reason", "Canonical URL", "Canonical status",
                "Meta robots", "X-Robots-Tag", "Indexability score", "Indexability solution", "Severity",
            ]
            indexability_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                status_code = int(page.get("status_code") or 0)
                is_indexable = bool(page.get("indexable"))
                noindex_flag = bool(page.get("noindex"))
                blocked_robots = bool(page.get("blocked_by_robots"))
                canonical_status = str(page.get("canonical_status") or "").lower()
                score = 100
                if status_code >= 400:
                    score -= 40
                if not is_indexable:
                    score -= 30
                if noindex_flag:
                    score -= 20
                if blocked_robots:
                    score -= 20
                if canonical_status in ("missing", "external", "invalid"):
                    score -= 10
                score = max(0, score)
                indexability_rows.append([
                    page.get("url", ""),
                    status_code,
                    is_indexable,
                    noindex_flag,
                    blocked_robots,
                    page.get("indexability_reason", ""),
                    page.get("canonical", ""),
                    page.get("canonical_status", ""),
                    page.get("meta_robots", ""),
                    page.get("x_robots_tag", ""),
                    score,
                    page_solution("technical", page),
                    sev,
                ])
            fill_sheet(
                "9_Indexability",
                indexability_headers,
                indexability_rows,
                severity_idx=12,
                widths=[52, 10, 10, 10, 14, 20, 30, 16, 20, 20, 14, 46, 10],
            )

            structured_headers = [
                "URL", "Structured total", "JSON-LD", "Microdata", "RDFa",
                "Structured types", "Hreflang", "Breadcrumbs",
                "FAQ schema", "Product schema", "Article schema",
                "Structured coverage %", "Structured solution", "Severity",
            ]
            structured_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                detail = page.get("structured_data_detail") or {}
                types = [str(t).lower() for t in (page.get("structured_types") or [])]
                has_faq = any("faq" in t for t in types)
                has_product = any("product" in t for t in types)
                has_article = any("article" in t for t in types)
                coverage = 0.0
                coverage += min(70.0, float(page.get("structured_data", 0) or 0) * 20.0)
                if int(page.get("hreflang_count", 0) or 0) > 0:
                    coverage += 15.0
                if bool(page.get("breadcrumbs")):
                    coverage += 15.0
                structured_rows.append([
                    page.get("url", ""),
                    page.get("structured_data", 0),
                    detail.get("json_ld", 0),
                    detail.get("microdata", 0),
                    detail.get("rdfa", 0),
                    ", ".join((page.get("structured_types") or [])[:8]),
                    page.get("hreflang_count", 0),
                    page.get("breadcrumbs", ""),
                    has_faq,
                    has_product,
                    has_article,
                    round(min(100.0, coverage), 1),
                    page_solution("structured", page),
                    sev,
                ])
            fill_sheet(
                "10_StructuredData",
                structured_headers,
                structured_rows,
                severity_idx=13,
                widths=[50, 12, 10, 10, 8, 40, 10, 12, 10, 12, 12, 18, 48, 10],
            )

            trust_eeat_headers = [
                "URL", "Trust score", "EEAT score", "Expertise", "Authority",
                "Trustworthiness", "Experience", "Author info", "Contact", "Legal", "Reviews", "Badges",
                "Trust gap", "Trust+EEAT solution", "Severity",
            ]
            trust_eeat_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                comp = page.get("eeat_components") or {}
                trust_score = to_float(page.get("trust_score"), 0.0)
                eeat_score = to_float(page.get("eeat_score"), 0.0)
                trust_gap = round(max(0.0, 70.0 - ((trust_score + eeat_score) / 2.0)), 1)
                trust_eeat_rows.append([
                    page.get("url", ""),
                    trust_score,
                    eeat_score,
                    comp.get("expertise", ""),
                    comp.get("authoritativeness", ""),
                    comp.get("trustworthiness", ""),
                    comp.get("experience", ""),
                    page.get("has_author_info", ""),
                    page.get("has_contact_info", ""),
                    page.get("has_legal_docs", ""),
                    page.get("has_reviews", ""),
                    page.get("trust_badges", ""),
                    trust_gap,
                    f"{page_solution('eeat', page)}; {page_solution('trust', page)}",
                    sev,
                ])
            fill_sheet(
                "11_Trust_EEAT",
                trust_eeat_headers,
                trust_eeat_rows,
                severity_idx=14,
                widths=[46, 10, 10, 10, 10, 12, 10, 10, 10, 10, 10, 10, 12, 54, 10],
            )

            topics_headers = [
                "URL", "Topic", "Is hub", "Incoming links", "Outgoing int links", "Semantic links count",
                "Suggested links", "Semantic links detail", "Top terms", "Top keywords", "Topical depth score",
                "Topics solution", "Severity",
            ]
            topics_rows = []
            semantic_by_source: Dict[str, List[Dict[str, Any]]] = {}
            for row in (pipeline.get("semantic_linking_map") or []):
                semantic_by_source.setdefault(row.get("source_url", ""), []).append(row)
            for page in pages:
                sev = infer_page_severity(page)
                src = page.get("url", "")
                semantic_rows = semantic_by_source.get(src, [])
                if not semantic_rows:
                    semantic_rows = page.get("semantic_links") or []
                summary = []
                for item in semantic_rows[:4]:
                    target = item.get("target_url", "")
                    topic = item.get("topic") or item.get("suggested_anchor") or ""
                    reason = item.get("reason", "")
                    summary.append(f"[{topic}] {target} {reason}".strip())
                semantic_count = len(semantic_rows)
                outgoing_internal = int(page.get("outgoing_internal_links", 0) or 0)
                topical_depth = min(100.0, semantic_count * 20.0 + outgoing_internal * 3.0 + (15.0 if page.get("topic_hub") else 0.0))
                tfidf_terms = tfidf_by_url.get(src, page.get("top_terms", [])) or []
                topics_rows.append([
                    src,
                    page.get("topic_label", ""),
                    page.get("topic_hub", ""),
                    page.get("incoming_internal_links", 0),
                    outgoing_internal,
                    semantic_count,
                    "; ".join([item.get("target_url", "") for item in semantic_rows[:5]]),
                    "\n".join(summary),
                    ", ".join(tfidf_terms[:8]),
                    ", ".join((page.get("top_keywords") or [])[:8]),
                    round(topical_depth, 1),
                    page_solution("topics", page),
                    sev,
                ])
            fill_sheet(
                "12_Topics_Semantics",
                topics_headers,
                topics_rows,
                severity_idx=12,
                widths=[42, 16, 10, 12, 14, 14, 42, 58, 36, 36, 14, 42, 10],
            )

            ai_headers = [
                "URL", "AI markers", "AI markers list", "Marker sample",
                "AI risk score", "AI risk level", "Toxicity score", "Filler ratio", "Humanization hint", "Severity",
            ]
            ai_rows = []
            for page in pages:
                sev = infer_page_severity(page)
                markers_count = int(page.get("ai_markers_count", 0) or 0)
                toxicity = to_float(page.get("toxicity_score"), 0.0)
                filler_ratio = to_float(page.get("filler_ratio"), 0.0)
                ai_risk = min(100.0, markers_count * 6.0 + toxicity * 0.7 + filler_ratio * 0.5)
                if ai_risk >= 70:
                    risk_level = "high"
                elif ai_risk >= 40:
                    risk_level = "medium"
                else:
                    risk_level = "low"
                ai_rows.append([
                    page.get("url", ""),
                    markers_count,
                    ", ".join((page.get("ai_markers_list") or [])[:12]) or "none",
                    page.get("ai_marker_sample", "") or "No text sample available",
                    round(ai_risk, 1),
                    risk_level,
                    toxicity,
                    filler_ratio,
                    page_solution("content", page),
                    sev,
                ])
            fill_sheet(
                "13_AI_Markers",
                ai_headers,
                ai_rows,
                severity_idx=9,
                widths=[48, 10, 48, 60, 12, 12, 12, 10, 52, 10],
            )

            raw_issue_headers = ["Severity", "URL", "Code", "Category", "Title", "Details", "Affected", "Recommendation"]
            raw_issue_rows = []
            seen_raw = set()
            for issue in issues:
                severity = (issue.get("severity") or "info").lower()
                url = issue.get("url", "")
                code = issue.get("code", "")
                details = issue.get("details", "")
                fingerprint = (severity, url, code, details)
                if fingerprint in seen_raw:
                    continue
                seen_raw.add(fingerprint)
                code_text = str(code)
                category = code_text.split("_", 1)[0] if "_" in code_text else code_text
                raw_issue_rows.append([
                    severity,
                    url,
                    code,
                    category,
                    issue.get("title", ""),
                    details,
                    issue.get("selector") or issue.get("path") or issue.get("field") or "",
                    issue.get("recommendation") or issue.get("fix") or "",
                ])
            fill_sheet(
                "14_Issues_Raw",
                raw_issue_headers,
                raw_issue_rows,
                severity_idx=0,
                widths=[10, 56, 24, 16, 28, 50, 24, 46],
            )

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Dispatch report generation by task type."""
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
