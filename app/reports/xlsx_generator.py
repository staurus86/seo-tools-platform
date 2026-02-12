"""
Excel Report Generator
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from typing import Dict, Any, List
from datetime import datetime
import os

from app.config import settings


class XLSXGenerator:
    """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В Р’В°Р РЋРІР‚С™Р В РЎвЂўР РЋР вЂљ Excel Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™Р В РЎвЂўР В Р вЂ """
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _create_header_style(self):
        """Р В Р Р‹Р В РЎвЂўР В Р’В·Р В РўвЂР В Р’В°Р В Р’ВµР РЋРІР‚С™ Р РЋР С“Р РЋРІР‚С™Р В РЎвЂР В Р’В»Р РЋР Р‰ Р В РўвЂР В Р’В»Р РЋР РЏ Р В Р’В·Р В Р’В°Р В РЎвЂ“Р В РЎвЂўР В Р’В»Р В РЎвЂўР В Р вЂ Р В РЎвЂќР В РЎвЂўР В Р вЂ """
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
        """Р В Р Р‹Р В РЎвЂўР В Р’В·Р В РўвЂР В Р’В°Р В Р’ВµР РЋРІР‚С™ Р РЋР С“Р РЋРІР‚С™Р В РЎвЂР В Р’В»Р РЋР Р‰ Р В РўвЂР В Р’В»Р РЋР РЏ Р РЋР РЏР РЋРІР‚РЋР В Р’ВµР В Р’ВµР В РЎвЂќ"""
        return {
            'border': Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        }
    
    def _apply_style(self, cell, style):
        """Р В РЎСџР РЋР вЂљР В РЎвЂР В РЎВР В Р’ВµР В Р вЂ¦Р РЋР РЏР В Р’ВµР РЋРІР‚С™ Р РЋР С“Р РЋРІР‚С™Р В РЎвЂР В Р’В»Р РЋР Р‰ Р В РЎвЂќ Р РЋР РЏР РЋРІР‚РЋР В Р’ВµР В РІвЂћвЂ“Р В РЎвЂќР В Р’Вµ"""
        if 'font' in style:
            cell.font = style['font']
        if 'fill' in style:
            cell.fill = style['fill']
        if 'alignment' in style:
            cell.alignment = style['alignment']
        if 'border' in style:
            cell.border = style['border']
    
    def generate_site_analyze_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В РЎвЂР РЋР вЂљР РЋРЎвЂњР В Р’ВµР РЋРІР‚С™ Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™ Р В Р’В°Р В Р вЂ¦Р В Р’В°Р В Р’В»Р В РЎвЂР В Р’В·Р В Р’В° Р РЋР С“Р В Р’В°Р В РІвЂћвЂ“Р РЋРІР‚С™Р В Р’В°"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Site Analysis"
        
        # Header
        ws['A1'] = 'SEO Site Analysis Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        # Basic info
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')
        ws['A4'] = 'Pages Analyzed:'
        ws['B4'] = data.get('pages_analyzed', 0)
        ws['A5'] = 'Completed:'
        ws['B5'] = data.get('completed_at', 'N/A')
        
        # Results section
        ws['A7'] = 'Results'
        ws['A7'].font = Font(bold=True, size=14)
        
        results = data.get('results', {})
        row = 8
        
        # Headers
        headers = ['Metric', 'Value', 'Status']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=row, column=col, value=header)
            self._apply_style(cell, self._create_header_style())
        
        # Sample data (will be replaced with real data from tools)
        sample_data = [
            ['Total Pages', results.get('total_pages', 0), 'OK'],
            ['Status', results.get('status', 'N/A'), 'OK'],
            ['Summary', results.get('summary', 'N/A'), 'OK']
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
        """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В РЎвЂР РЋР вЂљР РЋРЎвЂњР В Р’ВµР РЋРІР‚С™ Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™ robots.txt"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Robots.txt Check"
        
        ws['A1'] = 'Robots.txt Analysis Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')
        
        results = data.get('results', {})
        ws['A5'] = 'Robots.txt Found:'
        ws['B5'] = 'Yes' if results.get('robots_txt_found') else 'No'
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_sitemap_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate a detailed sitemap validation XLSX report."""
        wb = Workbook()
        header_style = self._create_header_style()
        cell_style = self._create_cell_style()
        results = data.get('results', {}) or {}
        report_url = data.get('url', 'N/A')

        ws = wb.active
        ws.title = "Summary"
        ws['A1'] = 'Sitemap Validation Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:E1')

        summary_rows = [
            ("URL", report_url),
            ("Valid", "Yes" if results.get("valid") else "No"),
            ("HTTP Status", results.get("status_code", "N/A")),
            ("Sitemaps Scanned", results.get("sitemaps_scanned", 0)),
            ("Sitemaps Valid", results.get("sitemaps_valid", 0)),
            ("Total URLs", results.get("urls_count", 0)),
            ("Unique URLs", results.get("unique_urls_count", 0)),
            ("Duplicate URLs", results.get("duplicate_urls_count", 0)),
            ("Invalid URLs", results.get("invalid_urls_count", 0)),
            ("Invalid lastmod", results.get("invalid_lastmod_count", 0)),
            ("Invalid changefreq", results.get("invalid_changefreq_count", 0)),
            ("Invalid priority", results.get("invalid_priority_count", 0)),
            ("Data Size (bytes)", results.get("size", 0)),
        ]
        row = 3
        for key, value in summary_rows:
            ws.cell(row=row, column=1, value=key).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        ws.column_dimensions['A'].width = 28
        ws.column_dimensions['B'].width = 80

        files_ws = wb.create_sheet("Sitemap Files")
        files_headers = [
            "Sitemap URL", "Type", "HTTP", "OK", "URLs",
            "Duplicates", "Size (bytes)", "Errors", "Warnings"
        ]
        for col, header in enumerate(files_headers, 1):
            cell = files_ws.cell(row=1, column=col, value=header)
            self._apply_style(cell, header_style)

        for row_idx, item in enumerate((results.get("sitemap_files", []) or []), start=2):
            values = [
                item.get("sitemap_url", ""),
                item.get("type", ""),
                item.get("status_code", ""),
                "Yes" if item.get("ok") else "No",
                item.get("urls_count", 0),
                item.get("duplicate_count", 0),
                item.get("size_bytes", 0),
                " | ".join(item.get("errors", [])[:5]),
                " | ".join(item.get("warnings", [])[:5]),
            ]
            for col, value in enumerate(values, 1):
                cell = files_ws.cell(row=row_idx, column=col, value=value)
                self._apply_style(cell, cell_style)
        files_ws.freeze_panes = "A2"
        for col, width in enumerate([72, 14, 10, 8, 12, 12, 14, 60, 60], 1):
            files_ws.column_dimensions[get_column_letter(col)].width = width

        errors_ws = wb.create_sheet("Errors")
        errors_ws.cell(row=1, column=1, value="Error")
        self._apply_style(errors_ws.cell(row=1, column=1), header_style)
        for idx, err in enumerate((results.get("errors", []) or []), start=2):
            cell = errors_ws.cell(row=idx, column=1, value=err)
            self._apply_style(cell, cell_style)
        errors_ws.column_dimensions['A'].width = 140
        errors_ws.freeze_panes = "A2"

        warnings_ws = wb.create_sheet("Warnings")
        warnings_ws.cell(row=1, column=1, value="Warning")
        self._apply_style(warnings_ws.cell(row=1, column=1), header_style)
        for idx, warn in enumerate((results.get("warnings", []) or []), start=2):
            cell = warnings_ws.cell(row=idx, column=1, value=warn)
            self._apply_style(cell, cell_style)
        warnings_ws.column_dimensions['A'].width = 140
        warnings_ws.freeze_panes = "A2"

        dup_ws = wb.create_sheet("Duplicates")
        dup_headers = ["URL", "First Sitemap", "Duplicate Sitemap"]
        for col, header in enumerate(dup_headers, 1):
            cell = dup_ws.cell(row=1, column=col, value=header)
            self._apply_style(cell, header_style)
        for row_idx, item in enumerate((results.get("duplicate_details", []) or []), start=2):
            dup_ws.cell(row=row_idx, column=1, value=item.get("url", ""))
            dup_ws.cell(row=row_idx, column=2, value=item.get("first_sitemap", ""))
            dup_ws.cell(row=row_idx, column=3, value=item.get("duplicate_sitemap", ""))
            for col in range(1, 4):
                self._apply_style(dup_ws.cell(row=row_idx, column=col), cell_style)
        dup_ws.freeze_panes = "A2"
        dup_ws.column_dimensions['A'].width = 80
        dup_ws.column_dimensions['B'].width = 60
        dup_ws.column_dimensions['C'].width = 60

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath

    def generate_render_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Generate render audit report."""
        wb = Workbook()
        ws = wb.active
        ws.title = "Render Audit"

        ws['A1'] = 'Render Audit Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')

        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')

        results = data.get('results', {})
        ws['A5'] = 'JS Render Diff:'
        ws['B5'] = 'Yes' if results.get('js_render_diff') else 'No'

        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    def generate_mobile_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В РЎвЂР РЋР вЂљР РЋРЎвЂњР В Р’ВµР РЋРІР‚С™ Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™ Р В РЎВР В РЎвЂўР В Р’В±Р В РЎвЂР В Р’В»Р РЋР Р‰Р В Р вЂ¦Р В РЎвЂўР В РІвЂћвЂ“ Р В РЎвЂ”Р РЋР вЂљР В РЎвЂўР В Р вЂ Р В Р’ВµР РЋР вЂљР В РЎвЂќР В РЎвЂ"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Mobile Check"
        
        ws['A1'] = 'Mobile Compatibility Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')
        
        results = data.get('results', {})
        devices = results.get('devices_tested', [])
        
        ws['A5'] = 'Devices Tested:'
        ws['A6'] = ', '.join(devices) if devices else 'N/A'
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_bot_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В РЎвЂР РЋР вЂљР РЋРЎвЂњР В Р’ВµР РЋРІР‚С™ Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™ Р В РЎвЂ”Р РЋР вЂљР В РЎвЂўР В Р вЂ Р В Р’ВµР РЋР вЂљР В РЎвЂќР В РЎвЂ Р В Р’В±Р В РЎвЂўР РЋРІР‚С™Р В РЎвЂўР В Р вЂ """
        wb = Workbook()
        ws = wb.active
        ws.title = "Bot Check"
        
        ws['A1'] = 'Bot Accessibility Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')
        
        results = data.get('results', {})
        bots = results.get('bots_checked', [])
        
        ws['A5'] = 'Bots Checked:'
        ws['A6'] = ', '.join(bots) if bots else 'N/A'
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_report(self, task_id: str, task_type: str, data: Dict[str, Any]) -> str:
        """Р В РІР‚СљР В Р’ВµР В Р вЂ¦Р В Р’ВµР РЋР вЂљР В РЎвЂР РЋР вЂљР РЋРЎвЂњР В Р’ВµР РЋРІР‚С™ Р В РЎвЂўР РЋРІР‚С™Р РЋРІР‚РЋР В Р’ВµР РЋРІР‚С™ Р В Р вЂ  Р В Р’В·Р В Р’В°Р В Р вЂ Р В РЎвЂР РЋР С“Р В РЎвЂР В РЎВР В РЎвЂўР РЋР С“Р РЋРІР‚С™Р В РЎвЂ Р В РЎвЂўР РЋРІР‚С™ Р РЋРІР‚С™Р В РЎвЂР В РЎвЂ”Р В Р’В° Р В Р’В·Р В Р’В°Р В РўвЂР В Р’В°Р РЋРІР‚РЋР В РЎвЂ"""
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
xlsx_generator = XLSXGenerator()
