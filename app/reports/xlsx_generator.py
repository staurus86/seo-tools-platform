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
    """Генератор Excel отчетов"""
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _create_header_style(self):
        """Создает стиль для заголовков"""
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
        """Создает стиль для ячеек"""
        return {
            'border': Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        }
    
    def _apply_style(self, cell, style):
        """Применяет стиль к ячейке"""
        if 'font' in style:
            cell.font = style['font']
        if 'fill' in style:
            cell.fill = style['fill']
        if 'alignment' in style:
            cell.alignment = style['alignment']
        if 'border' in style:
            cell.border = style['border']
    
    def generate_site_analyze_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует отчет анализа сайта"""
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
        """Генерирует отчет robots.txt"""
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
        """Генерирует отчет sitemap"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Sitemap Validation"
        
        ws['A1'] = 'Sitemap Validation Report'
        ws['A1'].font = Font(bold=True, size=16)
        ws.merge_cells('A1:D1')
        
        ws['A3'] = 'URL:'
        ws['B3'] = data.get('url', 'N/A')
        
        results = data.get('results', {})
        ws['A5'] = 'Valid:'
        ws['B5'] = 'Yes' if results.get('valid') else 'No'
        ws['A6'] = 'URLs Count:'
        ws['B6'] = results.get('urls_count', 0)
        
        filepath = os.path.join(self.reports_dir, f"{task_id}.xlsx")
        wb.save(filepath)
        return filepath
    
    def generate_render_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Генерирует отчет аудита рендеринга"""
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
        """Генерирует отчет мобильной проверки"""
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
        """Генерирует отчет проверки ботов"""
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
xlsx_generator = XLSXGenerator()
