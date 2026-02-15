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
    """Р“РµРЅРµСЂР°С‚ор Word РѕС‚С‡РµС‚ов"""
    
    def __init__(self):
        self.reports_dir = settings.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def _add_heading(self, doc, text: str, level: int = 1):
        """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РіРѕР»овок"""
        heading = doc.add_heading(text, level=level)
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
        doc.save(filepath)
        return filepath
    
    def generate_robots_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ РєР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚ robots.txt."""
        doc = Document()

        title = doc.add_heading('РћС‚С‡РµС‚ по robots.txt', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        doc.add_paragraph(f"URL: {data.get('url', 'н/д')}")
        results = data.get('results', {})
        doc.add_paragraph(f"Р¤Р°Р№Р» robots.txt РЅР°Р№РґРµРЅ: {'Р”Р°' if results.get('robots_txt_found') else 'РќРµС‚'}")
        doc.add_paragraph(
            "РћРїРёСЃР°РЅРёРµ: robots.txt СѓРїСЂР°РІР»СЏРµС‚ РґРѕСЃС‚упом РїРѕРёСЃРєРѕРІС‹С… Рё СЃРµСЂРІРёСЃРЅС‹С… Р±РѕС‚ов Рє СЂР°Р·РґРµР»Р°Рј СЃР°Р№С‚Р°. "
            "Errors in this file may limit indexation of important pages."
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
        doc.save(filepath)
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
        doc.save(filepath)
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
        doc.save(filepath)
        return filepath
    def generate_mobile_report(self, task_id: str, data: Dict[str, Any]) -> str:
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ СЂР°СЃС€РёСЂРµРЅРЅС‹Р№ РєР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚ по РјРѕР±РёР»СЊРЅРѕР№ РІРµСЂСЃРёРё СЃР°Р№С‚Р°."""
        doc = Document()

        issue_guides = {
            "viewport_missing": {
                "name": "РћС‚СЃСѓС‚СЃС‚РІСѓРµС‚ meta viewport",
                "why": "Р‘РµР· viewport СЃС‚СЂР°РЅРёС†Р° РѕС‚РѕР±СЂР°Р¶Р°РµС‚ся РєР°Рє РґРµСЃРєС‚РѕРїРЅР°СЏ РІРµСЂСЃРёСЏ, С‡С‚Рѕ СѓС…СѓРґС€Р°РµС‚ UX Рё SEO-РїРѕРІРµРґРµРЅС‡РµСЃРєРёРµ С„Р°РєС‚РѕСЂС‹.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "Р”РѕР±Р°РІРёС‚СЊ РІ <head> С‚РµРі: <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">.",
                    "РџСЂРѕРІРµСЂРёС‚СЊ, С‡С‚Рѕ С‚РµРі РїСЂРёСЃСѓС‚СЃС‚РІСѓРµС‚ РЅР° РІСЃРµС… С€Р°Р±Р»РѕРЅР°С… СЃС‚СЂР°РЅРёС†.",
                ],
            },
            "viewport_invalid": {
                "name": "РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ РЅР°СЃС‚СЂРѕР№РєР° viewport",
                "why": "РќРµРІРµСЂРЅС‹Рµ РїР°СЂР°РјРµС‚СЂС‹ viewport РїСЂРёРІРѕРґСЏС‚ Рє РЅРµРїСЂР°РІРёР»ьному РјР°СЃС€С‚Р°Р±Сѓ Рё РїСЂРѕР±Р»РµРјР°Рј СЃ Р°РґР°РїС‚РёРІРЅРѕСЃС‚ью.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "РСЃРїСЂР°РІРёС‚СЊ content viewport РЅР° width=device-width, initial-scale=1.",
                    "РџСЂРѕРІРµСЂРёС‚СЊ РѕС‚СЃСѓС‚СЃС‚РІРёРµ РєРѕРЅС„Р»РёРєС‚СѓСЋС‰РёС… РјРµС‚Р°-С‚Рµгов viewport.",
                ],
            },
            "horizontal_overflow": {
                "name": "Р“РѕСЂРёР·РѕРЅС‚Р°Р»СЊРЅР°СЏ РїСЂРѕРєСЂСѓС‚РєР°",
                "why": "РџРѕР»СЊР·РѕРІР°С‚РµР»Рё РІС‹РЅСѓР¶РґРµРЅС‹ РїСЂРѕРєСЂСѓС‡РёРІР°С‚СЊ СЃС‚СЂР°РЅРёС†Сѓ по РіРѕСЂРёР·РѕРЅС‚Р°Р»Рё, С‡С‚Рѕ СѓС…СѓРґС€Р°РµС‚ РєРѕРЅРІРµСЂСЃРёСЋ Рё СѓРґРѕР±СЃС‚во.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "РќР°Р№С‚Рё Р±Р»РѕРєРё, РІС‹С…РѕРґСЏС‰РёРµ Р·Р° С€РёСЂРёРЅСѓ СЌРєСЂР°РЅР° (С€РёСЂРёРЅР° РґРѕРєСѓРјРµРЅС‚Р° Р±РѕР»СЊС€Рµ viewport).",
                    "РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Р°РґР°РїС‚РёРІРЅС‹Рµ РµРґРёРЅРёС†С‹ Рё РѕРіСЂР°РЅРёС‡РёС‚СЊ С€РёСЂРёРЅСѓ РјРµРґРёР°-СЌР»РµРјРµРЅС‚ов С‡РµСЂРµР· max-width: 100%.",
                ],
            },
            "small_touch_targets": {
                "name": "РњР°Р»РµРЅСЊРєРёРµ РєР»РёРєР°Р±РµР»СЊРЅС‹Рµ СЌР»РµРјРµРЅС‚С‹",
                "why": "Р­Р»РµРјРµРЅС‚С‹ РјРµРЅСЊС€Рµ 44x44px Р·Р°С‚СЂСѓРґРЅСЏСЋС‚ РЅР°РІРёРіР°С†РёСЋ СЃ СЃРµнсорного СЌРєСЂР°РЅР°.",
                "impact": "РЎСЂРµРґРЅРёР№",
                "fix": [
                    "РЈРІРµР»РёС‡РёС‚СЊ СЂР°Р·РјРµСЂС‹ РєРЅРѕРїРѕРє/СЃСЃС‹Р»ок РґРѕ РјРёРЅРёРјСѓРј 44x44px.",
                    "Р”РѕР±Р°РІРёС‚СЊ РѕС‚СЃС‚СѓРїС‹ РјРµР¶ду СЃРѕСЃРµРґРЅРёРјРё РёРЅС‚РµСЂР°РєС‚РёРІРЅС‹РјРё СЌР»РµРјРµРЅС‚Р°РјРё.",
                ],
            },
            "small_fonts": {
                "name": "РЎР»РёС€ком РјРµР»РєРёР№ С‚РµРєСЃС‚",
                "why": "РњРµР»РєРёР№ С‚РµРєСЃС‚ СЃРЅРёР¶Р°РµС‚ С‡РёС‚Р°РµРјРѕСЃС‚СЊ Рё СѓРІРµР»РёС‡РёРІР°РµС‚ РїРѕРєР°Р·Р°С‚РµР»СЊ РѕС‚РєР°Р·ов.",
                "impact": "РЎСЂРµРґРЅРёР№",
                "fix": [
                    "РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Р±Р°Р·РѕРІС‹Р№ СЂР°Р·РјРµСЂ С€СЂРёС„С‚Р° РЅРµ РјРµРЅРµРµ 16px РґР»СЏ РјРѕР±РёР»СЊРЅС‹С… СЌРєСЂР°нов.",
                    "РџСЂРѕРІРµСЂРёС‚СЊ РјР°СЃС€С‚Р°Р±РёСЂРѕРІР°РЅРёРµ С‚РµРєСЃС‚Р° РІ РєР»СЋС‡РµРІС‹С… Р±Р»РѕРєР°С… (РјРµню, РєР°СЂС‚РѕС‡РєРё, С„РѕСЂРјС‹).",
                ],
            },
            "large_images": {
                "name": "РР·РѕР±СЂР°Р¶РµРЅРёСЏ С€РёСЂРµ СЌРєСЂР°РЅР°",
                "why": "РЎР»РёС€ком С€РёСЂРѕРєРёРµ РёР·РѕР±СЂР°Р¶РµРЅРёСЏ Р»РѕРјР°СЋС‚ СЃРµС‚ку Рё РІС‹Р·С‹РІР°СЋС‚ РіРѕСЂРёР·РѕРЅС‚Р°Р»СЊРЅС‹Р№ СЃРєСЂРѕР»Р».",
                "impact": "РЎСЂРµРґРЅРёР№",
                "fix": [
                    "Р”РѕР±Р°РІРёС‚СЊ РґР»СЏ РёР·РѕР±СЂР°Р¶РµРЅРёР№ max-width: 100%; height: auto;.",
                    "РџСЂРѕРІРµСЂРёС‚СЊ Р°РґР°РїС‚РёРІРЅРѕСЃС‚СЊ СЃР»Р°Р№РґРµров, Р±Р°РЅРЅРµров Рё РІСЃС‚СЂР°РёРІР°РµРјС‹С… РјРµРґРёР°-Р±Р»оков.",
                ],
            },
            "console_errors": {
                "name": "РћС€РёР±РєРё JavaScript РІ РєРѕРЅСЃРѕР»Рё",
                "why": "JS-РѕС€РёР±РєРё РјРѕРіСѓС‚ Р»РѕРјР°С‚СЊ РјРµню, С„РѕСЂРјС‹, С„РёР»СЊС‚СЂС‹ Рё РґСЂСѓРіРёРµ СЌР»РµРјРµРЅС‚С‹ РёРЅС‚РµСЂС„РµР№СЃР°.",
                "impact": "РЎСЂРµРґРЅРёР№",
                "fix": [
                    "Р Р°Р·РѕР±СЂР°С‚СЊ РѕС€РёР±РєРё РєРѕРЅСЃРѕР»Рё РІ РїРѕСЂСЏРґРєРµ РєСЂРёС‚РёС‡РЅРѕСЃС‚Рё.",
                    "РСЃРїСЂР°РІРёС‚СЊ РЅРµРґРѕСЃС‚СѓРїРЅС‹Рµ СЂРµСЃСѓСЂСЃС‹ Рё РёСЃРєР»СЋС‡РµРЅРёСЏ РІ РєР»РёРµРЅС‚ском РєРѕРґРµ.",
                ],
            },
            "runtime_error": {
                "name": "РћС€РёР±РєР° РІС‹РїРѕР»РЅРµРЅРёСЏ РїСЂРѕРІРµСЂРєРё",
                "why": "РџСЂРѕРІРµСЂРєР° РєРѕРЅРєСЂРµС‚ного СѓСЃС‚СЂРѕР№СЃС‚РІР° РЅРµ Р·Р°РІРµСЂС€РёР»Р°сь, РґР°РЅРЅС‹Рµ РЅРµРїРѕР»РЅС‹Рµ.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "РџСЂРѕРІРµСЂРёС‚СЊ РґРѕСЃС‚СѓРїРЅРѕСЃС‚СЊ СЃР°Р№С‚Р°, СЂРµРґРёСЂРµРєС‚С‹ Рё Р±Р»РѕРєРёСЂРѕРІРєРё.",
                    "РџРѕРІС‚РѕСЂРёС‚СЊ С‚РµСЃС‚ РїРѕСЃР»Рµ РёСЃРїСЂР°РІР»РµРЅРёСЏ РёРЅС„СЂР°СЃС‚СЂСѓРєС‚СѓСЂРЅС‹С… РѕРіСЂР°РЅРёС‡РµРЅРёР№.",
                ],
            },
            "playwright_unavailable": {
                "name": "РЎСЂРµРґР° Р±СЂР°СѓР·Рµрного С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ РЅРµРґРѕСЃС‚СѓРїРЅР°",
                "why": "Р‘РµР· Р±СЂР°СѓР·Рµрного РґРІРёР¶РєР° РЅРµР»СЊР·СЏ РїРѕР»СѓС‡РёС‚СЊ СЂРµР°Р»СЊРЅС‹Рµ СЃРєСЂРёРЅС€РѕС‚С‹ Рё РёР·РјРµСЂРµРЅРёСЏ РјРѕР±РёР»СЊРЅРѕР№ РІРµСЂСЃС‚РєРё.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Р·Р°РІРёСЃРёРјРѕСЃС‚Рё Playwright Рё Р±СЂР°СѓР·РµСЂ Chromium РІ РѕРєСЂСѓР¶РµРЅРёРё СЃРµСЂРІРµСЂР°.",
                    "РџРµСЂРµР·Р°РїСѓСЃС‚РёС‚СЊ СЃРµСЂРІРёСЃ Рё РїРѕРІС‚РѕСЂРёС‚СЊ Р°РЅР°Р»РёР·.",
                ],
            },
            "mobile_engine_error": {
                "name": "РЎР±РѕР№ РґРІРёР¶РєР° РјРѕР±РёР»СЊРЅРѕР№ РїСЂРѕРІРµСЂРєРё",
                "why": "РРЅСЃС‚СЂСѓРјРµРЅС‚ РЅРµ смог РІС‹РїРѕР»РЅРёС‚СЊ РїРѕР»РЅРѕС†РµРЅРЅС‹Р№ РјРѕР±РёР»СЊРЅС‹Р№ Р°СѓРґРёС‚.",
                "impact": "Р’С‹СЃРѕРєРёР№",
                "fix": [
                    "РџСЂРѕРІРµСЂРёС‚СЊ Р»РѕРіРё СЃРµСЂРІРёСЃР° Рё РѕРєСЂСѓР¶РµРЅРёРµ РІС‹РїРѕР»РЅРµРЅРёСЏ.",
                    "РЈСЃС‚СЂР°РЅРёС‚СЊ РїСЂРёС‡РёРЅСѓ РѕС€РёР±РєРё Рё РїРѕРІС‚орно Р·Р°РїСѓСЃС‚РёС‚СЊ РїСЂРѕРІРµрку.",
                ],
            },
        }

        title = doc.add_heading('РљР»РёРµРЅС‚СЃРєРёР№ РѕС‚С‡РµС‚: РјРѕР±РёР»СЊРЅР°СЏ РІРµСЂСЃРёСЏ СЃР°Р№С‚Р°', 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        url = data.get("url", "н/д")
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        doc.add_paragraph(f"РЎР°Р№С‚: {url}")
        doc.add_paragraph(f"Report generated at: {generated_at}")
        doc.add_paragraph(
            "Р¦РµР»СЊ РѕС‚С‡РµС‚Р°: РѕС†РµРЅРёС‚СЊ СѓРґРѕР±СЃС‚во РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ СЃР°Р№С‚Р° РЅР° РїРѕРїСѓР»СЏСЂРЅС‹С… РјРѕР±РёР»СЊРЅС‹С… СѓСЃС‚СЂРѕР№СЃС‚РІР°С…, "
            "РІС‹СЏРІРёС‚СЊ С‚РµС…РЅРёС‡РµСЃРєРёРµ РѕС€РёР±РєРё Р°РґР°РїС‚РёРІРЅРѕСЃС‚Рё Рё РїСЂРµРґРѕСЃС‚Р°РІРёС‚СЊ РїРѕРЅСЏС‚РЅС‹Р№ РїР»Р°РЅ РёСЃРїСЂР°РІР»РµРЅРёР№ РґР»СЏ РєРѕРјР°РЅРґС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё."
        )

        results = data.get('results', {}) or {}
        summary = results.get('summary', {}) or {}
        devices = results.get('device_results', []) or []
        all_issues = results.get('issues', []) or []
        actionable_issues = [i for i in all_issues if i.get("severity") in ("critical", "warning")]
        info_issues = [i for i in all_issues if i.get("severity") == "info"]

        self._add_heading(doc, '1. РЎРІРѕРґРєР° по РїСЂРѕРІРµСЂРєРµ', level=1)
        summary_rows = [
            ["Р”РІРёР¶ок РїСЂРѕРІРµСЂРєРё", results.get("engine", "legacy")],
            ["Р РµР¶РёРј РїСЂРѕРІРµСЂРєРё", "Р‘С‹СЃС‚СЂС‹Р№" if results.get("mode") == "quick" else "РџРѕР»РЅС‹Р№"],
            ["РџСЂРѕРІРµСЂРµРЅРѕ СѓСЃС‚СЂРѕР№СЃС‚РІ", summary.get("total_devices", len(results.get("devices_tested", [])))],
            ["РЈСЃС‚СЂРѕР№СЃС‚РІ Р±РµР· РєСЂРёС‚РёС‡РЅС‹С… РїСЂРѕР±Р»РµРј", summary.get("mobile_friendly_devices", 0)],
            ["РЈСЃС‚СЂРѕР№СЃС‚РІ СЃ РїСЂРѕР±Р»РµРјР°РјРё", summary.get("non_friendly_devices", 0)],
            ["РЎСЂРµРґРЅРµРµ РІСЂРµмя Р·Р°РіСЂСѓР·РєРё, мс", summary.get("avg_load_time_ms", 0)],
            ["РљРѕР»РёС‡РµСЃС‚во РѕС€РёР±ок (critical + warning)", len(actionable_issues)],
            ["РРЅС„РѕСЂРјР°С†РёРѕРЅРЅС‹Рµ Р·Р°РјРµС‡Р°РЅРёСЏ", len(info_issues)],
            ["РРЅС‚РµРіСЂР°Р»СЊРЅР°СЏ РѕС†РµРЅРєР°", results.get("score", "н/д")],
            ["РС‚ог", "РЎР°Р№С‚ СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓРµС‚ РјРѕР±РёР»СЊРЅС‹Рј С‚СЂРµР±РѕРІР°РЅРёСЏРј" if results.get("mobile_friendly") else "РўСЂРµР±СѓСЋС‚ся РґРѕСЂР°Р±РѕС‚РєРё РјРѕР±РёР»СЊРЅРѕР№ РІРµСЂСЃРёРё"],
        ]
        self._add_table(doc, ["РџРѕРєР°Р·Р°С‚РµР»СЊ", "Р—РЅР°С‡РµРЅРёРµ"], summary_rows)

        self._add_heading(doc, '2. РўРµС…РЅРёС‡РµСЃРєРёРµ РїР°СЂР°РјРµС‚СЂС‹ Р°СѓРґРёС‚Р°', level=1)
        tech_rows = [
            ["HTTP СЃС‚Р°С‚ус", results.get("status_code", "н/д")],
            ["Р¤РёРЅР°Р»СЊРЅС‹Р№ URL", results.get("final_url", url)],
            ["Viewport РЅР°Р№РґРµРЅ", "Р”Р°" if results.get("viewport_found") else "РќРµС‚"],
            ["РЎРѕРґРµСЂР¶РёРјРѕРµ viewport", results.get("viewport_content") or "РќРµ РЅР°Р№РґРµРЅРѕ"],
        ]
        self._add_table(doc, ["РџР°СЂР°РјРµС‚СЂ", "Р—РЅР°С‡РµРЅРёРµ"], tech_rows)

        self._add_heading(doc, '3. Р РµР·СѓР»СЊС‚Р°С‚С‹ по СѓСЃС‚СЂРѕР№СЃС‚РІР°Рј', level=1)
        device_rows = []
        for d in devices:
            device_rows.append([
                d.get("device_name", ""),
                "РўРµР»РµС„РѕРЅ" if d.get("category") == "phone" else ("РџР»Р°РЅС€РµС‚" if d.get("category") == "tablet" else d.get("category", "")),
                f"{(d.get('viewport') or {}).get('width', '-') }x{(d.get('viewport') or {}).get('height', '-')}",
                d.get("status_code", "н/д"),
                d.get("load_time_ms", 0),
                d.get("issues_count", 0),
                "Р”Р°" if d.get("mobile_friendly") else "РќРµС‚",
            ])
        if device_rows:
            self._add_table(
                doc,
                ["РЈСЃС‚СЂРѕР№СЃС‚во", "РўРёРї", "Viewport", "HTTP", "Р—Р°РіСЂСѓР·РєР° (мс)", "РћС€РёР±ок", "ОК РґР»СЏ mobile"],
                device_rows,
            )
        else:
            doc.add_paragraph("Р”Р°РЅРЅС‹Рµ по СѓСЃС‚СЂРѕР№СЃС‚РІР°Рј РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚.")

        self._add_heading(doc, '4. Р’С‹СЏРІР»РµРЅРЅС‹Рµ РѕС€РёР±РєРё Рё РїР»Р°РЅ РёСЃРїСЂР°РІР»РµРЅРёСЏ', level=1)
        if not actionable_issues:
            doc.add_paragraph("РљСЂРёС‚РёС‡РµСЃРєРёРµ РѕС€РёР±РєРё Рё РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ РЅРµ РѕР±РЅР°СЂСѓР¶РµРЅС‹.")
        else:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for issue in actionable_issues:
                code = issue.get("code", "unknown")
                grouped.setdefault(code, []).append(issue)

            for idx, (code, items) in enumerate(grouped.items(), start=1):
                guide = issue_guides.get(code, {
                    "name": items[0].get("title", code),
                    "why": "РћС€РёР±РєР° РІР»РёСЏРµС‚ РЅР° РєР°С‡РµСЃС‚во РјРѕР±РёР»ьного РёРЅС‚РµСЂС„РµР№СЃР° Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёР№ РѕРїС‹С‚.",
                    "impact": "РЎСЂРµРґРЅРёР№",
                    "fix": ["РџСЂРѕРІРµСЂРёС‚СЊ РІРµСЂСЃС‚ку Рё РёСЃРїСЂР°РІРёС‚СЊ РїСЂРёС‡РёРЅСѓ РѕС€РёР±РєРё РІ С€Р°Р±Р»РѕРЅР°С…/СЃС‚РёР»СЏС…."],
                })

                self._add_heading(doc, f"4.{idx} {guide['name']}", level=2)
                devices_list = sorted({str(i.get("device", "РЅРµ СѓРєР°Р·Р°РЅРѕ")) for i in items})
                doc.add_paragraph(f"РЎРµСЂСЊРµР·РЅРѕСЃС‚СЊ: {items[0].get('severity', 'warning')}")
                doc.add_paragraph(f"Р—Р°С‚СЂРѕРЅСѓС‚Рѕ СѓСЃС‚СЂРѕР№СЃС‚РІ: {len(devices_list)}")
                doc.add_paragraph(f"РЈСЃС‚СЂРѕР№СЃС‚РІР°: {', '.join(devices_list)}")
                doc.add_paragraph(f"РџРѕС‡Рµму СЌС‚Рѕ РІР°Р¶РЅРѕ: {guide['why']}")
                doc.add_paragraph(f"Р‘РёР·РЅРµСЃ-РІР»РёСЏРЅРёРµ: {guide['impact']}")
                doc.add_paragraph("Р§С‚Рѕ СЃРґРµР»Р°С‚СЊ:")
                for step in guide["fix"]:
                    doc.add_paragraph(step, style='List Number')
                doc.add_paragraph("РўРµС…РЅРёС‡РµСЃРєРёРµ РґРµС‚Р°Р»Рё РёР· РїСЂРѕРІРµСЂРєРё:")
                for example in items[:5]:
                    detail = str(example.get("details", "") or "").strip()
                    if detail:
                        doc.add_paragraph(f"вЂў {detail}")

        self._add_heading(doc, '5. РРЅС„РѕСЂРјР°С†РёРѕРЅРЅС‹Рµ РЅР°Р±Р»СЋРґРµРЅРёСЏ', level=1)
        if info_issues:
            for issue in info_issues:
                doc.add_paragraph(f"{issue.get('title', 'РќР°Р±Р»СЋРґРµРЅРёРµ')}: {issue.get('details', '')}", style='List Bullet')
        else:
            doc.add_paragraph("Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РёРЅС„РѕСЂРјР°С†РёРѕРЅРЅС‹Рµ Р·Р°РјРµС‡Р°РЅРёСЏ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚.")

        self._add_heading(doc, '6. РЎРєСЂРёРЅС€РѕС‚С‹ РїСЂРѕРІРµСЂРµРЅРЅС‹С… СѓСЃС‚СЂРѕР№СЃС‚РІ', level=1)
        added = 0
        for d in devices:
            shot = d.get("screenshot_path")
            if not shot or not os.path.exists(shot):
                continue
            doc.add_paragraph(
                f"{d.get('device_name', 'РЈСЃС‚СЂРѕР№СЃС‚во')} | "
                f"Viewport {(d.get('viewport') or {}).get('width', '-') }x{(d.get('viewport') or {}).get('height', '-') } | "
                f"РћС€РёР±ок: {d.get('issues_count', 0)}"
            )
            try:
                doc.add_picture(shot, width=Inches(5.8))
                added += 1
            except Exception:
                doc.add_paragraph(f"РќРµ СѓРґР°Р»Рѕсь РІСЃС‚СЂРѕРёС‚СЊ СЃРєСЂРёРЅС€РѕС‚: {shot}")
        if added == 0:
            doc.add_paragraph("РЎРєСЂРёРЅС€РѕС‚С‹ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚.")

        self._add_heading(doc, '7. РС‚РѕРіРё', level=1)
        if actionable_issues:
            doc.add_paragraph(
                "РћР±РЅР°СЂСѓР¶РµРЅС‹ РѕС€РёР±РєРё РјРѕР±РёР»СЊРЅРѕР№ РІРµСЂСЃРёРё, РєРѕС‚РѕСЂС‹Рµ С‚СЂРµР±СѓСЋС‚ РёСЃРїСЂР°РІР»РµРЅРёСЏ РґР»СЏ РїРѕРІС‹С€РµРЅРёСЏ "
                "РєР°С‡РµСЃС‚РІР° РїРѕР»СЊР·РѕРІР°С‚РµР»ьского РѕРїС‹С‚Р° Рё СЃС‚Р°Р±РёР»СЊРЅРѕСЃС‚Рё SEO-РїРѕРєР°Р·Р°С‚РµР»РµР№ РЅР° РјРѕР±РёР»ьном С‚СЂР°С„РёРєРµ."
            )
            doc.add_paragraph(
                "Р РµРєРѕРјРµРЅРґСѓРµС‚ся РІС‹РїРѕР»РЅРёС‚СЊ РёСЃРїСЂР°РІР»РµРЅРёСЏ по РїСЂРёРѕСЂРёС‚РµС‚Сѓ (critical -> warning), "
                "РїРѕСЃР»Рµ С‡Рµго РїСЂРѕРІРµСЃС‚Рё РїРѕРІС‚орную РїСЂРѕРІРµрку Рё Р·Р°С„РёРєСЃРёСЂРѕРІР°С‚СЊ СѓР»СѓС‡С€РµРЅРёРµ РјРµС‚СЂРёРє."
            )
        else:
            doc.add_paragraph(
                "РљСЂРёС‚РёС‡РЅС‹С… РїСЂРѕР±Р»РµРј РЅРµ РѕР±РЅР°СЂСѓР¶РµРЅРѕ. РњРѕР±РёР»СЊРЅР°СЏ РІРµСЂСЃРёСЏ СЃР°Р№С‚Р° РІ С‚РµРєСѓС‰РµР№ РєРѕРЅС„РёРіСѓСЂР°С†РёРё "
                "СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓРµС‚ Р±Р°Р·РѕРІС‹Рј С‚СЂРµР±РѕРІР°РЅРёСЏРј СѓРґРѕР±СЃС‚РІР° Рё С‚РµС…РЅРёС‡РµСЃРєРѕР№ РєРѕСЂСЂРµРєС‚РЅРѕСЃС‚Рё."
            )

        doc.add_paragraph()
        footer = doc.add_paragraph(f"РћС‚С‡РµС‚ СЃС„РѕСЂРјРёСЂРѕРІР°РЅ SEO РРЅСЃС‚СЂСѓРјРµРЅС‚С‹: {generated_at}")
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.runs[0].font.size = Pt(8)
        footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)

        filepath = os.path.join(self.reports_dir, f"{task_id}.docx")
        doc.save(filepath)
        return filepath

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
        doc.save(filepath)
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
        doc.save(filepath)
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
            'site_audit_pro': self.generate_site_audit_pro_report
        }
        
        generator = generators.get(task_type, self.generate_site_analyze_report)
        return generator(task_id, data)


# Singleton
docx_generator = DOCXGenerator()

