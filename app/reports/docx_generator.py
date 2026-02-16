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
        self._normalize_document_text(doc)
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
        self._normalize_document_text(doc)
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
        self._normalize_document_text(doc)
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
        self._normalize_document_text(doc)
        doc.save(filepath)
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
        actionable_issues = [i for i in all_issues if i.get("severity") in ("critical", "warning")]
        info_issues = [i for i in all_issues if i.get("severity") == "info"]

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
                doc.add_paragraph(
                    f"{issue.get('title', '\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435')}: {issue.get('details', '')}",
                    style="List Bullet",
                )
        else:
            doc.add_paragraph("Дополнительные информационные замечания отсутствуют.")

        self._add_heading(doc, "6. Скриншоты проверенных устройств", level=1)
        added = 0
        for d in devices:
            shot = d.get("screenshot_path")
            if not shot or not os.path.exists(shot):
                continue
            doc.add_paragraph(
                f"{d.get('device_name', '\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e')} | "
                f"Viewport {(d.get('viewport') or {}).get('width', '-')}x{(d.get('viewport') or {}).get('height', '-')} | "
                f"\u041e\u0448\u0438\u0431\u043e\u043a: {d.get('issues_count', 0)}"
            )
            try:
                doc.add_picture(shot, width=Inches(5.8))
                added += 1
            except Exception:
                doc.add_paragraph(f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0432\u0441\u0442\u0440\u043e\u0438\u0442\u044c \u0441\u043a\u0440\u0438\u043d\u0448\u043e\u0442: {shot}")
        if added == 0:
            doc.add_paragraph("Скриншоты отсутствуют.")

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
        self._normalize_document_text(doc)
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
        self._normalize_document_text(doc)
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
        self._normalize_document_text(doc)
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
