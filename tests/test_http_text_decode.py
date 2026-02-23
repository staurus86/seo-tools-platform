import unittest

from app.tools.http_text import decode_response_text


class _Resp:
    def __init__(self, content: bytes, encoding: str = "", apparent_encoding: str = "", headers=None, text: str = ""):
        self.content = content
        self.encoding = encoding
        self.apparent_encoding = apparent_encoding
        self.headers = headers or {}
        self.text = text


class HttpTextDecodeTests(unittest.TestCase):
    def test_prefers_utf8_when_declared_encoding_is_wrong(self):
        source = "Скачать DOCX и JSON"
        resp = _Resp(
            content=source.encode("utf-8"),
            encoding="cp1251",
            apparent_encoding="cp1251",
            headers={"Content-Type": "text/html; charset=cp1251"},
        )
        self.assertEqual(decode_response_text(resp), source)

    def test_falls_back_to_cp1251_for_legacy_pages(self):
        source = "Привет мир"
        resp = _Resp(content=source.encode("cp1251"), encoding="", apparent_encoding="")
        self.assertEqual(decode_response_text(resp), source)

    def test_uses_charset_from_header(self):
        source = "Тест заголовка"
        resp = _Resp(
            content=source.encode("cp1251"),
            headers={"content-type": "text/html; charset=windows-1251"},
        )
        self.assertEqual(decode_response_text(resp), source)


if __name__ == "__main__":
    unittest.main()
