import json
import pathlib
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import edisclosure_collect as ec

FIXTURES = pathlib.Path(__file__).resolve().parent / "testdata" / "edisclosure"


class ParseDocumentsTableTest(unittest.TestCase):
    def test_real_page(self):
        html = (FIXTURES / "PLZL_IFRS.html").read_text(encoding="utf-8")
        rows = ec.parse_documents_table(html, "IFRS", "PLZL")
        self.assertGreater(len(rows), 0)
        row = rows[0]
        self.assertEqual(row["ticker"], "PLZL")
        self.assertEqual(row["section"], "IFRS")
        self.assertTrue(row["fileid"].isdigit())
        self.assertEqual(
            row["url"],
            f"https://e-disclosure.ru/portal/FileLoad.ashx?Fileid={row['fileid']}",
        )

    def test_contract_fields_and_first_row(self):
        html = (FIXTURES / "PLZL_IFRS.html").read_text(encoding="utf-8")
        rows = ec.parse_documents_table(html, "IFRS", "PLZL")
        self.assertEqual(set(rows[0]), set(ec.DOCUMENT_FIELDS))
        row = rows[0]
        self.assertEqual(row["company_id"], "7832")
        self.assertEqual(row["table_num"], "1")
        self.assertEqual(row["period"], "2026, 6 месяцев")
        self.assertEqual(row["basis_date"], "28.08.2026")
        self.assertEqual(row["publication_date"], "31.08.2026")
        self.assertIn("645.07", row["size"])
        self.assertTrue(row["doc_type"])
        for document in rows:
            ec.validate_document(document, ec.COMPANIES)

    def test_missing_table_rejected(self):
        with self.assertRaisesRegex(ValueError, "files-table"):
            ec.parse_documents_table("<html><body></body></html>", "IFRS", "PLZL")


class FixtureSecurityTest(unittest.TestCase):
    def test_anti_forgery_tokens_redacted(self):
        html = (FIXTURES / "PLZL_IFRS.html").read_text(encoding="utf-8")
        self.assertEqual(html.count("REDACTED"), 2)
        self.assertEqual(
            len(re.findall(r'name="token"\s+value="REDACTED"', html)), 1
        )
        self.assertEqual(
            len(re.findall(
                r'name="__RequestVerificationToken"[^>]*value="REDACTED"', html
            )),
            1,
        )
        self.assertNotRegex(
            html, r'name="token"\s+value="(?!REDACTED")[^"]*"'
        )
        self.assertNotRegex(
            html,
            r'name="__RequestVerificationToken"[^>]*value="(?!REDACTED")[^"]*"',
        )


class ValidationTest(unittest.TestCase):
    def _row(self, **overrides):
        row = {
            "ticker": "PLZL",
            "company_id": "7832",
            "section": "IFRS",
            "doc_type": "x",
            "period": "",
            "basis_date": "",
            "publication_date": "",
            "fileid": "1",
            "url": "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1",
            "size": "",
            "table_num": "",
        }
        row.update(overrides)
        return row

    def test_valid_row_accepted(self):
        ec.validate_document(self._row(), ec.COMPANIES)

    def test_wrong_host_rejected(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1", "doc_type": "x",
               "period": "", "basis_date": "", "publication_date": "",
               "url": "https://example.com/portal/FileLoad.ashx?Fileid=1"}
        with self.assertRaisesRegex(ValueError, "e-disclosure"):
            ec.validate_document(row, ec.COMPANIES)

    def test_fileid_mismatch_rejected(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1", "doc_type": "x",
               "period": "", "basis_date": "", "publication_date": "",
               "url": "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=2"}
        with self.assertRaisesRegex(ValueError, "Fileid"):
            ec.validate_document(row, ec.COMPANIES)

    def test_non_numeric_fileid_rejected(self):
        url = "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=x"
        with self.assertRaisesRegex(ValueError, "только цифры"):
            ec.validate_document(self._row(fileid="x", url=url), ec.COMPANIES)

    def test_company_id_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError, "company_id"):
            ec.validate_document(self._row(company_id="1"), ec.COMPANIES)

    def test_userinfo_rejected(self):
        url = "https://user:pass@e-disclosure.ru/portal/FileLoad.ashx?Fileid=1"
        with self.assertRaisesRegex(ValueError, "userinfo"):
            ec.validate_document(self._row(url=url), ec.COMPANIES)

    def test_port_rejected(self):
        url = "https://e-disclosure.ru:8443/portal/FileLoad.ashx?Fileid=1"
        with self.assertRaisesRegex(ValueError, "порт"):
            ec.validate_document(self._row(url=url), ec.COMPANIES)

    def test_invalid_port_rejected(self):
        url = "https://e-disclosure.ru:port/portal/FileLoad.ashx?Fileid=1"
        with self.assertRaisesRegex(ValueError, "порт"):
            ec.validate_document(self._row(url=url), ec.COMPANIES)

    def test_dedupe_rejects_conflict(self):
        a = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
             "doc_type": "A", "period": "2025", "basis_date": "",
             "publication_date": "", "url":
             "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        b = dict(a, doc_type="B")
        with self.assertRaisesRegex(ValueError, "1943701"):
            ec.dedupe_documents([a, b])

    def test_dedupe_collapses_identical(self):
        a = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
             "doc_type": "A", "period": "2025", "basis_date": "",
             "publication_date": "", "url":
             "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        rows = ec.dedupe_documents([a, dict(a)])
        self.assertEqual(rows, [a])

    def test_manifest_count(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
               "doc_type": "A", "period": "2025", "basis_date": "",
               "publication_date": "", "url":
               "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        m = ec.build_manifest(
            [{"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
              "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"}],
            list(ec.SECTIONS), [row], "2026-10-18T12:00:00+03:00")
        self.assertEqual(m["document_count"], 1)
        self.assertEqual(m["documents_file"], "documents.jsonl")
        self.assertEqual(m["schema_version"], 1)
        self.assertEqual(m["collector"],
                         {"name": "edisclosure_collect.py", "version": "1"})
        self.assertEqual(m["generated_at"], "2026-10-18T12:00:00+03:00")
        self.assertEqual(m["sections"], list(ec.SECTIONS))
        self.assertEqual(m["companies"][0]["company_id"], "7832")

    def test_empty_sections_by_company(self):
        companies = [{"ticker": "PLZL"}, {"ticker": "SBER"}]
        docs = [{"ticker": "PLZL", "section": "IFRS"}]
        got = ec.empty_sections_by_company(companies, list(ec.SECTIONS), docs)
        self.assertEqual(got, [{"ticker": "PLZL", "sections": ["RAS", "AFFILIATES", "AGM"]},
                               {"ticker": "SBER", "sections": list(ec.SECTIONS)}])

    def test_empty_sections_omit_complete_company(self):
        companies = [{"ticker": "PLZL"}, {"ticker": "SBER"}]
        docs = [{"ticker": "PLZL", "section": section} for section in ec.SECTIONS]
        docs.append({"ticker": "SBER", "section": "IFRS"})
        got = ec.empty_sections_by_company(companies, list(ec.SECTIONS), docs)
        self.assertEqual(
            got,
            [{"ticker": "SBER", "sections": ["RAS", "AFFILIATES", "AGM"]}],
        )

    def test_company_without_documents_rejected(self):
        with self.assertRaisesRegex(ValueError, "SBER"):
            ec.ensure_company_has_documents({"PLZL": 211, "SBER": 0, "CHMF": 56})

    def test_company_with_documents_accepted(self):
        ec.ensure_company_has_documents({"PLZL": 211, "SBER": 23, "CHMF": 56})

    def test_empty_section_page_classified(self):
        html = (
            '<table class="company-table"><tr><td>Полное наименование компании'
            "</td><td>ПАО «Полюс»</td></tr></table>"
        )
        self.assertEqual(ec.classify_section_page(html), "empty")

    def test_documents_section_page_classified(self):
        self.assertEqual(
            ec.classify_section_page('<table class="files-table"></table>'),
            "documents",
        )

    def test_unknown_section_page_rejected(self):
        with self.assertRaisesRegex(ValueError, "неизвестная страница"):
            ec.classify_section_page("<html><body>challenge</body></html>")

    def test_write_output(self):
        row = {"ticker": "PLZL", "section": "IFRS", "fileid": "1943701",
               "doc_type": "A", "period": "2025", "basis_date": "",
               "publication_date": "", "url":
               "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
        manifest = ec.build_manifest(
            [{"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
              "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"}],
            list(ec.SECTIONS), [row], "2026-10-18T12:00:00+03:00")
        manifest["empty_sections"] = ec.empty_sections_by_company(
            [{"ticker": "PLZL"}], list(ec.SECTIONS), [row]
        )
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = pathlib.Path(tmp) / "nested" / "edisclosure"
            ec.write_output(out_dir, manifest, [row])
            written = json.loads((out_dir / "manifest.json").read_text(
                encoding="utf-8"))
            self.assertEqual(written["document_count"], 1)
            self.assertEqual(
                written["empty_sections"],
                [{"ticker": "PLZL", "sections": ["RAS", "AFFILIATES", "AGM"]}],
            )
            lines = (out_dir / "documents.jsonl").read_text(
                encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["fileid"], "1943701")
            self.assertFalse((out_dir / "documents.jsonl.tmp").exists())
            self.assertFalse((out_dir / "manifest.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
