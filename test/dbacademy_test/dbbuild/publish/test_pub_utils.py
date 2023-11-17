__all__ = ["PubUtilsTests"]

import unittest
from dbacademy.dbbuild.publish import pub_utils


class PubUtilsTests(unittest.TestCase):

    def test_parse_links(self):
        links = pub_utils.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(0, len(links))

        links = pub_utils.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(1, len(links))

        links = pub_utils.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a>
            # MAGIC # <a href="https://google.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(2, len(links))

        links = pub_utils.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a><a href="https://google.com" target="_blank">some link</a><a href="https://databricks.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(3, len(links))


if __name__ == '__main__':
    unittest.main()
