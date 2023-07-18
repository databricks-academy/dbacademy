import unittest


class MyTestCase(unittest.TestCase):

    def test_get_latest_commit_id(self):
        from dbacademy.clients import github

        commit_id = github.default_client().repo("dbacademy").commits.get_latest_commit_id(branch_name="main")

        self.assertIsNotNone(commit_id)

    def test_list_all_tags(self):
        from dbacademy.clients import github

        all_tags = github.default_client().repo("dbacademy").list_all_tags()
        self.assertTrue(len(all_tags) >= 30, f"Expected at least 30 entries, found {len(all_tags)}")

        # This call is only returning the last 30 making the test flaky to the extent of validating any specific entries.
        # self.assertEqual("1.0.20", all_tags[0], f"Expected version #1 to be 1.0.12, found {all_tags[0]}")
        # self.assertEqual("2.0.1", all_tags[29], f"Expected version #1 to be 1.0.41, found {all_tags[29]}")


if __name__ == '__main__':
    unittest.main()
