import unittest


class TestVocareumRestClient(unittest.TestCase):

    def test_list_assignments(self):
        from dbacademy.clients.vocareum import VocareumRestClient

        client = VocareumRestClient.from_environ()
        self.assertIsNotNone(client)

        assignments = client.courses.id("87112").assignments.list()
        self.assertIsNotNone(assignments)

        assignment_ids = [a.get("id") for a in assignments]

        expected_count = 4
        self.assertEqual(expected_count, len(assignment_ids))
        self.assertEqual(expected_count, len(set(assignment_ids)))

    def test_get_assignment(self):
        from dbacademy.clients.vocareum import VocareumRestClient

        client = VocareumRestClient.from_environ()
        self.assertIsNotNone(client)

        assignment = client.courses.id("87112").assignments.id("1858312").get()
        self.assertIsNotNone(assignment)
        self.assertEqual("1858312", assignment.get("id"))
        self.assertEqual("87112", assignment.get("courseid"))
        self.assertEqual("Getting Started with Databricks Notebooks", assignment.get("name"))


if __name__ == '__main__':
    unittest.main()
