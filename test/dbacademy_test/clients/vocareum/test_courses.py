import unittest


class TestVocareumRestClient(unittest.TestCase):

    def test_list_courses(self):
        from dbacademy.clients.vocareum import VocareumRestClient

        client = VocareumRestClient.from_environ()
        self.assertIsNotNone(client)

        courses = client.courses.list()
        self.assertIsNotNone(courses)

        course_ids = [c.get("id") for c in courses]

        expected_count = 23
        self.assertEqual(expected_count, len(courses))
        self.assertEqual(expected_count, len(set(course_ids)))

    def test_get_course_by_id(self):
        from dbacademy.clients.vocareum import VocareumRestClient

        client = VocareumRestClient.from_environ()
        self.assertIsNotNone(client)

        course = client.courses.id("85487").get()
        self.assertIsNotNone(course)
        self.assertEqual("85487", course.get("id"))
        self.assertEqual("[PRODUCTION EDX]  Databricks LLM Course-1", course.get("name"))
        # print(json.dumps(course, indent=4))


if __name__ == '__main__':
    unittest.main()
