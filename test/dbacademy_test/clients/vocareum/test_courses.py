import unittest
from dbacademy.clients import vocareum


class TestVocareumRestClient(unittest.TestCase):

    def test_list_courses(self):
        client = vocareum.from_environment()
        self.assertIsNotNone(client)

        courses = client.courses.list()
        self.assertIsNotNone(courses)

        course_ids = [c.get("id") for c in courses]

        expected_min_count = 20

        actual = len(courses)
        self.assertTrue(actual >= expected_min_count, f"Found {actual}")

        actual = len(set(course_ids))
        self.assertTrue(actual >= expected_min_count, f"Found {actual}")

    def test_get_course_by_id(self):
        client = vocareum.from_environment()
        self.assertIsNotNone(client)

        course = client.courses.id("85487").get()
        self.assertIsNotNone(course)
        self.assertEqual("85487", course.get("id"))
        self.assertEqual("[PRODUCTION EDX]  Databricks LLM Course-1 - Version-1", course.get("name"))
        # print(json.dumps(course, indent=4))


if __name__ == '__main__':
    unittest.main()
