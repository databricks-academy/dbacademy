import unittest


class TestCoursesClient(unittest.TestCase):

    def test_get_course(self):
        from dbacademy.clients.docebo import DoceboRestClient
        client = DoceboRestClient.from_environ()

        course = client.courses.get_course_by_id("134")
        self.assertIsNotNone(course)
        self.assertEquals(134, course.get("id"))

    def test_get_course_not_found(self):
        from dbacademy.clients.docebo import DoceboRestClient
        client = DoceboRestClient.from_environ()

        course = client.courses.get_course_by_id("1689")
        self.assertIsNone(course)

    def test_get_sessions_by_course(self):
        from dbacademy.clients.docebo import DoceboRestClient
        client = DoceboRestClient.from_environ()

        sessions = client.sessions.get_sessions_by_course_id("134")
        self.assertIsNotNone(sessions)
        self.assertTrue(len(sessions) > 0)

    def test_get_sessions_by_course_no_sessions(self):
        from dbacademy.clients.docebo import DoceboRestClient
        client = DoceboRestClient.from_environ()

        course_id = 874
        sessions = client.sessions.get_sessions_by_course_id(course_id)
        self.assertIsNotNone(sessions)
        self.assertEqual(0, len(sessions))

    def test_get_events_by_session(self):
        from dbacademy.clients.docebo import DoceboRestClient
        client = DoceboRestClient.from_environ()

        sessions = client.sessions.get_sessions_by_course_id("134")
        self.assertTrue(len(sessions) > 0)
        session_id = sessions[0].get("id")

        events = client.events.get_events_by_session_id(session_id)
        self.assertIsNotNone(events)
        self.assertTrue(len(events) > 0)


if __name__ == '__main__':
    unittest.main()
