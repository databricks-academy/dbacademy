import unittest
from dbacademy.workspaces_3_0.event_config_class import EventConfig


class TestEventConfig(unittest.TestCase):

    def test_create_event_config(self):
        event_config = EventConfig(event_id=1234, max_participants=250)
        self.assertEqual(1234, event_config.event_id)
        self.assertEqual(250, event_config.max_participants)

    def test_create_event_config_event_id(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "event_id" must be an integral value, found <class 'str'>.""", lambda: EventConfig(event_id="1234", max_participants=250))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "event_id" must be an integral value, found <class 'NoneType'>.""", lambda: EventConfig(event_id=None, max_participants=250))
        test_assertion_error(self, """The parameter "event_id" must be greater than zero, found "0".""", lambda: EventConfig(event_id=0, max_participants=250))

    def test_create_event_config_participant_count(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "max_participants" must be an integral value, found <class 'str'>.""", lambda: EventConfig(event_id=1234, max_participants="250"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "max_participants" must be an integral value, found <class 'NoneType'>.""", lambda: EventConfig(event_id=1234, max_participants=None))
        test_assertion_error(self, """The parameter "max_participants" must be greater than zero, found "0".""", lambda: EventConfig(event_id=1234, max_participants=0))


if __name__ == '__main__':
    unittest.main()
