__all__ = ["EventConfig"]


class EventConfig:
    def __init__(self, *, event_id: int, max_participants: int):

        assert type(event_id) == int, f"""The parameter "event_id" must be an integral value, found {type(event_id)}."""
        assert event_id > 0, f"""The parameter "event_id" must be greater than zero, found "{event_id}"."""

        assert type(max_participants) == int, f"""The parameter "max_participants" must be an integral value, found {type(max_participants)}."""
        assert max_participants > 0, f"""The parameter "max_participants" must be greater than zero, found "{max_participants}"."""

        self.__event_id = event_id
        self.__max_participants = max_participants

    @property
    def event_id(self):
        """
        The unique ID for an event; typically draw from the LMS as the Session ID or commonly referred to as the Class ID
        :return:
        """
        return self.__event_id

    @property
    def max_participants(self) -> int:
        """
        The maximum number of participants for an event; not to be confused with the number of participants per workspace
        :return: The total number of participant_count
        """
        return self.__max_participants
