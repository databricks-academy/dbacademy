from abc import ABCMeta, abstractmethod
from typing import List

from dbacademy.rest.common import *

__all__ = ["CRUD"]


class CRUD(ApiContainer, metaclass=ABCMeta):
    """
    Abstract base class for CRUD API operations.

    Subclasses should override these methods to define the core API calls:
    * _list()
    * _get(item_id)
    * _create(item)
    * _update(item)
    * _delete(item_id)
    * _wrap(item)
    """

    def __init__(self,
                 client: ApiClient,
                 path: str,
                 noun: str,
                 *,
                 singular: str = None,
                 plural: str = None,
                 id_key: str = None,
                 name_key: str = None):
        super().__init__()
        self.client = client
        self.path = path
        self.noun = noun
        self.singular = singular or self.noun
        self.plural = plural or self.singular + "s"
        self.id_key = id_key or noun + "_id"
        self.name_key = name_key or noun + "_name"
        # Update doc strings, replacing placeholders with actual values.
        cls = type(self)
        methods = [attr for attr in dir(cls) if not attr.startswith("__") and callable(getattr(cls, attr))]
        for name in methods:
            m = getattr(cls, name)
            if isinstance(m.__doc__, str):
                m.__doc__ = m.__doc__.format(**self.__dict__)

    @abstractmethod
    def _list(self, *, expected: HttpErrorCodes = None) -> List[Item]:
        """Perform API call"""
        pass

    @abstractmethod
    def _get(self, item_id: ItemId, *, expected: HttpErrorCodes = None) -> Item:
        """Perform API call"""
        pass

    @abstractmethod
    def _create(self, item: Item, *, expected: HttpErrorCodes = None) -> ItemOrId:
        """Perform API call"""
        pass

    @abstractmethod
    def _update(self, item: Item, *, expected: HttpErrorCodes = None) -> ItemOrId:
        """Perform API call"""
        pass

    @abstractmethod
    def _delete(self, item_id, *, expected: HttpErrorCodes = None):
        """Perform API call"""
        pass

    def _wrap(self, item: Item):
        """
         Return a (possibly wrapped) copy of the item.
         """
        return dict(item)

    def _refresh(self, item: Item, fetch: bool = False, item_id: ItemId = None):
        """
        Return a (possibly wrapped) copy of the item.

        If `fetch` is `True`, the item is refreshed from the server.
        If `fetch` is `False`, the provided item is copied.  If item_id is provided, it is set in the copy.
        """
        if item is None:
            return item
        if fetch:
            if item_id:
                return self.get_by_id(item_id)
            else:
                return self.get_by_example(item)
        else:
            result = self._wrap(item)
            if item_id:
                result[self.id_key] = item_id
            return result

    def _item_id(self, item: Item):
        """
        If `item` has `{id_key}` set, that value is returned.
        Otherwise, the `{id_key}` is determined by querying for a matching `{name_key}`.

        Returns:
            The `{id_key}` of the item.

        Args:
            item: A dict containing either `{id_key}` or `{name_key}`

        Raises:
            DatabricksApiException: If not found.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
       """
        if item_id := item.get(self.id_key):
            return item_id
        if item_name := item.get(self.name_key):
            return self.get_by_name(item_name, if_not_exists="error")[self.id_key]
        raise ValueError(f"spec must have either {self.id_key!r} or {self.name_key!r}")

    def list(self) -> List[Item]:
        """Returns a list of all {plural}."""
        return [self._refresh(item) for item in self._list()]

    def list_names(self) -> List[str]:
        """Returns a list the names of all {plural}."""
        return [item[self.name_key] for item in self._list()]

    def get_by_id(self, item_id: ItemId, if_not_exists: IfNotExists = "error") -> Item:
        """
        Fetch the {singular} with `{id_key}`=`item_id`

        Returns:
            The {singular} with `{id_key}`=`item_id`

        Args:
            item_id: The `{id_key}` of the {singular}.
            if_not_exists: "error" or "ignore".

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
            ValueError: If `if_not_exists` is not 'ignore' or 'error'.
        """
        if if_not_exists == "ignore":
            expected = 404
        elif if_not_exists == "error":
            expected = None
        else:
            raise ValueError("if_not_exists must be 'ignore' or 'error'")
        item = self._get(item_id, expected=expected)
        return self._refresh(item)

    def get_by_name(self, item_name: str, if_not_exists: IfNotExists = "error") -> Item:
        """
        Fetch the first {singular} found with `{name_key}`=`item_name`

        Returns:
            The first {singular} found with `{name_key}`=`item_name`

        Args:
            item_name: The `{name_key}` of the {singular}.
            if_not_exists: "error" or "ignore".

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
        """
        result = next((item for item in self._list() if item[self.name_key] == item_name), None)
        if result is None and if_not_exists == "error":
            raise DatabricksApiException(f"{self.singular} with name '{item_name}' not found", 404)
        return self._refresh(result)

    def get_by_example(self, item: Item, if_not_exists: IfNotExists = "error") -> Item:
        """
        Fetch a fresh copy of {singular} from the server.

        Returns:
            The first {singular} found matching `{id_key}` if present, else matching the `{name_key}`.

        Args:
            item: A dict containing either `{id_key}` or `{name_key}`
            if_not_exists: "error" or "ignore".

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
        """
        if item_id := item.get(self.id_key):
            return self.get_by_id(item_id, if_not_exists=if_not_exists)
        elif item_name := item.get(self.name_key):
            return self.get_by_name(item_name, if_not_exists=if_not_exists)
        else:
            raise ValueError(f"spec must have either {self.id_key!r} or {self.name_key!r}")

    def create(self, unimplemented):
        """Not Implemented."""
        raise DatabricksApiException("Not Implemented", 501)

    def create_by_example(self, item: Item, *, fetch: bool = False, if_exists: IfExists = "create") -> Item:
        """
        Create a new {singular}.

        Args:
            item: A dict specification for the new {singular}.
            fetch: If true, a fresh copy of the item be retrieved from the server else a clone of `item` is returned.
            if_exists: 'create', 'ignore', 'update', or 'error'.

        If already exists and if_exists is:
            "ignore": return the existing {singular}.
            "update": update (patch) the existing {singular} with values from `item`.
            "error": raises :class:`DatabricksApiException`.
            "create": attempt to create a new {singular} anyway, possibly resulting in duplicately named items.
            "overwrite": replace the existing {singular} with `item`.

        Raises:
            DatabricksApiException: If already exists and `if_exists=="error"`.
            DatabricksApiException: If the server rejects the request as invalid.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
            ValueError: If `if_exists` is not one of 'ignore', 'update', 'error', 'create', or 'overwrite'.
        """
        if if_exists == "create":
            if self.name_key not in item:
                raise ValueError(f"{self.singular} cannot be created without the {self.name_key}.")
            existing = None
        else:
            existing = self.get_by_example(item, if_not_exists="ignore") if if_exists != "create" else None
        if existing is None:
            result = self._create(item)
            if isinstance(str, dict):
                return self._refresh(result, fetch)
            else:
                return self._refresh(item, fetch, result)
        if if_exists == "ignore":
            return existing
        if if_exists == "update":
            return self.update(item, fetch=fetch, if_not_exists="error")
        if if_exists == "error":
            raise DatabricksApiException(f"{self.singular}({self.id_key}={existing[self.id_key]!r}, "
                                         f"{self.name_key}={existing[self.name_key]!r}) "
                                         "already exists.", 409)
        raise ValueError("if_exists must be one of 'ignore', 'update', 'error', 'create', or 'overwrite'")

    def create_or_update(self, item: Item, *, fetch: bool = False) -> Item:
        """
        Create a new {singular}, updating it instead of it already exists.  This has the same effect as calling
        `create(item, fetch=fetch, if_exists="update")`.

        Args:
            item: A dict specification for the new/updated {singular}.
            fetch: If true, a fresh copy of the item be retrieved from the server else a clone of `item` is returned.

        Raises:
            DatabricksApiException: If the server rejects the request as invalid.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
        """
        return self.create_by_example(item, fetch=fetch, if_exists="update")

    def update(self, item: Item, *, fetch: bool = False, if_not_exists: IfNotExists = "error") -> Item:
        """
        Update the {singular}.

        Args:
            item: A dict specification for the new {singular}.
            fetch: If true, a fresh copy of the item be retrieved from the server else a clone of `item` is returned.
            if_not_exists: "error" or "ignore"

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
            DatabricksApiException: If the server rejects the request as invalid.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
            ValueError: If `if_not_exists` is not 'ignore' or 'error'.
        """
        if if_not_exists == "ignore":
            expected = 404
        elif if_not_exists == "error":
            expected = None
        else:
            raise ValueError("if_not_exists must be 'ignore' or 'error'")
        result = self._update(item, expected=expected)
        if isinstance(str, dict):
            return self._refresh(result, fetch)
        else:
            return self._refresh(item, fetch, result)

    def delete_by_id(self, item_id, *, if_not_exists: IfNotExists = "error") -> bool:
        """
        Delete the {singular} with the given `{id_key}`.

        Args:
            item_id: The `{id_key}` of the {singular}.
            if_not_exists: "error" or "ignore"

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
            ValueError: If `if_not_exists` is not 'ignore' or 'error'.
        """
        if if_not_exists == "ignore":
            expected = 404
        elif if_not_exists == "error":
            expected = None
        else:
            raise ValueError("if_not_exists must be 'ignore' or 'error'")
        result = self._delete(item_id, expected=expected)
        return result is not None

    def delete_by_name(self, item_name, if_not_exists: IfNotExists = "error"):
        """
        Delete the first {singular} with the given name.

        Args:
            item_name: The `{name_key}` of the {singular}.
            if_not_exists: "error" or "ignore".

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
        """
        item = self.get_by_name(item_name, if_not_exists=if_not_exists)
        if item:
            return self.delete_by_id(item[self.id_key])

    def delete_by_example(self, item, if_not_exists: IfNotExists = "error"):
        """
        Delete the provided {singular}.

        Args:
            item: A dict containing either `{id_key}` or `{name_key}`
            if_not_exists: "error" or "ignore".

        If not found and if_not_exists is:
            "ignore": return `None`.
            "error": raises :class:`DatabricksApiException`.

        Raises:
            DatabricksApiException: If not found and `if_not_exists=="error"`.
            ValueError: If neither `{id_key}` nor `{name_key}` is in `item`.
        """
        if item_id := item.get(self.id_key):
            return self.delete_by_id(item_id, if_not_exists=if_not_exists)
        elif item_name := item.get(self.name_key):
            return self.delete_by_name(item_name, if_not_exists=if_not_exists)
        else:
            raise ValueError(f"spec must have either {self.id_key!r} or {self.name_key!r}")
