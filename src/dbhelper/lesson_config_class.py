from typing import Union
from .course_config_class import CourseConfig


class LessonConfig:
    def __init__(self, *,
                 name: str,
                 create_schema: bool,
                 create_catalog: bool,
                 requires_uc: bool,
                 installing_datasets: bool,
                 enable_streaming_support: bool):

        from dbacademy_gems import dbgems

        self.__name = name
        self.__installing_datasets = installing_datasets
        self.__requires_uc = requires_uc
        self.__enable_streaming_support = enable_streaming_support

        # Will be unconditionally True
        self.__created_schema = create_schema
        self.__created_catalog = create_catalog

        try:
            row = dbgems.sql("SELECT current_user() as username, current_catalog() as catalog, current_database() as schema").first()
            self.__username = row["username"]
            self.__initial_catalog = row["catalog"]
            self.__initial_schema = row["schema"]
        except:
            self.__username = "unknown@example.com"     # Because of unit tests
            self.__initial_catalog = "unknown_catalog"  # Because of unit tests
            self.__initial_schema = "unknown_schema"    # Because of unit tests

        if create_catalog:
            assert requires_uc, f"Inconsistent configuration: The parameter \"create_catalog\" was True and \"requires_uc\" was False."
            assert self.is_uc_enabled_workspace, f"Cannot create a catalog, UC is not enabled for this workspace/cluster."
            assert not create_schema, f"Cannot create a user-specific schema when creating UC catalogs"

        if self.is_uc_enabled_workspace:
            from .dbacademy_helper_class import DBAcademyHelper

            # By default, the catalog name will be the same as the default.
            self.__catalog_name = DBAcademyHelper.CATALOG_UC_DEFAULT

            # If we are creating a catalog, we will use a user-specific catalog
            if create_catalog:
                self.__catalog_name = self.to_catalog_name(self.username)

    @staticmethod
    def is_smoke_test():
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        from dbacademy_gems import dbgems
        from .dbacademy_helper_class import DBAcademyHelper
        return dbgems.spark.conf.get(DBAcademyHelper.SMOKE_TEST_KEY, "false").lower() == "true"

    @property
    def installing_datasets(self):
        return self.__installing_datasets

    @property
    def name(self) -> str:
        return self.__name

    @name.setter
    def name(self, name: str):
        self.__name = name

    @property
    def enable_streaming_support(self) -> bool:
        return self.__enable_streaming_support

    @property
    def requires_uc(self) -> bool:
        return self.__requires_uc

    @property
    def catalog_name(self) -> str:
        return self.__catalog_name

    @property
    def is_uc_enabled_workspace(self) -> bool:
        """
        There has to be better ways of implementing this, but it is the only option we have found so far.
        It works when the environment is enabled AND the cluster is configured properly.
        :return: True if this is a UC environment
        """
        from .dbacademy_helper_class import DBAcademyHelper
        return self.initial_catalog == DBAcademyHelper.CATALOG_UC_DEFAULT

    @property
    def initial_catalog(self) -> str:
        return self.__initial_catalog

    @property
    def initial_schema(self) -> str:
        return self.__initial_schema

    @property
    def username(self) -> str:
        return self.__username

    @property
    def created_catalog(self) -> bool:
        return self.__created_catalog

    @property
    def created_schema(self) -> bool:
        return self.__created_schema

    @staticmethod
    def to_catalog_name(username) -> str:
        import re, hashlib
        from .dbacademy_helper_class import DBAcademyHelper

        local_part = username.split("@")[0]  # Split the username, dropping the domain
        value = hashlib.sha3_512(username.encode('utf-8')).hexdigest()
        username_hash = abs(int(re.sub(r"[a-z]", "", value))) & 10000
        return DBAcademyHelper.clean_string(f"{local_part}-{username_hash}-dbacademy").lower()

    @staticmethod
    def to_schema_name(username: str, course: Union[CourseConfig, str]) -> str:
        """
        Given the specified username and course_code, creates a database name that follows the pattern "da-name_prefix@hash-course_code"
        where name_prefix is the right hand of an email as in "john.doe" given "john.doe@example.com", hash is truncated hash based on
        the full email address and course code.
        :param username: The full username (e.g. email address) to compose the database name from.
        :param course: The abbreviated version of the course's name or the CourseConfig object
        :return: Returns the name of the database for the given user and course.
        """
        import re
        from .dbacademy_helper_class import DBAcademyHelper

        assert course is not None, f"The course parameter must be specified."
        course_code = course.course_code if type(course) == CourseConfig else course

        schema_name, da_hash = DBAcademyHelper.to_username_hash(username, course_code)
        schema_name = f"da-{schema_name}@{da_hash}-{course_code}"                # Composite all the values to create the "dirty" database name
        schema_name = re.sub(r"[^a-zA-Z\d]", "_", schema_name)                   # Replace all special characters with underscores (not digit or alpha)
        while "__" in schema_name: schema_name = schema_name.replace("__", "_")  # Replace all double underscores with single underscores
        return schema_name
