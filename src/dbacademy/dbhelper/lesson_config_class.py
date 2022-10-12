class LessonConfig:
    def __init__(self, *,
                 name: str,
                 create_schema: bool,
                 create_catalog: bool,
                 requires_uc: bool,
                 installing_datasets: bool,
                 enable_streaming_support: bool):

        from dbacademy_gems import dbgems

        self.name = name

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
        import re
        self.__name = name

        if name is None:
            self.__clean_name = None
        else:
            value = re.sub(r"[^a-zA-Z\d]", "_", str(name))
            while "__" in value: value = value.replace("__", "_")
            self.__clean_name = name

    @property
    def clean_name(self) -> str:
        return self.__clean_name

    @property
    def enable_streaming_support(self) -> bool:
        return self.__enable_streaming_support

    @property
    def requires_uc(self) -> bool:
        return self.__requires_uc

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
