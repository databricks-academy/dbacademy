from typing import Union, Optional, Dict


class DBAcademyHelper:
    from dbacademy import common
    from dbacademy.dbhelper.lesson_config_class import LessonConfig
    from dbacademy.dbhelper.course_config_class import CourseConfig
    from pyspark.sql.streaming import StreamingQuery

    SCHEMA_DEFAULT = "default"
    SCHEMA_INFORMATION = "information_schema"

    SPARK_CONF_SMOKE_TEST = "dbacademy.smoke-test"
    SPARK_CONF_PATHS_DATASETS = "dbacademy.paths.datasets"
    SPARK_CONF_PATHS_USERS = "dbacademy.paths.users"
    SPARK_CONF_DATA_SOURCE_URI = "dbacademy.data-source-uri"
    SPARK_CONF_PROTECTED_EXECUTION = "dbacademy.protected-execution"
    SPARK_CONF_CLUSTER_TAG_SPARK_VERSION = "spark.databricks.clusterUsageTags.sparkVersion"

    CATALOG_SPARK_DEFAULT = "spark_catalog"
    CATALOG_UC_DEFAULT = "hive_metastore"

    TROUBLESHOOT_ERROR_TEMPLATE = "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information."

    def __init__(self,
                 course_config: CourseConfig,
                 lesson_config: LessonConfig,
                 debug: bool = False):
        """
        Creates an instance of DBAcademyHelper from the specified course and lesson config
        :param course_config: Those configuration parameters that remain consistent from the duration of a course
        :param lesson_config: Those configuration parameters that may change from lesson to lesson
        :param debug: Enables debug print messages.
        See also DBAcademyHelper.dprint
        """
        from dbacademy import dbgems
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper
        from dbacademy.dbhelper.dev_helper_class import DevHelper
        from dbacademy.dbhelper.validations.validation_helper_class import ValidationHelper
        from dbacademy.dbhelper.paths_class import Paths

        assert lesson_config is not None, f"The parameter lesson_config:LessonConfig must be specified."
        lesson_config.assert_valid()
        self.__lesson_config = lesson_config

        assert course_config is not None, f"The parameter course_config:CourseConfig must be specified."
        self.__course_config = course_config

        self.__debug = debug
        self.__start = dbgems.clock_start()

        # Initialized in the call to init()
        self.__initialized = False
        self.__smoke_test_lesson = False

        # Standard initialization
        self.naming_params = {"course": self.course_config.course_code}

        # The following objects provide advanced support for modifying the learning environment.
        self.client = DBAcademyRestClient()
        self.workspace = WorkspaceHelper(self)
        self.dev = DevHelper(self)
        self.tests = ValidationHelper(self)

        # Are we running under test? If so we can "optimize" for parallel execution
        # without affecting the student's runtime-experience. As in the student can
        # use one working directory and one database, but under test, we can use many
        if self.lesson_config.name is None and DBAcademyHelper.is_smoke_test():
            # The developer did not define a lesson and this is a smoke
            # test, so we can define a lesson here for the sake of testing

            # import re, hashlib
            # encoded_value = dbgems.get_notebook_path().encode('utf-8')
            # hash()
            # hashed_value = hashlib.sha3_512(encoded_value).hexdigest()
            # self.lesson_config.name = str(abs(int(re.sub(r"[a-z]", "", hashed_value))) & 10000)
            # We don't need repeatable has values here - once the lesson is done, we are done with this value.
            self.lesson_config.name = str(abs(hash(dbgems.get_notebook_path())) % 10000)
            self.__smoke_test_lesson = True

        # Convert any lesson value we have to lower case.
        self.lesson_config.name = None if self.lesson_config.name is None else self.lesson_config.name.lower()

        # Define username using the hive function (cleaner than notebooks API)
        self.username = self.lesson_config.username

        # This is the location in our Azure data repository of the datasets for this lesson
        self.__staging_source_uri = f"{DBAcademyHelper.get_dbacademy_datasets_staging()}/{self.course_config.data_source_name}/{self.course_config.data_source_version}"
        default_data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.course_config.data_source_name}/{self.course_config.data_source_version}"
        self.__data_source_uri = dbgems.get_parameter(DBAcademyHelper.SPARK_CONF_DATA_SOURCE_URI, default_data_source_uri)
        try:
            files = dbgems.dbutils.fs.ls(self.staging_source_uri)
            if len(files) > 0:
                self.__data_source_uri = self.staging_source_uri
                print("*"*80)
                print(f"* Found staged datasets - using alternate installation location:")
                print(f"* {self.staging_source_uri}")
                print("*"*80)
                print()
        except:
            pass

        ###########################################################################################
        # This next section is varies its configuration based on whether the lesson is
        # specifying the lesson name or if one can be generated automatically. As such that
        # content-developer specified lesson name integrates into the various parameters.
        ###########################################################################################

        # This is where the datasets will be downloaded to and should be treated as read-only for all practical purposes
        datasets_path = f"{DBAcademyHelper.get_dbacademy_datasets_path()}/{self.course_config.data_source_name}/{self.course_config.data_source_version}"

        self.paths = Paths(lesson_config=self.lesson_config,
                           working_dir_root=self.working_dir_root,
                           datasets=datasets_path)

        self.__lesson_config.lock_mutations()

        # With requirements initialized, we can
        # test various assertions about our environment
        self.__validate_spark_version()
        self.__validate_dbfs_writes(DBAcademyHelper.get_dbacademy_users_path())
        self.__validate_dbfs_writes(DBAcademyHelper.get_dbacademy_datasets_path())

    @staticmethod
    def get_dbacademy_datasets_path() -> str:
        """
        This method encapsulates the value resolution for the DBAcademy datasets. By convention, this should always be "dbfs:/mnt/dbacademy-datasets"
        however, to support those environments that require that the datasets be hosted at another location, this default value can be overridden
        by specifying the spark configuration parameter identified by DBAcademyHelper.SPARK_CONF_PATHS_DATASETS
        :return: the location of the DBAcademy datasets.
        """
        from dbacademy import dbgems
        return dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_PATHS_DATASETS, default="dbfs:/mnt/dbacademy-datasets")

    @staticmethod
    def get_dbacademy_users_path() -> str:
        """
        This method encapsulates the value resolution for the DBAcademy user's directory. By convention, this should always be "dbfs:/mnt/dbacademy-users"
        however, to support those environments that require that the datasets be hosted at another location, this default value can be overridden
        by specifying the spark configuration parameter identified by DBAcademyHelper.SPARK_CONF_PATHS_USERS
        :return: the location of the DBAcademy user's directory.
        """
        from dbacademy import dbgems
        return dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_PATHS_USERS, default="dbfs:/mnt/dbacademy-users")

    @staticmethod
    def get_dbacademy_datasets_staging() -> str:
        """
        This location is inspected upon initialization to determine where datasets should be loaded from. If a corresponding dataset is found in this location,
        under the CourseConfig.data_source_name and CourseConfig.data_source_version folders, then data will be installed from this staging repo enabling
        authoring of both courseware content and courseware datasets without the overhead of premature publishing of a dataset for a course under active development.
        :return: The location of the staging datasets.
        """
        return "dbfs:/mnt/dbacademy-datasets-staging"

    @property
    def staging_source_uri(self):
        """
        The location of the staging dataset for this course. When data is found at this location, this data will be
        loaded and validated against as compared to the datasets in the more repository that is typically installed.
        :return:
        """
        return self.__staging_source_uri

    @property
    def data_source_uri(self):
        """
        The location of the datasets for this course. This is the path from which all datasets will be installed
        from to bring data into the workspace and avoid cloud-cloud file reads through the entire course.
        :return:
        """
        return self.__data_source_uri

    @property
    def current_dbr(self) -> str:
        """
        This value is obtained from a number of different locations, falling back to more expensive REST calls if preferred methods fails. This is due
        to a long-old bug in which the spark version (DBR) expressed in the tags' collection is not always available at the time of execution of a course
        making use of this value flaky.

        Primary location:  dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_CLUSTER_TAG_SPARK_VERSION, None)
        Secondary location: dbgems.get_tag("sparkVersion", None)
        Trinary location: DBAcademyHelper.client.clusters.get_current_spark_version()
        :return: the current DBR for the consumer's cluster.
        """
        return self.__current_dbr

    @property
    def course_config(self) -> CourseConfig:
        """
        :return: the course's general configuration
        """
        return self.__course_config

    @property
    def lesson_config(self) -> LessonConfig:
        """
        :return: the lesson specific configuration
        """
        return self.__lesson_config

    def dprint(self, message: str) -> None:
        """
        Prints the specified message if DBAcademyHelper was initialized with debug=True
        :param message: The message to be printed
        :return: None
        """
        if self.__debug:
            print(f"DEBUG: {message}")

    @property
    def working_dir_root(self) -> str:
        """
        This is the common super-directory for each lesson, removal of which is designed to ensure
        that all assets created by students is removed. As such, it is not attached to the path
        object to hide it from students. Used almost exclusively in the Rest notebook.
        :return: the location of the working directory root
        """
        return f"{DBAcademyHelper.get_dbacademy_users_path()}/{self.username}/{self.course_config.course_name}"

    def unique_name(self, sep: str) -> str:
        """
        See DBAcademyHelper.to_unique_name
        :param sep: The seperator to use between words
        :return: a unique name consisting of the consumer's username, and the course code.
        """
        return self.to_unique_name(username=self.username,
                                   course_code=self.course_config.course_code,
                                   lesson_name=self.lesson_config.name,
                                   sep=sep)

    @staticmethod
    def to_unique_name(*, username: str, course_code: str, lesson_name: Optional[str], sep: str) -> str:
        """
        A utility function that produces a unique name to be used in creating jobs, warehouses, DLT pipelines, experiments and other user & course specific artifacts.
        The pattern consist of the local part of the email address, a 4-character hash, "da" for DBAcademy and the course's code. The value is then converted to lower case
        after which all non-alpha and non-digits are replaced with hyphens.
        See also DBAcademyHelper.unique_name
        :param username: The full email address of a user. See also `LessonConfig.username`
        :param course_code: The course's code. See also `CourseConfig.course_code`
        :param lesson_name: The lesson's name (usually None). See also `LessonConfig.name`
        :param sep: The seperator to use between words, defaults to a hyphen
        :return: The unique name composited from the specified username and course_code
        """
        from dbacademy import dbgems

        local_part = username.split("@")[0]
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)

        if lesson_name is None:
            name = f"{local_part} {username_hash} da {course_code}"
        else:
            name = f"{local_part} {username_hash} da {course_code} {lesson_name}"

        return dbgems.clean_string(name, replacement=sep).lower()

    @property
    def catalog_name_prefix(self) -> str:
        """
        See also DBAcademyHelper.to_catalog_name_prefix
        :return: the prefix for all catalog names that may be created for this consumer during the usage of a course.
        """
        return self.to_catalog_name_prefix(username=self.username)

    @staticmethod
    def to_catalog_name_prefix(*, username: str) -> str:
        """
        A utility function that produces a catalog name prefix. The pattern consist of the local part of the email address,
        a 4-character hash and "da" for DBAcademy. The value is then converted to lower case after which all non-alpha and
        non-digits are replaced with underscores.
        See also DBAcademyHelper.catalog_name_prefix
        :param username: The full email address of a user. See also `LessonConfig.username`
        :return: The catalog name composited from the specified username
        """
        return DBAcademyHelper.to_catalog_name(username=username, lesson_name=None)

    @property
    def catalog_name(self) -> str:
        """
        Unlike DBAcademy.schema_name, this value is not course specific providing for one catalog per user for all courses.
        See also DBAcademyHelper.to_catalog_name
        :return: the prescribed name of the catalog that the consumer is expected to use.
        """
        if not self.lesson_config.is_uc_enabled_workspace:
            # If this is not a UC workspace, then we use the default for spark
            return DBAcademyHelper.CATALOG_SPARK_DEFAULT

        elif not self.lesson_config.create_catalog:
            # We are not creating a catalog, use teh default value
            return DBAcademyHelper.CATALOG_UC_DEFAULT

        else:
            # If we are creating a catalog, we will use a user-specific catalog
            return self.to_catalog_name(username=self.username,
                                        lesson_name=self.lesson_config.name)

    @staticmethod
    def to_catalog_name(*, username: str, lesson_name: Optional[str]) -> str:
        """
        A utility function that produces a unique catalog name. The pattern consist of the local part of the email address,
        a 4-character hash and "da" for DBAcademy and the lesson_name. The value is then converted to lower case after which
        all non-alpha and non-digits are replaced with underscores.
        See also DBAcademyHelper.catalog_name
        :param username: The full email address of a user. See also `LessonConfig.username`
        :param lesson_name: The name of the lesson. See also LessonConfig.name
        :return: The unique name composited from the specified username and lesson
        """
        from dbacademy import dbgems
        from dbacademy.dbhelper.lesson_config_class import LessonConfig

        local_part = username.split("@")[0]
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)

        if lesson_name is None:
            # With no lesson, catalog and prefix are the same.
            return dbgems.clean_string(f"{local_part}-{username_hash}-da").lower()
        else:
            # Append the lesson name to the catalog name
            clean_name = LessonConfig.to_clean_lesson_name(lesson_name)
            return dbgems.clean_string(f"{local_part}-{username_hash}-da-{clean_name}").lower()

    @property
    def current_catalog(self) -> str:
        """
        :return: spark.sql("SELECT current_catalog()").first()[0]
        """
        from dbacademy import dbgems
        return dbgems.spark.sql("SELECT current_catalog()").first()[0]

    @property
    def schema_name_prefix(self) -> str:
        """
        See also DBAcademyHelper.to_schema_name_prefix
        :return: the prefix for all schema names that may be created for this consumer during the usage of a course or DBAcademyHelper.SCHEMA_DEFAULT if LessonConfig.create_catalog = True
        """
        if self.lesson_config.create_catalog:
            return DBAcademyHelper.SCHEMA_DEFAULT
        else:
            return self.to_schema_name_prefix(username=self.username,
                                              course_code=self.course_config.course_code)

    @staticmethod
    def to_schema_name_prefix(*, username: str, course_code: str) -> str:
        """
        Shorthand for DBAcademyHelper.to_schema_name(username=username, course_code=course_code, lesson_name=None)
        :param username: The full email address of a user. See also `LessonConfig.username`
        :param course_code: The course's code. See also CourseConfig.course_code
        :return: The schema name composited from the specified username and course_code
        """
        return DBAcademyHelper.to_schema_name(username=username,
                                              course_code=course_code,
                                              lesson_name=None)

    @property
    def schema_name(self) -> str:
        """
        See also DBAcademyHelper.to_schema_name
        :return: the prescribed name of the schema that the consumer is expected to use or DBAcademyHelper.SCHEMA_DEFAULT if LessonConfig.create_catalog = True
        """
        if self.lesson_config.create_catalog:
            return DBAcademyHelper.SCHEMA_DEFAULT
        else:
            return self.to_schema_name(username=self.username,
                                       course_code=self.course_config.course_code,
                                       lesson_name=self.lesson_config.name)

    @staticmethod
    def to_schema_name(*, username: str, course_code: str, lesson_name: Optional[str]) -> str:
        """
        A utility function that produces a unique schema name. The pattern consist of the local part of the email address,
        a 4-character hash and "da" for DBAcademy, course's code and the lesson_name. The value is then converted to lower case after which
        all non-alpha and non-digits are replaced with underscores.
        See also DBAcademyHelper.catalog_name
        :param username: The full email address of a user. See also `LessonConfig.username`
        :param course_code: The course's code. See also CourseConfig.course_code
        :param lesson_name: The name of the lesson. See also LessonConfig.name
        :return: The unique name composited from the specified username and lesson
        """
        from dbacademy import dbgems
        from dbacademy.dbhelper.lesson_config_class import LessonConfig

        local_part = username.split("@")[0]
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)

        if lesson_name is None:
            # No lesson, database name is the same as prefix
            # return DBAcademyHelper.to_schema_name_prefix(username=username, course_code=course_code)
            return dbgems.clean_string(f"{local_part}-{username_hash}-da-{course_code}").lower()

        else:
            # Schema name includes the lesson name
            clean_name = LessonConfig.to_clean_lesson_name(lesson_name)
            return dbgems.clean_string(f"{local_part}-{username_hash}-da-{course_code}-{clean_name}").lower()

    @property
    def current_schema(self) -> str:
        """
        :return: spark.sql("SELECT current_database()").first()[0]
        """
        from dbacademy import dbgems
        return dbgems.spark.sql("SELECT current_database()").first()[0]

    @staticmethod
    def is_smoke_test() -> bool:
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        from dbacademy import dbgems
        return dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_SMOKE_TEST, "false").lower() == "true"

    @common.deprecated("Use dbgems.clock_start() instead.")
    def clock_start(self) -> int:
        from dbacademy import dbgems
        return dbgems.clock_start()

    @common.deprecated("Use dbgems.clock_stopped() instead.")
    def clock_stopped(self, start: int, end: str = "") -> str:
        from dbacademy import dbgems
        return dbgems.clock_stopped(start, end)

    # noinspection PyMethodMayBeStatic
    def __troubleshoot_error(self, error: str, section: str) -> str:
        return DBAcademyHelper.TROUBLESHOOT_ERROR_TEMPLATE.format(error=error, section=section)

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necessary,
        this pattern does allow each function to be defined in its own cell which makes authoring notebooks a little easier.
        """
        import inspect

        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """

        setattr(DBAcademyHelper, function_ref.__name__, function_ref)

        return None if delete else function_ref

    def init(self) -> None:
        """
        Initializes the DBAcademyHelper object allowing for customization of the initialization processes such as calling DBAcademy.reset_lesson()
        after instantiation but before initialization.
        :return: None
        """

        if self.lesson_config.create_catalog:
            assert not self.lesson_config.create_schema, f"Creation of the schema (LessonConfig.create_schema=True) is not supported while creating the catalog (LessonConfig.create_catalog=True)"

        if self.lesson_config.installing_datasets:
            self.install_datasets()

        if self.lesson_config.create_catalog:
            self.__create_catalog()  # Create the UC catalog
        elif self.lesson_config.create_schema:
            self.__create_schema()  # Create the Schema (is not a catalog)

        self.__initialized = True  # Set the all-done flag.

    def __create_catalog(self) -> None:
        from dbacademy import dbgems
        try:
            start = dbgems.clock_start()
            print(f"Creating & using the catalog \"{self.catalog_name}\"", end="...")
            dbgems.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
            dbgems.sql(f"USE CATALOG {self.catalog_name}")

            dbgems.sql(f"CREATE DATABASE IF NOT EXISTS default")
            dbgems.sql(f"USE default")

            print(dbgems.clock_stopped(start))

        except Exception as e:
            raise AssertionError(self.__troubleshoot_error(f"Failed to create the catalog \"{self.catalog_name}\".", "Cannot Create Catalog")) from e

    def __create_schema(self) -> None:
        from dbacademy import dbgems

        start = dbgems.clock_start()
        try:

            print(f"\nCreating & using the schema \"{self.schema_name}\" in the catalog \"{self.catalog_name}\"", end="...")
            dbgems.sql(f"USE CATALOG {self.catalog_name}")
            dbgems.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name} LOCATION '{self.paths.user_db}'")
            dbgems.sql(f"USE {self.schema_name}")
            print(dbgems.clock_stopped(start))

        except Exception as e:
            raise AssertionError(self.__troubleshoot_error(f"Failed to create the schema \"{self.schema_name}\".", "Cannot Create Schema")) from e

    def reset_lesson(self) -> None:
        """
        Resets the lesson by calling DBAcademy.clean(validate_datasets=False)
        :return: None
        """
        return self.cleanup(validate_datasets=False)

    def cleanup(self, validate_datasets: bool = True) -> None:
        """
        Cleans up the user environment by stopping any active streams,
        dropping the database created by the call to init(),
        cleaning out the user-specific catalog, and removing the user's
        lesson-specific working directory and any assets created in that directory.
        """
        from dbacademy.dbhelper.workspace_cleaner_class import WorkspaceCleaner
        from dbacademy.dbhelper.dataset_manager_class import DatasetManager

        WorkspaceCleaner(self).reset_lesson()

        if validate_datasets:
            # The last step is to make sure the datasets are still intact and repair if necessary
            DatasetManager.from_dbacademy_helper(self).validate_datasets(fail_fast=True)

    def reset_learning_environment(self) -> None:
        """
        Used almost exclusively by the Reset notebook, invocation drops all databases, installed datasets and remove all working directories.
        Usage of this method is generally reserved for a "full" reset of a course which is common before invoke smoke tests.
        :return:
        """
        from dbacademy.dbhelper.workspace_cleaner_class import WorkspaceCleaner

        WorkspaceCleaner(self).reset_learning_environment()

    def conclude_setup(self) -> None:
        """
        Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
        """
        from dbacademy import dbgems

        print()
        assert self.__initialized, f"We cannot conclude setup without first calling DBAcademyHelper.init(..)"

        # Add custom attributes to the SQL context here.
        dbgems.set_spark_config("da.username", self.username)
        dbgems.set_spark_config("DA.username", self.username)

        dbgems.set_spark_config("da.catalog_name", self.catalog_name or "")
        dbgems.set_spark_config("DA.catalog_name", self.catalog_name or "")

        dbgems.set_spark_config("da.schema_name", self.schema_name)
        dbgems.set_spark_config("DA.schema_name", self.schema_name)

        # Purely for backwards compatability
        dbgems.set_spark_config("da.db_name", self.schema_name)
        dbgems.set_spark_config("DA.db_name", self.schema_name)

        # Automatically add all path attributes to the SQL context as well.
        for key in self.paths.__dict__:
            if not key.startswith("_"):
                value = self.paths.__dict__[key]
                if value is not None:
                    dbgems.set_spark_config(f"da.paths.{key.lower()}", value)
                    dbgems.set_spark_config(f"DA.paths.{key.lower()}", value)

        if self.lesson_config.create_catalog:
            # Get the list of schemas from the prescribed catalog
            schemas = [s[0] for s in dbgems.sql(f"SHOW SCHEMAS IN {self.catalog_name}").collect()]
        elif self.lesson_config.requires_uc:
            # No telling how many schemas there may be, we would only care about the default
            schemas = [DBAcademyHelper.SCHEMA_DEFAULT]
        else:
            # With no catalog, there can only be one schema.
            schemas = [self.schema_name]

        if DBAcademyHelper.SCHEMA_INFORMATION in schemas:
            del schemas[schemas.index(DBAcademyHelper.SCHEMA_INFORMATION)]

        for i, schema in enumerate(schemas):
            if i > 0:
                print()

            if self.lesson_config.create_catalog:
                # We have a catalog and presumably a default schema
                print(f"Predefined tables in \"{self.catalog_name}.{schema}\":")
                tables = dbgems.sql(f"SHOW TABLES IN {self.catalog_name}.{schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0:
                    print("| -none-")
                for row in tables:
                    print(f"| {row[0]}")

            elif self.lesson_config.requires_uc:
                # We require UC, but we didn't create the catalog.
                print(f"Using the catalog \"{self.lesson_config.initial_catalog}\" and the schema \"{self.lesson_config.initial_schema}\".")

            elif self.lesson_config.create_schema:
                # Not UC, but we created a schema so there should be tables in it
                # catalog_table = schema if self.env.initial_catalog == DBAcademyHelper.CATALOG_SPARK_DEFAULT else f"{self.env.initial_catalog}.{schema}"

                print(f"Predefined tables in \"{schema}\":")
                tables = dbgems.sql(f"SHOW TABLES IN {schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0:
                    print("| -none-")
                for row in tables:
                    print(f"| {row[0]}")

            else:
                # Not UC, didn't create the database
                print(f"Using the \"{self.lesson_config.initial_schema}\" schema.")

        print("\nPredefined paths variables:")
        self.paths.print(self_name="DA.")

        print(f"\nSetup completed {dbgems.clock_stopped(self.__start)}")

    def install_datasets(self, reinstall_datasets: bool = False) -> None:
        from dbacademy.dbhelper.dataset_manager_class import DatasetManager

        dataset_manager = DatasetManager.from_dbacademy_helper(self)
        dataset_manager.install_dataset(install_min_time=self.course_config.install_min_time,
                                        install_max_time=self.course_config.install_max_time,
                                        reinstall_datasets=reinstall_datasets)

    def print_copyrights(self, mappings: Optional[Dict[str, str]] = None) -> None:
        """
        Discovers the datasets used by this course and then prints their corresponding README files which should include the corresponding copyrights.
        :param mappings: Allows for redefinition of the location of the README.md file
        :return:
        """
        from dbacademy import dbgems

        if mappings is None:
            mappings = dict()

        datasets = [f for f in dbgems.dbutils.fs.ls(self.paths.datasets)]

        for dataset in datasets:
            readme_file = mappings.get(dataset.name, "README.md")
            readme_path = f"{dataset.path}{readme_file}"
            try:
                with open(readme_path.replace("dbfs:/", "/dbfs/")) as f:
                    contents = f.read()
                    lines = len(contents.split("\n")) + 1

                    html = f"""<html><body><h1>{dataset.path}</h1><textarea rows="{lines}" style="width:100%; overflow-x:scroll; white-space:nowrap">{contents}</textarea></body></html>"""
                    dbgems.display_html(html)

            except FileNotFoundError:
                html = f"""<html><body><h1>{dataset.path}</h1><textarea rows="3" style="width:100%; overflow-x:scroll; white-space:nowrap">**ERROR**\n{readme_file} was not found</textarea></body></html>"""
                dbgems.display_html(html)

    def __validate_spark_version(self) -> None:
        from dbacademy import dbgems

        self.__current_dbr = dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_CLUSTER_TAG_SPARK_VERSION, None)

        if self.__current_dbr is None and not dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_PROTECTED_EXECUTION, None):
            # In some random scenarios, the spark version will come back None because the tags are not yet initialized.
            # In this case, we can query the cluster and ask it for its spark version instead.
            self.__current_dbr = dbgems.get_tag("sparkVersion", None)

        try:
            if self.__current_dbr is None:
                self.__current_dbr = self.client.clusters.get_current_spark_version()
        except:
            warning = f"We are unable to obtain the Databricks Runtime value for your current session.\n" + \
                      f"Please be aware that this courseware may not execute properly unless you are using\n" + \
                      f"one of this course's supported DBRs:\n" + \
                      f"{self.course_config.supported_dbrs}"
            dbgems.print_warning("Spark Version", self.__troubleshoot_error(warning, "Spark Version"))

        message = f"The Databricks Runtime is expected to be one of {self.course_config.supported_dbrs}, found \"{self.current_dbr}\"."
        if self.current_dbr not in self.course_config.supported_dbrs:
            # This is an unsupported DBR
            if dbgems.get_spark_config("dbacademy.runtime.check", "fail").lower() == "warn":
                dbgems.print_warning("Unsupported DBR", message=message)
            else:
                raise AssertionError(self.__troubleshoot_error(message, "Spark Version"))

    def __validate_dbfs_writes(self, test_dir) -> None:
        from dbacademy import dbgems
        from contextlib import redirect_stdout

        if not dbgems.get_spark_config(DBAcademyHelper.SPARK_CONF_PROTECTED_EXECUTION, None):
            notebook_path = dbgems.clean_string(dbgems.get_notebook_path())
            username = dbgems.clean_string(self.username)
            file = f"{test_dir}/temp/dbacademy-{self.course_config.course_code}-{username}-{notebook_path}.txt"
            try:
                with redirect_stdout(None):
                    dbgems.dbutils.fs.put(file, "Please delete this file", True)
                    dbgems.dbutils.fs.rm(file, True)

            except Exception as e:
                raise AssertionError(self.__troubleshoot_error(f"Unable to write to {file}.", "Cannot Write to DBFS")) from e

    def run_high_availability_job(self, job_name: str, notebook_path: str) -> None:
        """
        Creates a job using the specified notebook path and job name and then executes it in a "secure" context that allows
        for configuring user specific permissions. This is a hack to our inability to programmatically invoke SQL queries, namely GRANT statements
        against a SQL Warehouse and wherein those same queries executed in a notebook has no affect
        :param job_name: The name to be given to the job
        :param notebook_path: The path to the notebook
        :return: None
        """

        self.client.jobs().delete_by_name(job_name, success_only=False)

        params = {
            "name": job_name,
            "tags": {
                "dbacademy.course": self.course_config.course_name,
                "dbacademy.source": self.course_config.course_name
            },
            "email_notifications": {},
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "format": "MULTI_TASK",
            "tasks": [
                {
                    "task_key": "Configure-Permissions",
                    "description": "Configure all users' permissions for user-specific databases.",
                    "libraries": [],
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": []
                    },
                    "new_cluster": {
                        "num_workers": 0,
                        "cluster_name": "",
                        "spark_conf": {
                            "spark.master": "local[*]",
                            "spark.databricks.acl.dfAclsEnabled": "true",
                            "spark.databricks.repl.allowedLanguages": "sql,python",
                            "spark.databricks.cluster.profile": "serverless"
                        },
                        "runtime_engine": "STANDARD",
                        "spark_env_vars": {
                            "WSFS_ENABLE_WRITE_SUPPORT": "true"
                        },
                    },
                },
            ],
        }
        cluster_params = params.get("tasks")[0].get("new_cluster")
        cluster_params["spark_version"] = self.client.clusters().get_current_spark_version()

        if self.client.clusters().get_current_instance_pool_id() is not None:
            cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:
            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        create_response = self.client.jobs().create(params)
        job_id = create_response.get("job_id")

        run_response = self.client.jobs().run_now(job_id)
        run_id = run_response.get("run_id")

        final_response = self.client.runs().wait_for(run_id)

        final_state = final_response.get("state").get("result_state")
        assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"

        self.client.jobs().delete_by_name(job_name, success_only=False)

        print()
        print("Update completed successfully.")

    def init_mlflow_as_job(self) -> None:
        """
        Used to initialize MLflow with the job ID when ran under test.
        """
        import mlflow
        from dbacademy import dbgems

        if dbgems.get_job_id():
            unique_name = self.unique_name("-")
            mlflow.set_experiment(f"/Curriculum/Test Results/{unique_name}-{dbgems.get_job_id()}")

    @staticmethod
    def block_until_stream_is_ready(query: Union[str, StreamingQuery], min_batches: int = 2, delay_seconds: int = 5) -> None:
        """
        A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different use cases.

        The first use case is in jobs where the stream is started in one cell but execution of subsequent cells start prematurely.

        The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.

        The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command.
        :param query:
        :param min_batches:
        :param delay_seconds:
        :return:
        """
        import time, pyspark
        from dbacademy import dbgems
        from pyspark.sql.streaming import StreamingQuery

        assert query is not None and type(query) in [str, StreamingQuery], f"Expected the query parameter to be of type \"str\" or \"pyspark.sql.streaming.StreamingQuery\", found \"{type(query)}\"."

        if type(query) != pyspark.sql.streaming.StreamingQuery:
            queries = [aq for aq in dbgems.spark.streams.active if aq.name == query]
            while len(queries) == 0:
                print("The query is not yet active...")
                time.sleep(delay_seconds)  # Give it a couple of seconds
                queries = [aq for aq in dbgems.spark.streams.active if aq.name == query]

            if len(queries) > 1:
                raise ValueError(f"More than one spark query was found for the name \"{query}\".")
            query = queries[0]

        while True:
            count = len(query.recentProgress)
            print(f"Processed {count} of {min_batches} batches...")

            if not query.isActive:
                print("The query is no longer active...")
                break
            elif count >= min_batches:
                break

            time.sleep(delay_seconds)  # Give it a couple of seconds

        count = len(query.recentProgress)
        print(f"The stream is now active with {count} batches having been processed.")
