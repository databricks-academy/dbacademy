from typing import Union, Optional, Dict, List

import pyspark
from dbacademy import dbgems, common
from dbacademy.dbrest import DBAcademyRestClient

from .paths_class import Paths


class DBAcademyHelper:
    from .lesson_config_class import LessonConfig
    from .course_config_class import CourseConfig

    DEFAULT_SCHEMA = "default"
    INFORMATION_SCHEMA = "information_schema"
    SPECIAL_SCHEMAS = [DEFAULT_SCHEMA, INFORMATION_SCHEMA]

    SMOKE_TEST_KEY = "dbacademy.smoke-test"

    CATALOG_SPARK_DEFAULT = "spark_catalog"
    CATALOG_UC_DEFAULT = "hive_metastore"

    PROTECTED_EXECUTION = "dbacademy.protected-execution"
    TROUBLESHOOT_ERROR_TEMPLATE = "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information."

    def __init__(self,
                 course_config: CourseConfig,
                 lesson_config: LessonConfig,
                 debug: bool = False):
        """
        Creates an instance of DBAcademyHelper from the specified course and lesson config.
        :param course_config: Those configuration parameters that remain consistent from the duration of a course
        :param lesson_config: Those configuration parameters that may change from lesson to lesson.
        :param debug: Enables debug print messages.
        See also DBAcademyHelper.dprint
        """
        from .workspace_helper_class import WorkspaceHelper
        from .dev_helper_class import DevHelper
        from .validations.validation_helper_class import ValidationHelper

        assert lesson_config is not None, f"The parameter lesson_config:LessonConfig must be specified."
        self.__lesson_config = lesson_config

        assert course_config is not None, f"The parameter course_config:CourseConfig must be specified."
        self.__course_config = course_config

        self.__debug = debug
        self.__start = dbgems.clock_start()
        self.__spark = dbgems.spark

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
        if self.lesson_config.name is None and self.is_smoke_test():
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
        self.staging_source_uri = f"{DBAcademyHelper.get_dbacademy_datasets_staging()}/{self.course_config.data_source_name}/{self.course_config.data_source_version}"
        self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.course_config.data_source_name}/{self.course_config.data_source_version}"
        try:
            files = dbgems.dbutils.fs.ls(self.staging_source_uri)
            if len(files) > 0:
                self.data_source_uri = self.staging_source_uri
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
                           datasets=datasets_path,
                           enable_streaming_support=lesson_config.enable_streaming_support)

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
        by specifying the spark configuration parameter "dbacademy.paths.datasets"
        :return: the location of the DBAcademy datasets.
        """
        return dbgems.spark.conf.get("dbacademy.paths.datasets", default="dbfs:/mnt/dbacademy-datasets")

    @staticmethod
    def get_dbacademy_users_path() -> str:
        """
        This method encapsulates the value resolution for the DBAcademy user's directory. By convention, this should always be "dbfs:/mnt/dbacademy-users"
        however, to support those environments that require that the datasets be hosted at another location, this default value can be overridden
        by specifying the spark configuration parameter "dbacademy.paths.users"
        :return: the location of the DBAcademy user's directory.
        """
        return dbgems.spark.conf.get("dbacademy.paths.users", default="dbfs:/mnt/dbacademy-users")

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
    def current_dbr(self) -> str:
        """
        This value is obtained from a number of different locations, falling back to more expensive REST calls if preferred methods fails. This is due
        to a long-old bug in which the spark version (DBR) expressed in the tags' collection is not always available at the time of execution of a course
        making use of this value flaky.

        Primary location:  dbgems.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None)
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

    @property
    def unique_name(self) -> str:
        """
        See DBAcademyHelper.to_unique_name
        :return: a unique name consisting of the consumer's username, and the course code.
        """
        return self.to_unique_name(username=self.username,
                                   course_code=self.course_config.course_code)

    @staticmethod
    def to_unique_name(*, username: str, course_code: str) -> str:
        """
        A utility function that produces a unique name to be used in creating jobs, warehouses, DLT pipelines, experiments and other user & course specific artifacts.
        The pattern consist of the local part of the email address, a 4-character hash, "da" for DBAcademy and the course's code. The value is then converted to lower case
        after which all non-alpha and non-digits are replaced with hyphens.
        See also DBAcademyHelper.unique_name
        :param username: The full email address of a user. See also LessonConfig.username
        :param course_code: The course's code. See also CourseConfig.course_code
        :return: The unique name composited from the specified username and course_code
        """
        local_part = dbgems.clean_string(username.split("@")[0], "-")
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)
        return f"{local_part}-{username_hash}-da-{course_code}".lower()

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
        :param username: The full email address of a user. See also LessonConfig.username
        :return: The catalog name composited from the specified username
        """
        local_part = username.split("@")[0]
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)

        return dbgems.clean_string(f"{local_part}-{username_hash}-da").lower()

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
    def to_catalog_name(*, username: str, lesson_name: str) -> str:
        """
        A utility function that produces a unique catalog name. The pattern consist of the local part of the email address,
        a 4-character hash and "da" for DBAcademy and the lesson_name. The value is then converted to lower case after which
        all non-alpha and non-digits are replaced with underscores.
        See also DBAcademyHelper.catalog_name
        :param username: The full email address of a user. See also LessonConfig.username
        :param lesson_name: The name of the lesson. See also LessonConfig.name
        :return: The unique name composited from the specified username and lesson
        """
        from dbacademy.dbhelper.lesson_config_class import LessonConfig

        catalog_name_prefix = DBAcademyHelper.to_catalog_name_prefix(username=username)

        if lesson_name is None:
            # With no lesson, catalog and prefix are the same.
            return catalog_name_prefix
        else:
            # Append the lesson name to the catalog name
            clean_name = LessonConfig.to_clean_lesson_name(lesson_name)
            return dbgems.clean_string(f"{catalog_name_prefix}-{clean_name}").lower()

    @property
    def current_catalog(self) -> str:
        """
        :return: spark.sql("SELECT current_catalog()").first()[0]
        """
        return dbgems.spark.sql("SELECT current_catalog()").first()[0]

    @property
    def schema_name_prefix(self) -> str:
        """
        See also DBAcademyHelper.to_schema_name_prefix
        :return: the prefix for all schema names that may be created for this consumer during the usage of a course or "default" if LessonConfig.create_catalog = True
        """
        if self.lesson_config.create_catalog:
            return "default"
        else:
            return self.to_schema_name_prefix(username=self.username,
                                              course_code=self.course_config.course_code)

    @staticmethod
    def to_schema_name_prefix(*, username: str, course_code: str) -> str:
        """
        A utility function that produces a schema name prefix. The pattern consist of the local part of the email address,
        a 4-character hash and "da" for DBAcademy and the course's code. The value is then converted to lower case after
        which all non-alpha and non-digits are replaced with underscores.
        See also DBAcademyHelper.schema_name_prefix
        :param username: The full email address of a user. See also LessonConfig.username
        :param course_code: The course's code. See also CourseConfig.course_code
        :return: The schema name composited from the specified username and course_code
        """
        local_part = username.split("@")[0]
        hash_basis = f"{username}{dbgems.get_workspace_id()}"
        username_hash = dbgems.stable_hash(hash_basis, length=4)
        return dbgems.clean_string(f"{local_part}-{username_hash}-da-{course_code}").lower()

    @property
    def schema_name(self) -> str:
        """
        See also DBAcademyHelper.to_schema_name
        :return: the prescribed name of the schema that the consumer is expected to use or "default" if LessonConfig.create_catalog = True
        """
        if self.lesson_config.create_catalog:
            return "default"
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
        :param username: The full email address of a user. See also LessonConfig.username
        :param course_code: The course's code. See also CourseConfig.course_code
        :param lesson_name: The name of the lesson. See also LessonConfig.name
        :return: The unique name composited from the specified username and lesson
        """
        import re

        if lesson_name is None:
            # No lesson, database name is the same as prefix
            return DBAcademyHelper.to_schema_name_prefix(username=username,
                                                         course_code=course_code)
        else:
            # Schema name includes the lesson name
            clean_name = re.sub(r"[^a-zA-Z\d]", "_", str(lesson_name))
            while "__" in clean_name: clean_name = clean_name.replace("__", "_")

            schema_name_prefix = DBAcademyHelper.to_schema_name_prefix(username=username,
                                                                       course_code=course_code)
            return f"{schema_name_prefix}_{clean_name}"

    @property
    def current_schema(self) -> str:
        """
        :return: spark.sql("SELECT current_database()").first()[0]
        """
        return dbgems.spark.sql("SELECT current_database()").first()[0]

    @staticmethod
    def is_smoke_test() -> bool:
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        return dbgems.spark.conf.get(DBAcademyHelper.SMOKE_TEST_KEY, "false").lower() == "true"

    @common.deprecated("Use dbgems.clock_start() instead.")
    def clock_start(self) -> int:
        return dbgems.clock_start()

    @common.deprecated("Use dbgems.clock_stopped() instead.")
    def clock_stopped(self, start: int, end: str = "") -> str:
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
            self.install_datasets()               # Install the data

        if self.lesson_config.create_catalog: self.__create_catalog()  # Create the UC catalog
        elif self.lesson_config.create_schema: self.__create_schema()  # Create the Schema (is not a catalog)

        self.__initialized = True                 # Set the all-done flag.

    def __create_catalog(self) -> None:
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
        start = dbgems.clock_start()
        # self.created_db = True

        try:
            print(f"Creating & using the schema \"{self.schema_name}\"", end="...")
            dbgems.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name} LOCATION '{self.paths.user_db}'")
            dbgems.sql(f"USE {self.schema_name}")
            print(dbgems.clock_stopped(start))

        except Exception as e:
            raise AssertionError(self.__troubleshoot_error(f"Failed to create the schema \"{self.schema_name}\".", "Cannot Create Schema")) from e

    @common.deprecated(reason="Use DBAcademyHelper.reset_lesson() instead.")
    def reset_environment(self) -> None:
        return self.reset_lesson()

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

        active_streams = len(self.__spark.streams.active) > 0  # Test to see if there are any active streams
        remove_wd = self.paths.exists(self.paths.working_dir)  # Test to see if the working directory exists

        if self.lesson_config.create_catalog:
            drop_catalog = True    # If we created it, we clean it
            drop_schema = False     # But don't the schema
        else:
            drop_catalog = False   # We didn't clean the catalog so don't touch it.
            drop_schema = self.__spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.schema_name}'").count() == 1

        if drop_catalog or drop_schema or remove_wd or active_streams:
            print("Resetting the learning environment:")

        self.__spark.catalog.clearCache()
        self.__cleanup_stop_all_streams()

        if drop_catalog: self.__drop_catalog()
        elif drop_schema: self.__drop_schema()

        if remove_wd: self.__cleanup_working_dir()

        if validate_datasets:
            # The last step is to make sure the datasets are still intact and repair if necessary
            self.validate_datasets(fail_fast=True)

    def __cleanup_working_dir(self) -> None:
        start = dbgems.clock_start()
        print(f"| removing the working directory \"{self.paths.working_dir}\"", end="...")

        dbgems.dbutils.fs.rm(self.paths.working_dir, True)

        print(dbgems.clock_stopped(start))

    @staticmethod
    def __drop_database(schema_name) -> None:
        from pyspark.sql.utils import AnalysisException

        try: location = dbgems.sql(f"DESCRIBE TABLE EXTENDED {schema_name}").filter("col_name == 'Location'").first()["data_type"]
        except Exception: location = None  # Ignore this concurrency error

        try: dbgems.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        try: dbgems.dbutils.fs.rm(location)
        except: pass  # We are going to ignore this as it is most likely deleted or None

    def __drop_schema(self) -> None:

        start = dbgems.clock_start()
        print(f"| dropping the schema \"{self.schema_name}\"", end="...")

        self.__drop_database(self.schema_name)

        print(dbgems.clock_stopped(start))

    def __drop_catalog(self) -> None:
        from pyspark.sql.utils import AnalysisException

        start = dbgems.clock_start()
        print(f"| dropping the catalog \"{self.catalog_name}\"", end="...")

        try: self.__spark.sql(f"DROP CATALOG IF EXISTS {self.catalog_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        print(dbgems.clock_stopped(start))

    def __cleanup_stop_all_streams(self) -> None:
        for stream in self.__spark.streams.active:
            start = dbgems.clock_start()
            print(f"| stopping the stream \"{stream.name}\"", end="...")
            stream.stop()
            try: stream.awaitTermination()
            except: pass  # Bury any exceptions
            print(dbgems.clock_stopped(start))

    def reset_learning_environment(self) -> None:
        """
        Used almost exclusively by the Reset notebook, invocation drops all databases, installed datasets and remove all working directories.
        Usage of this method is generally reserved for a "full" reset of a course which is common before invoke smoke tests.
        :return:
        """
        start = dbgems.clock_start()
        print("Resetting the learning environment:")
        self.__reset_databases()
        self.__reset_datasets()
        self.__reset_working_dir()
        print(f"\nThe learning environment was successfully reset {dbgems.clock_stopped(start)}.")

    def __reset_databases(self) -> None:
        from pyspark.sql.utils import AnalysisException

        # Drop all user-specific catalogs
        catalog_names = [c.catalog for c in dbgems.spark.sql(f"SHOW CATALOGS").collect()]
        for catalog_name in catalog_names:
            if catalog_name.startswith(self.catalog_name_prefix):
                print(f"Dropping the catalog \"{catalog_name}\"")
                try: dbgems.spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
                except AnalysisException: pass  # Ignore this concurrency error

        # Refresh the list of catalogs
        catalog_names = [c.catalog for c in dbgems.spark.sql(f"SHOW CATALOGS").collect()]
        for catalog_name in catalog_names:
            # There are potentially two "default" catalogs from which we need to remove user-specific schemas
            if catalog_name in [DBAcademyHelper.CATALOG_SPARK_DEFAULT, DBAcademyHelper.CATALOG_UC_DEFAULT]:
                schema_names = [d.databaseName for d in dbgems.spark.sql(f"SHOW DATABASES IN {catalog_name}").collect()]
                for schema_name in schema_names:
                    if schema_name.startswith(self.schema_name_prefix) and schema_name != "default":
                        print(f"Dropping the schema \"{catalog_name}.{schema_name}\"")
                        self.__drop_database(f"{catalog_name}.{schema_name}")

    def __reset_working_dir(self) -> None:
        # noinspection PyProtectedMember
        working_dir_root = self.paths._working_dir_root

        if Paths.exists(working_dir_root):
            print(f"Deleting working directory \"{working_dir_root}\".")
            dbgems.dbutils.fs.rm(working_dir_root, True)

    def __reset_datasets(self) -> None:
        if Paths.exists(self.paths.datasets):
            print(f"Deleting datasets \"{self.paths.datasets}\".")
            dbgems.dbutils.fs.rm(self.paths.datasets, True)

    # TODO Not actually used
    def __cleanup_feature_store_tables(self) -> None:
        # noinspection PyUnresolvedReferences,PyPackageRequirements
        from databricks import feature_store

        # noinspection PyUnresolvedReferences
        fs = feature_store.FeatureStoreClient()

        # noinspection PyUnresolvedReferences
        for table in self.client.feature_store.search_tables(max_results=1000000):
            name = table.get("name")
            if name.startswith(self.unique_name):
                print(f"Dropping feature store table {name}")
                fs.drop_table(name)

    # TODO Not actually used
    def __cleanup_mlflow_models(self) -> None:
        import mlflow

        # noinspection PyCallingNonCallable
        for rm in mlflow.list_registered_models(max_results=1000):
            if rm.name.startswith(self.unique_name):
                print(f"Deleting registered model {rm.name}")
                for mv in rm.latest_versions:
                    if mv.current_stage in ["Staging", "Production"]:
                        # noinspection PyUnresolvedReferences
                        mlflow.transition_model_version_stage(name=rm.name, version=mv.version, stage="Archived")

                # noinspection PyUnresolvedReferences
                mlflow.delete_registered_model(rm.name)

    # TODO Not actually used
    # noinspection PyMethodMayBeStatic
    def __cleanup_experiments(self) -> None:
        pass
        # import mlflow
        # experiments = []
        # for experiment in mlflow.list_experiments(max_results=999999):
        #     try:
        #         mlflow.delete_experiment(experiment.experiment_id)
        #     except Exception as e:
        #         print(f"Skipping \"{experiment.name}\"")

    def conclude_setup(self) -> None:
        """
        Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
        """
        print()
        assert self.__initialized, f"We cannot conclude setup without first calling DBAcademyHelper.init(..)"

        # Add custom attributes to the SQL context here.
        self.__spark.conf.set("da.username", self.username)
        self.__spark.conf.set("DA.username", self.username)

        self.__spark.conf.set("da.catalog_name", self.catalog_name or "")
        self.__spark.conf.set("DA.catalog_name", self.catalog_name or "")

        self.__spark.conf.set("da.schema_name", self.schema_name)
        self.__spark.conf.set("DA.schema_name", self.schema_name)

        # Purely for backwards compatability
        self.__spark.conf.set("da.db_name", self.schema_name)
        self.__spark.conf.set("DA.db_name", self.schema_name)

        # Automatically add all path attributes to the SQL context as well.
        for key in self.paths.__dict__:
            if not key.startswith("_"):
                value = self.paths.__dict__[key]
                if value is not None:
                    self.__spark.conf.set(f"da.paths.{key.lower()}", value)
                    self.__spark.conf.set(f"DA.paths.{key.lower()}", value)

        if self.lesson_config.create_catalog:
            # Get the list of schemas from the prescribed catalog
            schemas = [s[0] for s in dbgems.sql(f"SHOW SCHEMAS IN {self.catalog_name}").collect()]
        elif self.lesson_config.requires_uc:
            # No telling how many schemas there may be, we would only care about the default
            schemas = ["default"]
        else:
            # With no catalog, there can only be one schema.
            schemas = [self.schema_name]

        if DBAcademyHelper.INFORMATION_SCHEMA in schemas:
            del schemas[schemas.index(DBAcademyHelper.INFORMATION_SCHEMA)]

        for i, schema in enumerate(schemas):
            if i > 0: print()

            if self.lesson_config.create_catalog:
                # We have a catalog and presumably a default schema
                print(f"Predefined tables in \"{self.catalog_name}.{schema}\":")
                tables = self.__spark.sql(f"SHOW TABLES IN {self.catalog_name}.{schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0: print("| -none-")
                for row in tables: print(f"| {row[0]}")

            elif self.lesson_config.requires_uc:
                # We require UC, but we didn't create the catalog.
                print(f"Using the catalog \"{self.lesson_config.initial_catalog}\" and the schema \"{self.lesson_config.initial_schema}\".")

            elif self.lesson_config.create_schema:
                # Not UC, but we created a schema so there should be tables in it
                # catalog_table = schema if self.env.initial_catalog == DBAcademyHelper.CATALOG_SPARK_DEFAULT else f"{self.env.initial_catalog}.{schema}"

                print(f"Predefined tables in \"{schema}\":")
                tables = self.__spark.sql(f"SHOW TABLES IN {schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0: print("| -none-")
                for row in tables: print(f"| {row[0]}")

            else:
                # Not UC, didn't create the database
                print(f"Using the \"{self.lesson_config.initial_schema}\" schema.")

        print("\nPredefined paths variables:")
        self.paths.print(self_name="DA.")

        print(f"\nSetup completed {dbgems.clock_stopped(self.__start)}")

    def install_datasets(self, reinstall_datasets: bool = False) -> None:
        """
        Install the datasets used by this course to DBFS.

        This ensures that data and compute are in the same region which subsequently mitigates performance issues
        when the storage and compute are, for example, on opposite sides of the world.
        """
        # if not repairing_dataset: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")
        # if not repairing_dataset: print(f"\nYour local dataset directory is {self.paths.datasets}")

        if Paths.exists(self.paths.datasets):
            # It's already installed...
            if reinstall_datasets:
                print(f"\nRemoving previously installed datasets")
                dbgems.dbutils.fs.rm(self.paths.datasets, True)

            if not reinstall_datasets:
                print(f"\nSkipping install of existing datasets to \"{self.paths.datasets}\"")
                self.validate_datasets(fail_fast=False)
                return

        print(f"\nInstalling datasets:")
        print(f"| from \"{self.data_source_uri}\"")
        print(f"| to \"{self.paths.datasets}\"")
        print(f"| NOTE: The datasets that we are installing are located in Washington, USA - depending on the")
        print(f"|       region that your workspace is in, this operation can take as little as {self.course_config.install_min_time} and")
        print(f"|       upwards to {self.course_config.install_max_time}, but this is a one-time operation.")

        # Using data_source_uri is a temporary hack because it assumes we can actually
        # reach the remote repository - in cases where it's blocked, this will fail.
        files = dbgems.dbutils.fs.ls(self.data_source_uri)

        what = "dataset" if len(files) == 1 else "datasets"
        print(f"\nInstalling {len(files)} {what}: ")

        install_start = dbgems.clock_start()
        for f in files:
            start = dbgems.clock_start()
            print(f"| copying /{f.name[:-1]}", end="...")

            source_path = f"{self.data_source_uri}/{f.name}"
            target_path = f"{self.paths.datasets}/{f.name}"

            dbgems.dbutils.fs.cp(source_path, target_path, True)
            print(dbgems.clock_stopped(start))

        self.validate_datasets(fail_fast=False)

        print(f"""\nThe install of the datasets completed successfully {dbgems.clock_stopped(install_start)}""")

    def print_copyrights(self, mappings: Optional[Dict[str, str]] = None) -> None:
        """
        Discovers the datasets used by this course and then prints their corresponding README files which should include the corresponding copyrights.
        :param mappings: Allows for redefinition of the location of the README.md file
        :return:
        """
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

    def list_r(self, path: str, prefix: Optional[str] = None, results: Optional[List[str]] = None) -> List[str]:
        """
        Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
        """
        if prefix is None: prefix = path
        if results is None: results = list()

        try:
            files = dbgems.dbutils.fs.ls(path)
        except:
            files = []

        for file in files:
            data = file.path[len(prefix):]
            results.append(data)
            if file.isDir():
                self.list_r(file.path, prefix, results)

        results.sort()
        return results

    def __validate_spark_version(self) -> None:
        self.__current_dbr = dbgems.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None)

        if self.__current_dbr is None and not dbgems.spark.conf.get(DBAcademyHelper.PROTECTED_EXECUTION, None):
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

        msg = f"The Databricks Runtime is expected to be one of {self.course_config.supported_dbrs}, found \"{self.current_dbr}\"."
        assert self.current_dbr in self.course_config.supported_dbrs, self.__troubleshoot_error(msg, "Spark Version")

    def __validate_dbfs_writes(self, test_dir) -> None:
        from contextlib import redirect_stdout

        if not dbgems.spark.conf.get(DBAcademyHelper.PROTECTED_EXECUTION, None):
            notebook_path = dbgems.clean_string(dbgems.get_notebook_path())
            username = dbgems.clean_string(self.username)
            file = f"{test_dir}/temp/dbacademy-{self.course_config.course_code}-{username}-{notebook_path}.txt"
            try:
                with redirect_stdout(None):
                    dbgems.dbutils.fs.put(file, "Please delete this file", True)
                    dbgems.dbutils.fs.rm(file, True)

            except Exception as e:
                raise AssertionError(self.__troubleshoot_error(f"Unable to write to {file}.", "Cannot Write to DBFS")) from e

    def validate_datasets(self, fail_fast: bool) -> None:
        """
        Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
        """

        validation_start = dbgems.clock_start()

        if self.staging_source_uri == self.data_source_uri:
            # When working with staging data, we need to enumerate what is in there
            # and use it as a definitive source to the complete enumeration of our files
            start = dbgems.clock_start()
            print("\nEnumerating staged files for validation", end="...")
            self.course_config.remote_files = self.list_r(self.staging_source_uri)
            print(dbgems.clock_stopped(start))
            print()

        print(f"\nValidating the locally installed datasets:")

        ############################################################
        # Proceed with the actual validation and repair if possible
        ############################################################

        print("| listing local files", end="...")
        start = dbgems.clock_start()
        local_files = self.list_r(self.paths.datasets)
        print(dbgems.clock_stopped(start))

        ############################################################
        # Repair directories first, this will pick up the majority
        # of the issues by addressing the larger sets.
        ############################################################

        fixes = 0
        repaired_paths = []

        def not_fixed(test_file: str):
            for repaired_path in repaired_paths:
                if test_file.startswith(repaired_path):
                    return False
            return True

        # Remove extra directories (cascade effect vs one file at a time)
        for file in local_files:
            if file not in self.course_config.remote_files and file.endswith("/") and not_fixed(file):
                fixes += 1
                start = dbgems.clock_start()
                repaired_paths.append(file)
                print(f"| removing extra path: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.paths.datasets}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

        # Add extra directories (cascade effect vs one file at a time)
        for file in self.course_config.remote_files:
            if file not in local_files and file.endswith("/") and not_fixed(file):
                fixes += 1
                start = dbgems.clock_start()
                repaired_paths.append(file)
                print(f"| restoring missing path: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.paths.datasets}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

        ############################################################
        # Repair only straggling files
        ############################################################

        # Remove one file at a time (picking up what was not covered by processing directories)
        for file in local_files:
            if file not in self.course_config.remote_files and not file.endswith("/") and not_fixed(file):
                fixes += 1
                start = dbgems.clock_start()
                print(f"| removing extra file: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.paths.datasets}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

        # Add one file at a time (picking up what was not covered by processing directories)
        for file in self.course_config.remote_files:
            if file not in local_files and not file.endswith("/") and not_fixed(file):
                fixes += 1
                start = dbgems.clock_start()
                print(f"| restoring missing file: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.paths.datasets}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

        if fixes == 1: print(f"| fixed 1 issue", end=" ")
        elif fixes > 0: print(f"| fixed {fixes} issues", end=" ")
        else: print(f"| completed", end=" ")
        print(dbgems.clock_stopped(validation_start, " total"))
        print()

        if fail_fast: assert fixes == 0, f"Unexpected modifications to source datasets."

    def run_high_availability_job(self, job_name: str, notebook_path: str) -> None:
        """
        Creates a job using the specified notebook path and job name and then executes it in a "secure" context that allows
        for configuring user specific permissions. This is a hack to our inability to programaticlaly invoke SQL queries, namely GRANT statements
        against a SQL Warehouse and wherein those same queries executed in a notebook has no affect.
        :param job_name: The name to be given to the job.
        :param notebook_path: The path to the notebook
        :return: None
        """

        # job_name = f"DA-{self.course_name}-Configure-Permissions"
        self.client.jobs().delete_by_name(job_name, success_only=False)

        # notebook_path = f"{dbgems.get_notebook_dir()}/Configure-Permissions"

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

        if dbgems.get_job_id():
            mlflow.set_experiment(f"/Curriculum/Test Results/{self.unique_name}-{dbgems.get_job_id()}")

    @staticmethod
    def block_until_stream_is_ready(query: Union[str, pyspark.sql.streaming.StreamingQuery], min_batches: int = 2, delay_seconds: int = 5) -> None:
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
        assert query is not None and type(query) in [str, pyspark.sql.streaming.StreamingQuery], f"Expected the query parameter to be of type \"str\" or \"pyspark.sql.streaming.StreamingQuery\", found \"{type(query)}\"."

        if type(query) != pyspark.sql.streaming.StreamingQuery:
            queries = [aq for aq in dbgems.spark.streams.active if aq.name == query]
            while len(queries) == 0:
                print("The query is not yet active...")
                time.sleep(delay_seconds)  # Give it a couple of seconds
                queries = [aq for aq in dbgems.spark.streams.active if aq.name == query]

            if len(queries) > 1: raise ValueError(f"More than one spark query was found for the name \"{query}\".")
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
