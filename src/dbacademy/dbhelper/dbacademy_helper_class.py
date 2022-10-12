from dbacademy_gems import dbgems
from .lesson_config_class import LessonConfig
from .course_config_class import CourseConfig


class DBAcademyHelper:
    import pyspark
    from typing import Union

    DEFAULT_SCHEMA = "default"
    INFORMATION_SCHEMA = "information_schema"
    SPECIAL_SCHEMAS = [DEFAULT_SCHEMA, INFORMATION_SCHEMA]

    SMOKE_TEST_KEY = "dbacademy.smoke-test"

    CATALOG_SPARK_DEFAULT = "spark_catalog"
    CATALOG_UC_DEFAULT = "hive_metastore"

    REQUIREMENTS = []

    def __init__(self, *,
                 course_config: CourseConfig,
                 lesson_config: LessonConfig,
                 debug: bool = False):

        from dbacademy_helper.paths_class import Paths
        from dbacademy.dbrest import DBAcademyRestClient
        from .workspace_helper import WorkspaceHelper
        from .dev_helper import DevHelper
        from .tests import TestHelper

        assert lesson_config is not None, f"The parameter lesson_config:LessonConfig must be specified."
        self.__lesson_config = lesson_config

        assert course_config is not None, f"The parameter course_config:CourseConfig must be specified."
        self.__course_config = course_config

        self.__debug = debug
        self.__start = self.clock_start()
        self.__spark = dbgems.spark

        # Initialized in the call to init()
        self.__initialized = False
        self.__smoke_test_lesson = False
        # self.created_db = False
        # self.created_catalog = False

        # Standard initialization
        self.naming_params = {"course": self.course_config.course_code}

        # The following objects provide advanced support for modifying the learning environment.
        self.client = DBAcademyRestClient()
        self.workspace = WorkspaceHelper(self)
        self.dev = DevHelper(self)
        self.tests = TestHelper(self)

        # With requirements initialized, we can assert our spark versions.
        self.__current_dbr = None

        if len(self.course_config.supported_dbrs) > 0:
            assert self.current_dbr in self.course_config.supported_dbrs, self.__troubleshoot_error(f"The Databricks Runtime is expected to be one of {self.course_config.supported_dbrs}, found \"{self.current_dbr}\".", "Spark Version")

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
        self.staging_source_uri = f"dbfs:/mnt/dbacademy-datasets-staging/{self.course_config.data_source_name}/{self.course_config.data_source_version}"
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
        datasets_path = f"dbfs:/mnt/dbacademy-datasets/{self.course_config.data_source_name}/{self.course_config.data_source_version}"

        self.paths = Paths(lesson_config=self.lesson_config,
                           working_dir_root=self.working_dir_root,
                           datasets=datasets_path,
                           enable_streaming_support=lesson_config.enable_streaming_support)

    @property
    def current_dbr(self):
        if self.__current_dbr is None:
            self.__current_dbr = self.client.clusters.get_current_spark_version()

        return self.__current_dbr

    @property
    def course_config(self) -> CourseConfig:
        return self.__course_config

    @property
    def lesson_config(self) -> LessonConfig:
        return self.__lesson_config

    def dprint(self, message):
        if self.__debug:
            print(f"DEBUG: {message}")

    @property
    def working_dir_root(self) -> str:
        # This is the common super-directory for each lesson, removal of which is designed to ensure
        # that all assets created by students is removed. As such, it is not attached to the path
        # object to hide it from students. Used almost exclusively in the Rest notebook.
        return f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_config.course_name}"

    @property
    def unique_name(self) -> str:
        return self.to_unique_name(self.username)

    def to_unique_name(self, username):
        local_part = username.split("@")[0]
        username_hash = dbgems.stable_hash(username, length=4)
        course_code = self.course_config.course_code
        return f"{local_part}-{username_hash}-dbacademy-{course_code}".lower()

    @property
    def catalog_name_prefix(self):
        return self.to_catalog_name_prefix(self.username)

    def to_catalog_name_prefix(self, username):
        local_part = username.split("@")[0]
        username_hash = dbgems.stable_hash(username, length=4)
        course_code = self.course_config.course_code
        return DBAcademyHelper.clean_string(f"{local_part}-{username_hash}-dbacademy-{course_code}").lower()

    @property
    def catalog_name(self):

        if not self.lesson_config.is_uc_enabled_workspace:
            # If this is not a UC workspace, then we use the default for spark
            return DBAcademyHelper.CATALOG_SPARK_DEFAULT

        elif not self.lesson_config.created_catalog:
            # We are not creating a catalog, use teh default value
            return DBAcademyHelper.CATALOG_UC_DEFAULT

        else:
            # If we are creating a catalog, we will use a user-specific catalog
            return self.to_catalog_name(self.username)

    def to_catalog_name(self, username: str) -> str:
        catalog_name_prefix = self.to_catalog_name_prefix(username)

        if self.lesson_config.name is None:
            # With no lesson, catalog and prefix are the same.
            return catalog_name_prefix
        else:
            # Append the lesson name to the catalog name
            return DBAcademyHelper.clean_string(f"{catalog_name_prefix}-{self.lesson_config.clean_name}").lower()

    @property
    def schema_name_prefix(self):
        if self.lesson_config.created_catalog:
            return "default"
        else:
            return self.to_schema_name_prefix(username=self.username)

    def to_schema_name_prefix(self, username: str) -> str:
        local_part = username.split("@")[0]
        username_hash = dbgems.stable_hash(username, length=4)
        course_code = self.course_config.course_code
        return DBAcademyHelper.clean_string(f"da-{local_part}-{username_hash}-{course_code}").lower()

    @property
    def schema_name(self):
        return self.to_schema_name(self.username)

    def to_schema_name(self, username: str) -> str:
        if self.lesson_config.name is None:
            # No lesson, database name is the same as prefix
            return self.to_schema_name_prefix(username)
        else:
            # Schema name includes the lesson name
            return f"{self.to_schema_name_prefix(username)}_{self.lesson_config.clean_name}"

    @staticmethod
    def is_smoke_test():
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        return dbgems.spark.conf.get(DBAcademyHelper.SMOKE_TEST_KEY, "false").lower() == "true"

    # noinspection PyMethodMayBeStatic
    def clock_start(self):
        import time
        return int(time.time())

    # noinspection PyMethodMayBeStatic
    def clock_stopped(self, start):
        import time
        return f"({int(time.time()) - start} seconds)"

    # noinspection PyMethodMayBeStatic
    def __troubleshoot_error(self, error, section):
        return f"{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information."

    @property
    def __requires_uc(self):
        return self.lesson_config is not None and self.lesson_config.requires_uc
        # return DBAcademyHelper.REQUIREMENTS_UC in self.requirements

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

    def init(self):

        if self.lesson_config.installing_datasets:
            self.install_datasets()               # Install the data

        if self.lesson_config.created_catalog:
            self.__create_catalog()               # Create the UC catalog
        if self.lesson_config.created_schema:
            self.__create_schema()                # Create the Schema (is not a catalog)

        self.__initialized = True                 # Set the all-done flag.

    def __create_catalog(self):
        try:
            start = self.clock_start()
            print(f"Creating & using the catalog \"{self.catalog_name}\"", end="...")
            dbgems.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
            dbgems.sql(f"USE CATALOG {self.catalog_name}")

            dbgems.sql(f"CREATE DATABASE IF NOT EXISTS default")
            dbgems.sql(f"USE default")

            print(self.clock_stopped(start))

        except Exception as e:
            raise AssertionError(self.__troubleshoot_error(f"Failed to create the catalog \"{self.catalog_name}\".", "Cannot Create Catalog")) from e

    def __create_schema(self):
        start = self.clock_start()
        # self.created_db = True

        try:
            print(f"Creating & using the schema \"{self.schema_name}\"", end="...")
            dbgems.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name} LOCATION '{self.paths.user_db}'")
            dbgems.sql(f"USE {self.schema_name}")
            print(self.clock_stopped(start))

        except Exception as e:
            raise AssertionError(self.__troubleshoot_error(f"Failed to create the schema \"{self.schema_name}\".", "Cannot Create Schema")) from e

    @dbgems.deprecated(reason="Use DBAcademyHelper.reset_lesson() instead.")
    def reset_environment(self):
        return self.reset_lesson()

    def reset_lesson(self):
        return self.cleanup(validate_datasets=False)

    def cleanup(self, validate_datasets=True):
        """
        Cleans up the user environment by stopping any active streams,
        dropping the database created by the call to init(),
        cleaning out the user-specific catalog, and removing the user's
        lesson-specific working directory and any assets created in that directory.
        """

        active_streams = len(self.__spark.streams.active) > 0  # Test to see if there are any active streams
        remove_wd = self.paths.exists(self.paths.working_dir)  # Test to see if the working directory exists

        if self.lesson_config.created_catalog:
            drop_catalog = True    # If we created it, we clean it
            drop_schema = False     # But don't the schema
        else:
            drop_catalog = False   # We didn't clean the catalog so don't touch it.
            drop_schema = self.__spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.schema_name}'").count() == 1

        if drop_catalog or drop_schema or remove_wd or active_streams:
            print("Resetting the learning environment...")

        self.__spark.catalog.clearCache()
        self.__cleanup_stop_all_streams()

        if drop_catalog: self.__drop_catalog()
        elif drop_schema: self.__drop_schema()

        if remove_wd: self.__cleanup_working_dir()

        if validate_datasets:
            # The last step is to make sure the datasets are still intact and repair if necessary
            self.validate_datasets(fail_fast=False, repairing_dataset=False)

    def __cleanup_working_dir(self):
        start = self.clock_start()
        print(f"...removing the working directory \"{self.paths.working_dir}\"", end="...")

        dbgems.dbutils.fs.rm(self.paths.working_dir, True)

        print(self.clock_stopped(start))

    @staticmethod
    def __drop_database(schema_name):
        from pyspark.sql.utils import AnalysisException

        try: location = dbgems.sql(f"DESCRIBE TABLE EXTENDED {schema_name}").filter("col_name == 'Location'").first()["data_type"]
        except Exception: location = None  # Ignore this concurrency error

        try: dbgems.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        try: dbgems.dbutils.fs.rm(location)
        except: pass  # We are going to ignore this as it is most likely deleted or None

    def __drop_schema(self):

        start = self.clock_start()
        print(f"...dropping the schema \"{self.schema_name}\"", end="...")

        self.__drop_database(self.schema_name)

        print(self.clock_stopped(start))

    def __drop_catalog(self):
        from pyspark.sql.utils import AnalysisException

        start = self.clock_start()
        print(f"...dropping the catalog \"{self.catalog_name}\"", end="...")

        try: self.__spark.sql(f"DROP CATALOG IF EXISTS {self.catalog_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        print(self.clock_stopped(start))

    def __cleanup_stop_all_streams(self):
        for stream in self.__spark.streams.active:
            start = self.clock_start()
            print(f"...stopping the stream \"{stream.name}\"", end="...")
            stream.stop()
            try: stream.awaitTermination()
            except: pass  # Bury any exceptions
            print(self.clock_stopped(start))

    def reset_learning_environment(self):
        start = self.clock_start()
        print("Resetting the learning environment:")
        self.__reset_databases()
        self.__reset_datasets()
        self.__reset_working_dir()
        print(f"\nThe learning environment was successfully reset {self.clock_stopped(start)}.")

    def __reset_databases(self):
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

    def __reset_working_dir(self):
        from dbacademy_helper.paths_class import Paths

        # noinspection PyProtectedMember
        working_dir_root = self.paths._working_dir_root

        if Paths.exists(working_dir_root):
            print(f"Deleting working directory \"{working_dir_root}\".")
            dbgems.dbutils.fs.rm(working_dir_root, True)

    def __reset_datasets(self):
        from dbacademy_helper.paths_class import Paths

        if Paths.exists(self.paths.datasets):
            print(f"Deleting datasets \"{self.paths.datasets}\".")
            dbgems.dbutils.fs.rm(self.paths.datasets, True)

    def __cleanup_feature_store_tables(self):
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

    def __cleanup_mlflow_models(self):
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

    # noinspection PyMethodMayBeStatic
    def __cleanup_experiments(self):
        pass
        # import mlflow
        # experiments = []
        # for experiment in mlflow.list_experiments(max_results=999999):
        #     try:
        #         mlflow.delete_experiment(experiment.experiment_id)
        #     except Exception as e:
        #         print(f"Skipping \"{experiment.name}\"")

    def conclude_setup(self):
        """
        Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
        """
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

        if self.lesson_config.created_catalog:
            # Get the list of schemas from the prescribed catalog
            schemas = [s[0] for s in dbgems.sql(f"SHOW SCHEMAS IN {self.catalog_name}").collect()]
        elif self.__requires_uc:
            # No telling how many schemas there may be, we would only care about the default
            schemas = ["default"]
        else:
            # With no catalog, there can only be one schema.
            schemas = [self.schema_name]

        if DBAcademyHelper.INFORMATION_SCHEMA in schemas:
            del schemas[schemas.index(DBAcademyHelper.INFORMATION_SCHEMA)]

        for i, schema in enumerate(schemas):
            if i > 0: print()

            if self.lesson_config.created_catalog:
                # We have a catalog and presumably a default schema
                print(f"Predefined tables in \"{self.catalog_name}.{schema}\":")
                tables = self.__spark.sql(f"SHOW TABLES IN {self.catalog_name}.{schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0: print("  -none-")
                for row in tables: print(f"  {row[0]}")

            elif self.__requires_uc:
                # We require UC, but we didn't create the catalog.
                print(f"Using the catalog \"{self.lesson_config.initial_catalog}\" and the schema \"{self.lesson_config.initial_schema}\".")

            elif self.lesson_config.created_schema:
                # Not UC, but we created a schema so there should be tables in it
                # catalog_table = schema if self.env.initial_catalog == DBAcademyHelper.CATALOG_SPARK_DEFAULT else f"{self.env.initial_catalog}.{schema}"

                print(f"Predefined tables in \"{schema}\":")
                tables = self.__spark.sql(f"SHOW TABLES IN {schema}").filter("isTemporary == false").select("tableName").collect()
                if len(tables) == 0: print("  -none-")
                for row in tables: print(f"  {row[0]}")

            else:
                # Not UC, didn't create the database
                print(f"Using the \"{self.lesson_config.initial_schema}\" schema.")

        print("\nPredefined paths variables:")
        self.paths.print(self_name="DA.")

        print(f"\nSetup completed in {self.clock_start() - self.__start} seconds")

    def install_datasets(self, reinstall_datasets=False, repairing_dataset=False):
        """
        Install the datasets used by this course to DBFS.

        This ensures that data and compute are in the same region which subsequently mitigates performance issues
        when the storage and compute are, for example, on opposite sides of the world.
        """
        from dbacademy_helper.paths_class import Paths

        # if not repairing_dataset: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")
#        if not repairing_dataset: print(f"\nYour local dataset directory is {self.paths.datasets}")

        if Paths.exists(self.paths.datasets):
            # It's already installed...
            if reinstall_datasets:
                if not repairing_dataset: print(f"\nRemoving previously installed datasets")
                dbgems.dbutils.fs.rm(self.paths.datasets, True)

            if not reinstall_datasets:
                print(f"\nSkipping install of existing datasets to \"{self.paths.datasets}\"")
                self.validate_datasets(fail_fast=False, repairing_dataset=False)
                return

        print(f"\nInstalling datasets...")
        print(f"...from \"{self.data_source_uri}\"")
        print(f"...to \"{self.paths.datasets}\"")
        print()
        print(f"NOTE: The datasets that we are installing are located in Washington, USA - depending on the")
        print(f"      region that your workspace is in, this operation can take as little as {self.course_config.install_min_time} and")
        print(f"      upwards to {self.course_config.install_max_time}, but this is a one-time operation.")

        # Using data_source_uri is a temporary hack because it assumes we can actually
        # reach the remote repository - in cases where it's blocked, this will fail.
        files = dbgems.dbutils.fs.ls(self.data_source_uri)

        what = "dataset" if len(files) == 1 else "datasets"
        print(f"\nInstalling {len(files)} {what}: ")

        install_start = self.clock_start()
        for f in files:
            start = self.clock_start()
            print(f"Copying /{f.name[:-1]}", end="...")

            source_path = f"{self.data_source_uri}/{f.name}"
            target_path = f"{self.paths.datasets}/{f.name}"

            dbgems.dbutils.fs.cp(source_path, target_path, True)
            print(f"({self.clock_start() - start} seconds)")

        self.validate_datasets(fail_fast=True, repairing_dataset=repairing_dataset)

        print(f"""\nThe install of the datasets completed successfully in {self.clock_start() - install_start} seconds.""")

    def print_copyrights(self, mappings: dict = None):
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

    def list_r(self, path, prefix=None, results=None):
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

    def do_validate(self):
        """
        Utility method to compare local datasets to the registered list of remote files.
        """
        start = self.clock_start()
        local_files = self.list_r(self.paths.datasets)

        errors = []

        for file in local_files:
            if file not in self.course_config.remote_files:
                what = "path" if file.endswith("/") else "file"
                errors.append(f"...Extra {what}: {file}")
                break

        for file in self.course_config.remote_files:
            if file not in local_files:
                what = "path" if file.endswith("/") else "file"
                errors.append(f"...Missing {what}: {file}")
                break

        print(f"({self.clock_start() - start} seconds)")
        for error in errors:
            print(error)

        return len(errors) == 0

    def validate_datasets(self, fail_fast: bool, repairing_dataset: bool):
        """
        Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
        """

        if self.staging_source_uri == self.data_source_uri:
            start = self.clock_start()
            print("\nEnumerating staged files for validation", end="...")
            self.course_config.remote_files = self.list_r(self.staging_source_uri)
            print(self.clock_stopped(start))

        if repairing_dataset:
            print(f"\nRevalidating the locally installed datasets", end="...")
        else:
            print(f"\nValidating the locally installed datasets", end="...")

        result = self.do_validate()

        if not result:
            if fail_fast:
                raise Exception("Validation failed - see previous messages for more information.")
            else:
                print("...Attempting to repair locally installed dataset")
                self.install_datasets(reinstall_datasets=True, repairing_dataset=True)

    def run_high_availability_job(self, job_name, notebook_path):

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

    def init_mlflow_as_job(self):
        """
        Used to initialize MLflow with the job ID when ran under test.
        """
        import mlflow

        if dbgems.get_job_id():
            mlflow.set_experiment(f"/Curriculum/Test Results/{self.unique_name}-{dbgems.get_job_id()}")

    @staticmethod
    def clean_string(value, replacement: str = "_"):
        import re
        replacement_2x = replacement+replacement
        value = re.sub(r"[^a-zA-Z\d]", replacement, str(value))
        while replacement_2x in value: value = value.replace(replacement_2x, replacement)
        return value

    @staticmethod
    def block_until_stream_is_ready(query: Union[str, pyspark.sql.streaming.StreamingQuery], min_batches: int = 2, delay_seconds: int = 5):
        """
        A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different use cases.

        The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.

        The first use case is in jobs where the stream is started in one cell but execution of subsequent cells start prematurely.

        The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command

        :param query: An instance of a query object or the name of the query
        :param min_batches: The minimum number of batches to be processed before allowing execution to continue.
        :param delay_seconds: The amount of delay in seconds between each test.
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
