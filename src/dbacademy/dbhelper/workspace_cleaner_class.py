from dbacademy import dbgems
from dbacademy.dbhelper.paths_class import Paths
from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper


class WorkspaceCleaner:
    from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
    
    def __init__(self, da: DBAcademyHelper):
        self.__da = da
        self.__unique_name = None

    def reset_lesson(self) -> None:

        status = False
        if self.__da.lesson_config.name is None:
            print(f"Resetting the learning environment:")
        else:
            print(f"Resetting the learning environment ({self.__da.lesson_config.name}):")

        dbgems.spark.catalog.clearCache()
        status = self._stop_all_streams() or status

        if self.__da.course_config.ml_feature_enabled:
            status = self._drop_feature_store_tables(lesson_only=True) or status
            status = self._cleanup_mlflow_endpoints(lesson_only=True) or status
            status = self._cleanup_mlflow_models(lesson_only=True) or status
            status = self._cleanup_experiments(lesson_only=True) or status

        status = self._drop_catalog() or status
        status = self._drop_schema() or status

        # Always last to remove DB files that are not removed by sql-drop operations.
        status = self._cleanup_working_dir() or status

        if not status:
            print("| No action taken")

    def reset_learning_environment(self) -> None:

        print("Resetting the learning environment for all lessons:")

        start = dbgems.clock_start()

        dbgems.spark.catalog.clearCache()
        self._stop_all_streams()

        if self.__da.course_config.ml_feature_enabled:
            self._drop_feature_store_tables(lesson_only=False)
            self._cleanup_mlflow_endpoints(lesson_only=False)
            self._cleanup_mlflow_models(lesson_only=False)
            self._cleanup_experiments(lesson_only=False)

        self._reset_databases()
        self._reset_datasets()
        self._reset_working_dir()

        print(f"| the learning environment was successfully reset {dbgems.clock_stopped(start)}.")

    def _reset_working_dir(self) -> None:
        # noinspection PyProtectedMember
        working_dir_root = self.__da.paths._working_dir_root

        if Paths.exists(working_dir_root):
            print(f"| deleting working directory \"{working_dir_root}\".")
            dbgems.dbutils.fs.rm(working_dir_root, True)

    def _reset_datasets(self) -> None:
        if Paths.exists(self.__da.paths.datasets):
            print(f"| deleting datasets \"{self.__da.paths.datasets}\".")
            dbgems.dbutils.fs.rm(self.__da.paths.datasets, True)

    def _reset_databases(self) -> None:
        from pyspark.sql.utils import AnalysisException

        # Drop all user-specific catalogs
        catalog_names = [c.catalog for c in dbgems.spark.sql(f"SHOW CATALOGS").collect()]
        for catalog_name in catalog_names:
            if catalog_name.startswith(self.__da.catalog_name_prefix):
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
                    if schema_name.startswith(self.__da.schema_name_prefix) and schema_name != DBAcademyHelper.SCHEMA_DEFAULT:
                        print(f"| dropping the schema \"{catalog_name}.{schema_name}\"")
                        self._drop_database(f"{catalog_name}.{schema_name}")

    @staticmethod
    def _drop_database(schema_name) -> None:
        from pyspark.sql.utils import AnalysisException

        try: location = dbgems.sql(f"DESCRIBE TABLE EXTENDED {schema_name}").filter("col_name == 'Location'").first()["data_type"]
        except Exception: location = None  # Ignore this concurrency error

        try: dbgems.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        try: dbgems.dbutils.fs.rm(location)
        except: pass  # We are going to ignore this as it is most likely deleted or None

    def _drop_catalog(self) -> bool:
        from pyspark.sql.utils import AnalysisException

        if not self.__da.lesson_config.create_catalog:
            return False  # If we don't create the catalog, don't drop it

        start = dbgems.clock_start()
        print(f"| dropping the catalog \"{self.__da.catalog_name}\"", end="...")

        try: 
            dbgems.spark.sql(f"DROP CATALOG IF EXISTS {self.__da.catalog_name} CASCADE")
        except AnalysisException: 
            pass  # Ignore this concurrency error

        print(dbgems.clock_stopped(start))
        return True

    def _drop_schema(self) -> bool:

        if self.__da.lesson_config.create_catalog:
            return False  # If we create the catalog, we don't drop the schema
        elif dbgems.spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.__da.schema_name}'").count() == 0:
            return False  # If the database doesn't exist, it cannot be dropped

        start = dbgems.clock_start()
        print(f"| dropping the schema \"{self.__da.schema_name}\"", end="...")

        self._drop_database(self.__da.schema_name)

        print(dbgems.clock_stopped(start))
        return True

    @staticmethod
    def _stop_all_streams() -> bool:

        if len(dbgems.spark.streams.active) == 0:
            return False  # Bail if there are no active streams

        for stream in dbgems.spark.streams.active:
            start = dbgems.clock_start()
            print(f"| stopping the stream \"{stream.name}\"", end="...")
            stream.stop()
            try:
                stream.awaitTermination()
            except:
                pass  # Bury any exceptions
            print(dbgems.clock_stopped(start))
        
        return True

    def _cleanup_working_dir(self) -> bool:

        if not self.__da.paths.exists(self.__da.paths.working_dir):
            return False  # Bail if the directory doesn't exist

        start = dbgems.clock_start()
        print(f"| removing the working directory \"{self.__da.paths.working_dir}\"", end="...")

        dbgems.dbutils.fs.rm(self.__da.paths.working_dir, True)

        print(dbgems.clock_stopped(start))
        return True

    def _drop_feature_store_tables(self, lesson_only: bool) -> bool:
        import logging
        from databricks import feature_store

        prefix = self.__da.schema_name if lesson_only else self.__da.schema_name_prefix
        items = self.__da.client.ml.feature_store.search_tables()
        feature_store_tables = [i for i in items if i.get("name").startswith(prefix)]

        if len(feature_store_tables) == 0:
            return False  # No tables, nothing to drop

        logger = logging.getLogger("databricks.feature_store._compute_client._compute_client")
        logger_disabled = logger.disabled
        logger.disabled = True

        try:
            for table in feature_store_tables:
                name = table.get("name")
                print(f"| dropping feature store table \"{name}\"")
                feature_store.FeatureStoreClient().drop_table(name)
        finally:
            logger.disabled = logger_disabled

        return True

    def _get_unique_name(self, lesson_only: bool) -> str:
        if self.__unique_name is not None:
            return self.__unique_name

        if lesson_only:
            self.__unique_name = self.__da.unique_name("-")
        else:
            self.__unique_name = self.__da.to_unique_name(username=self.__da.username, course_code=self.__da.course_config.course_code, lesson_name=None, sep="-")

        return self.__unique_name

    def _cleanup_experiments(self, lesson_only: bool) -> bool:
        import mlflow
        from mlflow.entities import ViewType

        start = dbgems.clock_start()

        experiments = mlflow.search_experiments(view_type=ViewType.ACTIVE_ONLY)
        experiments = [e for e in experiments if e.name.split("/")[-1].startswith(self._get_unique_name(lesson_only))]

        if len(experiments) == 0:
            return False

        # Not our normal pattern, but the goal here is to report on ourselves only if experiments were found.
        print(f"| enumerating MLflow Experiments...{dbgems.clock_stopped(start)}")

        for experiment in experiments:
            status = self.__da.client.workspace.get_status(experiment.name)
            if status and status.get("object_type") == "MLFLOW_EXPERIMENT":
                print(f"| deleting experiment \"{experiment.name}\" ({experiment.experiment_id})")
                mlflow.delete_experiment(experiment.experiment_id)

        return True

    def _cleanup_mlflow_models(self, lesson_only: bool) -> bool:
        import time

        models = []
        start = dbgems.clock_start()

        # Filter out the models that pertain to this course and user
        unique_name = self._get_unique_name(lesson_only)
        for model in self.__da.client.ml.mlflow_models.list():
            name = model.get("name")
            for part in name.split("_"):
                if lesson_only and unique_name == part:
                    models.append(model)
                    # print(f"| Matched model \"{name}\" against \"{unique_name}\" ({lesson_only})")
                elif part.startswith(unique_name):
                    models.append(model)
                    # print(f"| Matched model \"{name}\" against \"{unique_name}\" ({lesson_only})")

        if len(models) == 0:
            return False

        # Not our normal pattern, but the goal here is to report on ourselves only if models were found.
        print(f"| enumerating MLflow models...{dbgems.clock_stopped(start)}")
        active_stages = ["production", "staging"]

        for model in models:
            start = dbgems.clock_start()
            name = model.get("name")
            print(f"| deleting model {name}", end="...")

            for version in self.__da.client.ml.mlflow_model_versions.list(name):
                v = version.get("version")
                stage = version.get("current_stage").lower()
                if stage in active_stages:
                    print(f" archiving model v{v}", end="...")
                    self.__da.client.ml.mlflow_model_versions.transition_stage(name, v, "archived")

            all_archived = False
            while not all_archived:
                all_archived = True  # Assume True at start
                for version in self.__da.client.ml.mlflow_model_versions.list(name):
                    if version.get("current_stage").lower() in active_stages:
                        all_archived = False
                        v = version.get("version")
                        print(f" waiting for v{v}", end="...")
                        time.sleep(5)

            self.__da.client.ml.mlflow_models.delete(name)
            print(dbgems.clock_stopped(start))

        return True

    def _cleanup_mlflow_endpoints(self, lesson_only: bool) -> bool:

        endpoints = []
        start = dbgems.clock_start()

        # Filter out the endpoints that pertain to this course and user
        unique_name = self._get_unique_name(lesson_only)
        for endpoint in self.__da.client.ml.mlflow_endpoints.list_endpoints():
            for part in endpoint.get("registered_model_name").split("_"):
                if lesson_only and unique_name == part:
                    endpoints.append(endpoint)
                elif part.startswith(unique_name):
                    endpoints.append(endpoint)

        if len(endpoints) == 0:
            return False

        # Not our normal pattern, but the goal here is to report on ourselves only if endpoints were found.
        print(f"| enumerating MLflow endpoints...{dbgems.clock_stopped(start)}")

        for endpoint in endpoints:
            name: str = endpoint.get("registered_model_name")
            print(f"| disabling MLflow endpoint \"{name}\"")
            self.__da.client.ml.mlflow_endpoints.disable(name)

        return True
