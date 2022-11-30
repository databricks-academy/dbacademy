from dbacademy import dbgems
from dbacademy.dbhelper.paths_class import Paths
from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper


class WorkspaceCleaner:
    from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
    
    def __init__(self, da: DBAcademyHelper):
        self.__da = da

        self.announcement = None

    def reset_lesson(self) -> None:

        self.announcement = "Resetting the learning environment:"
        
        dbgems.spark.catalog.clearCache()
        self.__cleanup_stop_all_streams()

        self.__drop_catalog()
        self.__drop_schema()

        self.__drop_feature_store_tables()
        self.__cleanup_mlflow_models()
        self.__cleanup_experiments()

        # Always last to remove DB files that are not removed by sql-drop operations.
        self.__cleanup_working_dir()

    def reset_learning_environment(self) -> None:

        self.announcement = "Resetting the workspaces' learning environment:"

        start = dbgems.clock_start()
        self.__reset_databases()
        self.__reset_datasets()
        self.__reset_working_dir()
        self.__drop_feature_store_tables()
        self.__cleanup_mlflow_models()
        self.__cleanup_experiments()
        
        print(f"\nThe learning environment was successfully reset {dbgems.clock_stopped(start)}.")

    def __print_announcement_once(self, context: str) -> None:
        if self.announcement:
            print(self.announcement, context)
            self.announcement = None

    def __reset_working_dir(self) -> None:
        # noinspection PyProtectedMember
        working_dir_root = self.__da.paths._working_dir_root

        if Paths.exists(working_dir_root):
            print(f"Deleting working directory \"{working_dir_root}\".")
            dbgems.dbutils.fs.rm(working_dir_root, True)

    def __reset_datasets(self) -> None:
        if Paths.exists(self.__da.paths.datasets):
            print(f"Deleting datasets \"{self.__da.paths.datasets}\".")
            dbgems.dbutils.fs.rm(self.__da.paths.datasets, True)

    def __reset_databases(self) -> None:
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
                        print(f"Dropping the schema \"{catalog_name}.{schema_name}\"")
                        self.__drop_database(f"{catalog_name}.{schema_name}")

    @staticmethod
    def __drop_database(schema_name) -> None:
        from pyspark.sql.utils import AnalysisException

        try: location = dbgems.sql(f"DESCRIBE TABLE EXTENDED {schema_name}").filter("col_name == 'Location'").first()["data_type"]
        except Exception: location = None  # Ignore this concurrency error

        try: dbgems.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        except AnalysisException: pass  # Ignore this concurrency error

        try: dbgems.dbutils.fs.rm(location)
        except: pass  # We are going to ignore this as it is most likely deleted or None

    def __drop_catalog(self) -> bool:
        from pyspark.sql.utils import AnalysisException

        if not self.__da.lesson_config.create_catalog:
            return False  # If we don't create the catalog, don't drop it

        self.__print_announcement_once("__drop_catalog")

        start = dbgems.clock_start()
        print(f"| dropping the catalog \"{self.__da.catalog_name}\"", end="...")

        try: 
            dbgems.spark.sql(f"DROP CATALOG IF EXISTS {self.__da.catalog_name} CASCADE")
        except AnalysisException: 
            pass  # Ignore this concurrency error

        print(dbgems.clock_stopped(start))
        return True

    def __drop_schema(self) -> bool:

        if self.__da.lesson_config.create_catalog:
            return False  # If we create the catalog, we don't drop the schema
        elif dbgems.spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.__da.schema_name}'").count() == 0:
            return False  # If the database doesn't exist, it cannot be dropped

        self.__print_announcement_once("__drop_schema")

        start = dbgems.clock_start()
        print(f"| dropping the schema \"{self.__da.schema_name}\"", end="...")

        self.__drop_database(self.__da.schema_name)

        print(dbgems.clock_stopped(start))
        return True

    def __cleanup_stop_all_streams(self) -> None:

        if len(dbgems.spark.streams.active) == 0:
            return  # Bail if there are no active streams

        self.__print_announcement_once("__cleanup_stop_all_streams")

        for stream in dbgems.spark.streams.active:
            start = dbgems.clock_start()
            print(f"| stopping the stream \"{stream.name}\"", end="...")
            stream.stop()
            try:
                stream.awaitTermination()
            except:
                pass  # Bury any exceptions
            print(dbgems.clock_stopped(start))

    def __cleanup_working_dir(self) -> None:

        if not self.__da.paths.exists(self.__da.paths.working_dir):
            return  # Bail if the directory doesn't exist

        self.__print_announcement_once("__cleanup_working_dir")

        start = dbgems.clock_start()
        print(f"| removing the working directory \"{self.__da.paths.working_dir}\"", end="...")

        dbgems.dbutils.fs.rm(self.__da.paths.working_dir, True)

        print(dbgems.clock_stopped(start))

    def __drop_feature_store_tables(self) -> None:
        from databricks import feature_store
        from contextlib import redirect_stdout

        self.__print_announcement_once("__drop_feature_store_tables")

        start = dbgems.clock_start()
        # announcement = f"| Scanning for feature store tables...{dbgems.clock_stopped(start)}"
        fs = feature_store.FeatureStoreClient()
        feature_store_tables = self.__da.client.ml.feature_store.search_tables()

        for table in feature_store_tables:
            name = table.get("name")
            if name.startswith(self.__da.schema_name_prefix):
                # if announcement: print(announcement); announcement = None
                print(f"| Dropping feature store table \"{name}\"")
                with redirect_stdout(None):
                    fs.drop_table(name)
            else:
                # if announcement: print(announcement); announcement = None
                print(f"| Skipping feature store table \"{name}\" {self.__da.schema_name_prefix}")

    def __cleanup_experiments(self) -> None:
        pass
        # import mlflow
        # from mlflow.entities import ViewType
        #
        # self.print_announcement_once()
        #
        # start = dbgems.clock_start()
        # experiments = mlflow.search_experiments(view_type=ViewType.ACTIVE_ONLY)
        # advertisement = f"\nEnumerating MLflow Experiments...{dbgems.clock_stopped(start)}"
        #
        # for experiment in experiments:
        #     if "/" in experiment.name:
        #         last = experiment.name.split("/")[-1]
        #         if last.startswith(self.unique_name):
        #             status = self.client.workspace.get_status(experiment.name)
        #             if status and status.get("object_type") == "MLFLOW_EXPERIMENT":
        #                 if advertisement: print(advertisement); advertisement = None
        #                 print(f"| Deleting experiment \"{experiment.name}\" ({experiment.experiment_id})")
        #                 mlflow.delete_experiment(experiment.experiment_id)
        #             else:
        #                 if advertisement: print(advertisement); advertisement = None
        #                 print(f"| Cannot delete experiment \"{experiment.name}\" ({experiment.experiment_id})")
        #         else:
        #             pass
        #             # print(f"Skipping experiment \"{experiment.name}\" ({experiment.experiment_id})")
        #     else:
        #         print(f"| Skipping experiment \"{experiment.name}\" ({experiment.experiment_id})")
        #

    def __cleanup_mlflow_models(self) -> None:
        pass
        # import mlflow
        # from mlflow.entities import ViewType
        #
        # self.print_announcement_once()
        #
        # start = dbgems.clock_start()
        # print(f"Enumerating MLflow Models", end="...")
        # self.client.ml.mlflow.
        # experiments = mlflow.models.(view_type=ViewType.ACTIVE_ONLY)
        # print(dbgems.clock_stopped(start))
    #
    #     for experiment in experiments:
    #         if "/" in experiment.name:
    #             last = experiment.name.split("/")[-1]
    #             if last.startswith(self.unique_name):
    #                 print(f"Deleting registered model {experiment.name}")
    #                 mlflow.delete_experiment(experiment.experiment_id)
    #             else:
    #                 print(f"Skipping registered model {experiment.name}")
    #         else:
    #             print(f"Skipping registered model {experiment.name}")
    #
    #         # if not rm.name.startswith(self.unique_name):
    #         #     print(f"Skipping registered model {rm.name}")
    #         # else:
    #         #     print(f"Deleting registered model {rm.name}")
    #             # for mv in rm.:
    #             #     if mv.current_stage in ["Staging", "Production"]:
    #             #         # noinspection PyUnresolvedReferences
    #             #         mlflow.transition_model_version_stage(name=rm.name, version=mv.version, stage="Archived")
    #
    #             # noinspection PyUnresolvedReferences
    #             # mlflow.delete_registered_model(rm.name)
