__all__ = ["BuildConfigData"]

from typing import List, Dict, Any, Optional

from dbacademy.clients.dbrest import DBAcademyRestClient
from dbacademy.dbbuild.change_log import ChangeLog
from dbacademy.dbbuild.publish.notebook_def import NotebookDef


class BuildConfigData:

    def __init__(self, *,
                 name: str,
                 supported_dbrs: List[str],
                 ignored_errors: List[str],
                 i18n: bool,
                 i18n_language: Optional[str],
                 cloud: str,
                 version: str,
                 workers: int,
                 job_arguments: Dict[str, Any],
                 source_repo: str,
                 source_dir_name: str,
                 publishing_info: Dict[str, Any],
                 include_solutions: bool,
                 spark_conf: Dict[str, Any],
                 white_list: List[str],
                 black_list: List[str],
                 libraries: List[Dict[str, Any]],
                 client: DBAcademyRestClient,
                 readme_file_name: str,
                 instance_pool_id: str,
                 single_user_name: str,
                 spark_version: str):

        import time, uuid
        from dbacademy import dbgems
        from dbacademy.common import Cloud, validate
        from dbacademy.clients.rest.factory import dbrest_factory
        from dbacademy.dbhelper.course_config import CourseConfig

        self.__validated = False
        self.__created_notebooks = False

        self.__client = client or dbrest_factory.current_workspace()

        # Course Name
        self.__name = validate(name=name).required.str()
        self.__build_name = CourseConfig.to_build_name(name)

        self.__validated = False
        self.__passing_tests: Dict[str, bool] = dict()

        try:
            self.__username = dbgems.sql("SELECT current_user()").first()[0]
        except:
            self.__username = "mickey.mouse@disney.com"  # When unit testing
        validate(username=self.__username).required.str()

        supported_dbrs = validate(supported_dbrs=supported_dbrs).required.as_type(List, str)       # Make sure one of the two values was specified.
        supported_dbrs = supported_dbrs if isinstance(supported_dbrs, list) else [supported_dbrs]  # Convert single values to a list.
        self.__supported_dbrs = validate(supported_dbrs=supported_dbrs).required.list(str)         # Validate the entire set of supported DBRs.

        self.__language_options = None
        self.__ignored_errors = validate(ignored_errors=ignored_errors).optional.list(str, auto_create=True)

        self.__i18n = validate(i18n=i18n or False).required.bool()
        self.__i18n_language = validate(i18n_language=i18n_language).optional.str()

        self.__test_type = None
        self.__notebooks: Dict[str, NotebookDef] = dict()

        # The instance of this test run
        self.__suite_id = f"{time.time()}-{uuid.uuid1()}"

        # The name of the cloud on which these tests were run
        self.__cloud = cloud or Cloud.current_cloud().value

        # The Distribution's version
        self.__version = validate(version=version).required.str()
        self.__core_version = self.version

        # The runtime you wish to test against - lazily evaluated via property
        self.__spark_version = spark_version

        self.__current_cluster = None
        # We can use local-mode clusters here
        self.__workers = validate(workers=workers).required.int()
        self.__instance_pool_id = instance_pool_id      # The cluster's instance pool from which to obtain VMs - lazily evaluated via property
        self.__single_user_name = single_user_name      # The cluster's single-user name - lazily evaluated via property

        # Spark configuration parameters
        self.__spark_conf = validate(spark_conf=spark_conf).optional.dict(str, auto_create=True)
        if self.__workers == 0:
            self.__spark_conf["spark.master"] = "local[*]"

        # Test-Job's arguments
        self.__job_arguments = validate(job_arguments=job_arguments).optional.dict(str, auto_create=True)

        # The libraries to be attached to the cluster
        self.__libraries = validate(libraries=libraries).optional.list(dict, auto_create=True)

        # The offset here assumes we are in the .../Course/Build-Scripts folder
        self.__source_repo = validate(source_repo=source_repo).optional.str() or dbgems.get_notebook_dir(offset=-2)

        source_dir_name: str = validate(source_dir_name=source_dir_name or "Source").required.str()
        self.__source_dir = f"{self.__source_repo}/{source_dir_name}"

        self.__readme_file_name = validate(readme_file_name=readme_file_name or "README.md").optional.str()
        self.__include_solutions = validate(include_solutions=include_solutions or False).required.bool()

        self.__white_list = validate(white_list=white_list).optional.list(str)
        self.__black_list = validate(black_list=black_list).optional.list(str)

        self.__change_log: Optional[ChangeLog] = None
        self.__publishing_info = validate(publishing_info=publishing_info).optional.dict(str, auto_create=True)

    @property
    def passing_tests(self) -> Dict[str, bool]:
        return self.__passing_tests

    @passing_tests.setter
    def passing_tests(self, passing_tests: Dict[str, bool]) -> None:
        self.__passing_tests.clear()
        self.__passing_tests.update(passing_tests)

    @property
    def created_notebooks(self) -> bool:
        return self.__created_notebooks

    @created_notebooks.setter
    def created_notebooks(self, created_notebooks: bool) -> None:
        self.__created_notebooks = created_notebooks

    @property
    def name(self) -> str:
        return self.__name

    @property
    def username(self) -> str:
        return self.__username

    @property
    def supported_dbrs(self) -> List[str]:
        return self.__supported_dbrs

    @property
    def language_options(self) -> List[str]:
        return self.__language_options

    @property
    def ignored_errors(self) -> List[str]:
        return self.__ignored_errors

    @property
    def i18n(self) -> bool:
        return self.__i18n

    @property
    def i18n_language(self) -> Optional[str]:
        return self.__i18n_language

    @property
    def test_type(self) -> str:
        return self.__test_type

    @property
    def notebooks(self) -> Dict[str, NotebookDef]:
        return self.__notebooks

    @property
    def suite_id(self) -> str:
        return self.__suite_id

    @property
    # TODO Update this to type Cloud
    def cloud(self) -> str:
        return self.__cloud

    @property
    def version(self) -> str:
        return self.__version

    @property
    def core_version(self) -> str:
        return self.__core_version

    @property
    def workers(self) -> int:
        return self.__workers

    @property
    def job_arguments(self) -> Dict[str, Any]:
        return self.__job_arguments

    @property
    def source_repo(self) -> str:
        return self.__source_repo

    @property
    def source_dir(self) -> str:
        return self.__source_dir

    @property
    def publishing_info(self) -> Dict[str, Any]:
        return self.__publishing_info

    @property
    def change_log(self) -> Optional[ChangeLog]:
        return self.__change_log

    @change_log.setter
    def change_log(self, change_log: ChangeLog) -> None:
        self.__change_log = change_log

    @property
    def include_solutions(self) -> bool:
        return self.__include_solutions

    @property
    def spark_conf(self) -> Dict[str, Any]:
        return self.__spark_conf

    @property
    def build_name(self) -> str:
        return self.__build_name

    @property
    def white_list(self) -> List[str]:
        return self.__white_list

    @property
    def black_list(self) -> List[str]:
        return self.__black_list

    @property
    def libraries(self) -> List[Dict[str, Any]]:
        return self.__libraries

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def readme_file_name(self):
        return self.__readme_file_name

    @property
    def current_cluster(self) -> Dict[str, Any]:
        """
        Utility method that loads various cluster properties in one REST request to the current cluster vs one per property initialization
        :return: a dictionary of the cluster's properties
        """
        if self.__current_cluster is None:
            self.__current_cluster = self.client.clusters.get_current()

        return self.__current_cluster

    @property
    def instance_pool_id(self) -> str:
        """
        The instance pool to use for testing.
        :return: the cluster's instance_pool_id
        """
        if self.__instance_pool_id is None:  # This may have not been specified upon instantiation
            self.__instance_pool_id = self.current_cluster.get("instance_pool_id")
            assert self.__instance_pool_id is not None, f"The current cluster is not configured to use an instance pool which is required for execution of smoke-tests."

        return self.__instance_pool_id

    @property
    def single_user_name(self) -> str:
        """
        The single-user name.
        :return: The cluster's single_user_name
        """
        if self.__single_user_name is None:  # This may have not been specified upon instantiation
            self.__single_user_name = self.current_cluster.get("single_user_name")
            assert self.__single_user_name is not None, f"The current cluster is not configured for execution as single-user which is required for execution of smoke-tests."

        return self.__single_user_name

    @property
    def spark_version(self) -> str:
        """
        The spark version to use for testing; defaults to the current cluster's spark-version
        :return: The cluster's spark_version
        """
        if self.__spark_version is None:  # This may have not been specified upon instantiation
            self.__spark_version = self.current_cluster.get("spark_version")

        return self.__spark_version

    @property
    def validated(self) -> bool:
        """
        Flag to indicate that the build configuration has been validated.
        :return:
        """
        return self.__validated

    @validated.setter
    def validated(self, validated: bool) -> None:
        self.__validated = validated
