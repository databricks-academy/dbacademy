__all__ = ["BuildConfig", "load_build_config"]

from typing import List, Dict, Any, Optional, Callable, TypeVar, Type, Union
from dbacademy.common import validate, Cloud
from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy.dbbuild.change_log import ChangeLog
from dbacademy.dbbuild.publish.notebook_def import NotebookDef

ParameterType = TypeVar("ParameterType")


class BuildConfig:

    LANGUAGE_OPTIONS_DEFAULT = "Default"

    VERSION_TEST = "Test"
    VERSION_BUILD = "Build"
    VERSION_TRANSLATION = "Translation"
    VERSIONS_LIST = [VERSION_BUILD, VERSION_TEST, VERSION_TRANSLATION]

    @classmethod
    def load(cls, build_config_file_path: str, *, version: str,
             name: str = None,
             supported_dbrs: List[str] = None,
             spark_version: str = None,
             cloud: str = None,
             instance_pool_id: str = None,
             single_user_name: str = None,
             workers: int = 0,
             libraries: List[Dict[str, Any]] = None,
             client: DBAcademyRestClient = None,
             source_dir_name: str = None,
             source_repo: str = None,
             readme_file_name: str = None,
             spark_conf: Dict[str, Any] = None,
             job_arguments: Dict[str, Any] = None,
             include_solutions: bool = True,
             i18n: bool = False,
             i18n_language: str = None,
             ignoring: List[str] = None,
             publishing_info: Dict[str, Any] = None,
             white_list: List[str] = None,
             black_list: List[str] = None,
             notebook_configs: Dict[str, Any] = None):
        """
        This factory method, dbacademy.dbbuild.BuildConfig.load(), has been deprecated. Please use the new factory method
        dbacademy.dbbuild.BuildConfig.load_build_config() or even better, the BuildConfig constructor.
        :param build_config_file_path: The path to the JSON file defining the build's configuration.
        :param version: see the BuildConfig's constructor parameter by the same name.
        :param name: see the BuildConfig's constructor parameter by the same name.
        :param supported_dbrs: see the BuildConfig's constructor parameter by the same name.
        :param spark_version: see the BuildConfig's constructor parameter by the same name.
        :param cloud: see the BuildConfig's constructor parameter by the same name.
        :param instance_pool_id: see the BuildConfig's constructor parameter by the same name.
        :param single_user_name: see the BuildConfig's constructor parameter by the same name.
        :param workers: see the BuildConfig's constructor parameter by the same name.
        :param libraries: see the BuildConfig's constructor parameter by the same name.
        :param client: see the BuildConfig's constructor parameter by the same name.
        :param source_dir_name: see the BuildConfig's constructor parameter by the same name.
        :param source_repo: see the BuildConfig's constructor parameter by the same name.
        :param readme_file_name: see the BuildConfig's constructor parameter by the same name.
        :param spark_conf: see the BuildConfig's constructor parameter by the same name.
        :param job_arguments: see the BuildConfig's constructor parameter by the same name.
        :param include_solutions: see the BuildConfig's constructor parameter by the same name.
        :param i18n: see the BuildConfig's constructor parameter by the same name.
        :param i18n_language: see the BuildConfig's constructor parameter by the same name.
        :param ignoring: see the BuildConfig's constructor parameter by the same name.
        :param publishing_info: see the BuildConfig's constructor parameter by the same name.
        :param white_list: see the BuildConfig's constructor parameter by the same name.
        :param black_list: see the BuildConfig's constructor parameter by the same name.
        :param notebook_configs: see the BuildConfig's initialize_notebooks() parameter by the same name.
        :return: BuildConfig
        """

        _print_build_config_deprecation_warning(_print_warning=True)

        return load_build_config(build_config_file_path=build_config_file_path,
                                 name=name,
                                 version=version,
                                 supported_dbrs=supported_dbrs,
                                 spark_version=spark_version,
                                 cloud=cloud,
                                 instance_pool_id=instance_pool_id,
                                 single_user_name=single_user_name,
                                 workers=workers,
                                 libraries=libraries,
                                 client=client,
                                 source_dir_name=source_dir_name,
                                 source_repo=source_repo,
                                 readme_file_name=readme_file_name,
                                 spark_conf=spark_conf,
                                 job_arguments=job_arguments,
                                 include_solutions=include_solutions,
                                 i18n=i18n,
                                 i18n_language=i18n_language,
                                 ignoring=ignoring,
                                 publishing_info=publishing_info,
                                 white_list=white_list,
                                 black_list=black_list,
                                 notebook_configs=notebook_configs,
                                 _print_warning=False)

    def __init__(self,
                 *,
                 name: str,
                 version: str,
                 supported_dbrs: List[str] = None,
                 spark_version: str = None,
                 cloud: str = None,
                 instance_pool_id: str = None,
                 single_user_name: str = None,
                 workers: int = 0,
                 libraries: List[Dict[str, Any]] = None,
                 client: DBAcademyRestClient = None,
                 source_dir_name: str = None,
                 source_repo: str = None,
                 readme_file_name: str = None,
                 spark_conf: Dict[str, Any] = None,
                 job_arguments: Dict[str, Any] = None,
                 include_solutions: bool = True,
                 i18n: bool = False,
                 i18n_language: str = None,
                 ignoring: List[str] = None,
                 publishing_info: Dict[str, Any] = None,
                 white_list: List[str] = None,
                 black_list: List[str] = None):

        import uuid
        import time
        from dbacademy import dbgems
        from dbacademy.common import Cloud, validate
        from dbacademy.dbbuild.publish.notebook_def import NotebookDef
        from dbacademy.dbhelper.course_config import CourseConfig
        from dbacademy.clients.rest.factory import dbrest_factory

        self.__validated = False
        self.__created_notebooks = False
        self.__passing_tests: Dict[str, bool] = dict()

        try:
            self.__username = dbgems.sql("SELECT current_user()").first()[0]
        except:
            self.__username = "mickey.mouse@disney.com"  # When unit testing
        validate(username=self.__username).required.str()

        self.__supported_dbrs = validate(supported_dbrs=supported_dbrs).list(str, auto_create=True)

        self.__language_options = None
        self.__ignoring = validate(ignoring=ignoring).list(str, auto_create=True)

        self.__i18n = validate(i18n=i18n or False).required.bool()
        self.__i18n_language = validate(i18n_language=i18n_language).str()

        self.__test_type = None
        self.__notebooks: Dict[str, NotebookDef] = dict()

        self.__client = client or dbrest_factory.current_workspace()

        # The instance of this test run
        self.__suite_id = f"{time.time()}-{uuid.uuid1()}"

        # The name of the cloud on which these tests were run
        self.__cloud = cloud or Cloud.current_cloud().value

        # Course Name
        self.__name = validate(name=name).required.str()
        self.__build_name = CourseConfig.to_build_name(name)

        # The Distribution's version
        self.__version = validate(version=version).required.str()
        self.__core_version = self.version

        # The runtime you wish to test against - lazily evaluated via property
        self.__spark_version = spark_version

        # We can use local-mode clusters here
        self.__workers = validate(workers=workers).required.int()

        self.__current_cluster = None
        self.__instance_pool_id = instance_pool_id      # The cluster's instance pool from which to obtain VMs - lazily evaluated via property
        self.__single_user_name = single_user_name      # The cluster's single-user name - lazily evaluated via property

        # Spark configuration parameters
        self.__spark_conf = validate(spark_conf=spark_conf).dict(str, auto_create=True)
        if self.__workers == 0:
            self.__spark_conf["spark.master"] = "local[*]"

        # Test-Job's arguments
        self.__job_arguments = validate(job_arguments=job_arguments).dict(str, auto_create=True)

        # The libraries to be attached to the cluster
        self.__libraries = validate(libraries=libraries).list(dict, auto_create=True)

        # The offset here assumes we are in the .../Course/Build-Scripts folder
        self.__source_repo = validate(source_repo=source_repo).str() or dbgems.get_notebook_dir(offset=-2)

        source_dir_name: str = validate(source_dir=source_dir_name or "Source").required.str()
        self.__source_dir = f"{self.__source_repo}/{source_dir_name}"

        self.__readme_file_name = validate(readme_file_name=readme_file_name or "README.md").str()
        self.__include_solutions = validate(include_solutions=include_solutions or False).required.bool()

        self.__white_list = validate(white_list=white_list).list(str)
        self.__black_list = validate(black_list=black_list).list(str)

        self.__change_log: Optional[ChangeLog] = None
        self.__publishing_info = validate(publishing_info=publishing_info).dict(str, auto_create=True)

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
    def ignoring(self) -> List[str]:
        return self.__ignoring

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

    @property
    def include_solutions(self) -> bool:
        return self.__include_solutions

    @property
    def name(self) -> str:
        return self.__name

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

    def initialize_notebooks(self, notebook_configs: Optional[Dict[str, Any]]) -> None:
        from dbacademy.dbbuild.publish.notebook_def import NotebookDef
        from dbacademy.dbhelper import dbh_constants

        # We don't default to None, require the user to do that, but we support it with an empty list.
        notebook_configs = validate(notebook_configs=notebook_configs).dict(str, auto_create=True)

        # Remove the existing notebooks so that we can recreate them.
        self.notebooks.clear()

        entities = self.client.workspace().ls(self.source_dir, recursive=True)

        if entities is None:
            raise Exception(f"The specified source directory ({self.source_dir}) does not exist.")

        entities.sort(key=lambda e: e["path"])

        has_wip = False
        for i in range(len(entities)):
            entity = entities[i]
            order = i       # Start with the natural order
            test_round = 2  # Default test_round for all notebooks
            include_solution = self.include_solutions  # Initialize to the default value
            path = entity["path"][len(self.source_dir) + 1:]  # Get the notebook's path relative to the source root

            error_message = f"""The {dbh_constants.WORKSPACE_HELPER.WORKSPACE_SETUP} pattern is no longer supported; please update to the {dbh_constants.WORKSPACE_HELPER.UNIVERSAL_WORKSPACE_SETUP} pattern."""
            assert path.lower() != f"includes/{dbh_constants.WORKSPACE_HELPER.WORKSPACE_SETUP.lower()}", error_message

            if "includes/" in path.lower():  # Any folder that ends in "includes/"
                test_round = 0  # Never test notebooks in the "includes" folders

            if path.lower() == "includes/reset":
                order = 0                 # Reset needs to run first.
                test_round = 1            # Add to test_round #1
                include_solution = False  # Exclude from the solutions folder

            if path.lower() == f"includes/{dbh_constants.WORKSPACE_HELPER.WORKSPACE_SETUP.lower()}":
                order = 1                 # Reset needs to run first.
                test_round = 1            # Add to test_round #1
                include_solution = False  # Exclude from the solutions folder

            if "wip" in path.lower():
                has_wip = True
                print(f"""** WARNING ** The notebook "{path}" is excluded from the build as a work in progress (WIP)""")
            else:
                replacements = {"supported_dbrs": ", ".join(self.supported_dbrs)}

                # Add our notebook to the set of notebooks to be tested.
                self.notebooks[path] = NotebookDef(client=self.client,
                                                   test_round=test_round,
                                                   path=path,
                                                   ignored=False,
                                                   include_solution=include_solution,
                                                   replacements=replacements,
                                                   order=order,
                                                   i18n=self.i18n,
                                                   i18n_language=self.i18n_language,
                                                   ignoring=self.ignoring,
                                                   version=self.version)
        if has_wip:
            print()

        for name, notebook_config in notebook_configs.items():
            assert name in self.notebooks, f"The notebook \"{name}\" specified in the notebook configs doesn't exist."
            notebook = self.notebooks.get(name)

            notebook.order =            notebook_config.get("order") if "order" in notebook_config else notebook.order
            notebook.test_round =       notebook_config.get("test_round") if "test_round" in notebook_config else notebook.test_round
            notebook.ignored =          notebook_config.get("ignored") if "ignored" in notebook_config else notebook.ignored
            notebook.include_solution = notebook_config.get("include_solution") if "include_solution" in notebook_config else notebook.include_solution
            notebook.ignoring =         notebook_config.get("ignored_errors") if "ignored_errors" in notebook_config else list()

        # Now that we are all done, we can mark the notebooks as "created"
        self.__created_notebooks = True

    @classmethod
    def get_lesson_number(cls, notebook_path: str):
        """
        Utility function to return the notebook's 2-character numerical prefix.
        :param notebook_path: the path to the notebook.
        :return: the notebook's numerical prefix if the 2-character prefix is numerical, else -1.
        """
        assert notebook_path is not None, f"The parameter \"notebook_path\" must be specified."

        sep = " - "
        if sep not in notebook_path:
            return -1

        prefix = notebook_path.split(sep)[0].strip()
        if prefix == "":
            return -1

        while not prefix[0].isnumeric():
            # Remove the first character
            prefix = prefix[1:]
            if prefix == "":
                return -1

        if prefix.isnumeric():
            return int(prefix)
        else:
            return -1

    def ignore_failures(self, test_function: Callable[[str, int], bool]) -> None:
        """
        Updates the notebook's configuration parameter \"ignored\" based on the result of the provided callable.
        The callable takes two parameters, the first is a string representing the notebook's path, and the second is the notebook's numerical prefix or -1.
        :param test_function: The callable that will return true for which failures will be ignored. Params are path:str and lesson_number:int
        :return:
        """

        for path in list(self.notebooks.keys()):
            number = self.get_lesson_number(path)
            if test_function(path, number):
                self.notebooks[path].ignored = True

    def exclude_notebook(self, test_function: Callable[[str, int], bool]) -> None:
        """
        Removes an existing notebook from the collection of notebooks for this build.
        :param test_function: The callable that will return true if the notebooks should be excluded. Params are path:str and lesson_number:int
        :return:
        """
        for path in list(self.notebooks.keys()):
            number = self.get_lesson_number(path)
            if test_function(path, number):
                del self.notebooks[path]

    def set_test_round(self, test_round: int, test_function: Callable[[str, int], bool]) -> None:
        """
        Updates the notebook's configuration parameter \"test_round\" based on the result of the provided callable.
        :param test_round: The test round to which this notebooks should be assigned.
        :param test_function: The callable that will return true if the notebooks should be excluded. Params are path:str and lesson_number:int
        :return:
        """
        for path in list(self.notebooks.keys()):
            number = self.get_lesson_number(path)
            if test_function(path, number):
                self.notebooks[path].test_round = test_round

    def validate(self, validate_version: bool = True, validate_readme: bool = True):
        """
        Asserts that the build configuration is valid. Upon validating, prints the build parameters
        :param validate_version: Flag to disable validation of the version number
        :param validate_readme: Flag to disable validation of the README file
        :return:
        """
        import json
        assert self.__created_notebooks, f"The notebooks have not yet been initialized; Please call BuildConfig.initialize_notebooks() before proceeding."

        if validate_version:
            self.__validate_version()

        if validate_readme:
            self.__validate_readme()

        print("Build Configuration:")
        print(f"| suite_id:          {self.suite_id}")
        print(f"| name:              {self.name}")
        print(f"| version:           {self.version}")
        print(f"| spark_version:     {self.spark_version}")
        print(f"| workers:           {self.workers}")
        print(f"| instance_pool_id:  {self.instance_pool_id}")
        print(f"| spark_conf:        {self.spark_conf}")
        print(f"| cloud:             {self.cloud}")
        print(f"| libraries:         {json.dumps(self.libraries, indent =4)}")
        print(f"| source_repo:       {self.source_repo}")
        print(f"| source_dir:        {self.source_dir}")
        print(f"| supported_dbrs:    " + ", ".join(self.supported_dbrs))
        print(f"| i18n:              {self.i18n}")
        print(f"| i18n_language:     " + (self.i18n_language if self.i18n_language else "None (English)"))

        if len(self.notebooks) == 0:
            print(f"| notebooks:         none")
        else:
            print(f"| notebooks:         {len(self.notebooks)}")
            self.__index_notebooks()

        self.__validated = True

    @property
    def readme_file_name(self):
        return self.__readme_file_name

    @property
    def current_cluster(self):
        """
        Utility method that loads various cluster properties in one REST request to the current cluster vs one per property initialization
        :return: a dictionary of the cluster's properties
        """
        if self.__current_cluster is None:
            self.__current_cluster = self.client.clusters().get_current()

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

    def __validate_readme(self) -> None:

        if self.version in BuildConfig.VERSIONS_LIST:
            return  # Implies we have an actual version of the form N.N.N
        elif self.i18n_language is not None:
            return  # We are building a translation, presumably days to weeks later, this is not expected to match

        self.__change_log = ChangeLog(source_repo=self.source_repo,
                                      readme_file_name=self.readme_file_name,
                                      target_version=None)

        self.change_log.validate(expected_version=self.core_version, date=None)

    def __validate_version(self):
        if self.version not in BuildConfig.VERSIONS_LIST:
            self.version.split(".")

            msg = f"The version parameter must be one of {BuildConfig.VERSIONS_LIST} or of the form \"N.N.N\" where \"N\" is an integral value, found \"{self.version}\""

            parts = self.version.split(".")
            assert len(parts) == 3, f"{msg}: {len(parts)}"

            major, minor, bug = parts

            assert major.isnumeric(), f"{msg}: major={major}"
            assert minor.isnumeric(), f"{msg}: minor={minor}"
            assert bug.isnumeric(), f"{msg}: bug={bug}"

    def __index_notebooks(self):
        max_name_length = 0
        for path in self.notebooks:
            max_name_length = len(path) if len(path) > max_name_length else max_name_length

        rounds = list(map(lambda notebook_path: self.notebooks.get(notebook_path).test_round, self.notebooks))
        rounds.sort()
        rounds = set(rounds)

        for test_round in rounds:
            if test_round == 0:
                print("\nRound #0: (published but not tested)")
            else:
                print(f"\nRound #{test_round}")

            notebook_paths = list(self.notebooks.keys())
            notebook_paths.sort()

            # for path in notebook_paths:
            for notebook in sorted(self.notebooks.values(), key=lambda n: n.order):
                # notebook = self.notebooks[path]
                if test_round == notebook.test_round:
                    path = notebook.path.ljust(max_name_length)
                    ignored = str(notebook.ignored).ljust(5)
                    include_solution = str(notebook.include_solution).ljust(5)

                    replacements = notebook.replacements.copy()  # Take a deep copy to minimize noise
                    if "required_dbrs" in replacements:
                        del replacements["required_dbrs"]

                    replacements_copy = replacements.copy()
                    if "supported_dbrs" in replacements_copy:
                        del replacements_copy["supported_dbrs"]

                    if len(replacements_copy) == 0:
                        print(f"{notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements=None")
                    else:
                        print(f"{notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements={{")
                        max_key_length = 0
                        for key in replacements_copy:
                            max_key_length = len(key) if len(key) > max_key_length else max_key_length

                        for key in replacements_copy:
                            value = replacements_copy.get(key)
                            print(f"     {key}", end="")
                            print(f" " * (max_key_length - len(key)), end="")
                            print(f": {value}")
                        print("     }")

    # Used by notebooks
    # TODO Cannot define return type
    def to_resource_diff(self):
        """
        Creates an instance of ResourceDiff from the current build configuration
        :return: An instance of ResourceDiff
        """
        from dbacademy.dbbuild.publish.resource_diff_class import ResourceDiff
        assert self.validated, f"Cannot diff until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed."

        return ResourceDiff(self)

    # Used by notebooks
    # TODO Cannot define return type
    def to_publisher(self, publishing_mode: Optional[str] = None):
        """
        Creates an instance of Publisher from the current build configuration
        :param publishing_mode: See Publisher.publishing_mode
        :return: the current publishing mode
        """
        from dbacademy.dbbuild.publish.publisher_class import Publisher
        assert self.validated, f"Cannot publish until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

        return Publisher(self, publishing_mode)

    # Used by notebooks
    # TODO Cannot define return type
    def to_translator(self, require_i18n_selection: bool = True):
        """
        Creates an instance of Translator from the current build configuration.
        :return:
        """
        publisher = self.to_publisher(publishing_mode=None)
        publisher.validate(silent=True)
        return publisher.to_translator(require_i18n_selection)

    # Used by notebooks
    # TODO Cannot define return type
    def to_test_suite(self, test_type: str = None, keep_success: bool = False):
        """
        Creates an instance of TestSuite from the current build configuration
        :param test_type: See TestSuite.test_type
        :param keep_success: See TestSuite.keep_success
        :return:
        """
        from dbacademy.dbbuild.test.test_suite_class import TestSuite

        assert self.validated, f"Cannot test until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

        return TestSuite(build_config=self,
                         test_dir=self.source_dir,
                         test_type=test_type,
                         keep_success=keep_success)

    def assert_all_tests_passed(self, clouds: List[str] = None) -> None:
        """
        Asserts that tests for the specified clouds have passed
        :param clouds: The list of clouds consisting of the values "AWS", "MSA", "GCP" and if None, will default to a list containing all three
        :return: None
        """

        if self.version in BuildConfig.VERSIONS_LIST:
            return  # This is Test, Build or Translation and as such does not need to be validated.

        clouds = clouds or ["AWS", "MSA", "GCP"]
        clouds = [c.upper() for c in clouds]

        for cloud in clouds:
            assert cloud in self.__passing_tests, f"The tests for the cloud {cloud} and version {self.version} were not found. Please run the corresponding smoke tests before proceeding."
            assert self.__passing_tests.get(cloud), f"The tests for the cloud {cloud} and version {self.version} did not pass. Please address the test failures and run the corresponding smoke tests before proceeding."

    def validate_all_tests_passed(self, cloud: Union[str, Cloud]):
        """
        Verifies that tests the for this course, cloud and version have passed and will prohibit progression if the tests have not passed
        :param cloud: One of the three values "AWS", "MSA" or "GCP"
        :return: None
        """
        from dbacademy import common

        assert self.validated, f"Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()"

        cloud = validate(cloud=cloud).required.enum(Cloud, auto_convert=True)
        self.__passing_tests[cloud.value] = True

        common.print_warning("NOT IMPLEMENTED", f"This function has not yet been implemented for {cloud}.")


def _print_build_config_deprecation_warning(*, _print_warning: bool) -> None:
    from dbacademy import common

    if _print_warning:
        common.print_title("DEPRECATION WARNING")
        print("The method BuildConfig.load(...) has been deprecated for the type-safe method load_build_config(..)"
              "which in-turn enables auto-completion hints from notebooks. Please update this script, replacing the"
              "old method with the New Method #1 or even better, New Method #2 which provides better documentation"
              "and readability than a JSON config file.")
        print()
        common.print_title("Old Method")
        print("""from dbacademy.dbbuild import BuildConfig""")
        print("""build_config = BuildConfig.load("_build-config.json", version="Test")""")
        print()
        common.print_title("New Method #1")
        print("""from dbacademy.dbbuild import load_build_config""")
        print("""build_config = load_build_config("_build-config.json", version="Test")""")
        print()
        common.print_title("New Method #2")
        print("""from dbacademy.dbbuild.build_config import BuildConfig""")
        print("""build_config = BuildConfig(name="Some Course", version="Test", ...)""")
        print("""build_config.include_solutions = False""")
        print("""build_config.i18n = True""")


def __load_from_dict(*, config: Dict[str, Any], name: str, value: ParameterType, expected_type: Type[ParameterType]) -> ParameterType:
    if value is not None and name in config:
        other = config.get(name)
        raise ValueError(f"""The BuildConfig parameter "{name}" was specified at runtime ({value}) and in the build-config file ({other}); one of the two references must be removed.""")

    if name not in config:
        return value
    else:
        value = config[name]
        del config[name]  # Delete the entry for later validation.
        return validate(parameter_name_override=name, value=value).as_type(expected_type)


def load_build_config(build_config_file_path: str, *, version: str,
                      name: str = None,
                      supported_dbrs: List[str] = None,
                      spark_version: str = None,
                      cloud: str = None,
                      instance_pool_id: str = None,
                      single_user_name: str = None,
                      workers: int = 0,
                      libraries: List[Dict[str, Any]] = None,
                      client: DBAcademyRestClient = None,
                      source_dir_name: str = None,
                      source_repo: str = None,
                      readme_file_name: str = None,
                      spark_conf: Dict[str, Any] = None,
                      job_arguments: Dict[str, Any] = None,
                      include_solutions: bool = None,
                      i18n: bool = None,
                      i18n_language: str = None,
                      ignoring: List[str] = None,
                      publishing_info: Dict[str, Any] = None,
                      white_list: Optional[List[str]] = None,
                      black_list: Optional[List[str]] = None,
                      notebook_configs: Dict[str, Any] = None,
                      _print_warning: bool = False) -> BuildConfig:
    """
    Creates an instance of BuildConfig by initializing values from the specified json file, build_config_file_path.
    WARNING: This method is deprecated as it relies on JSON config files which are hard to document and validate; Please use the BuildConfig(..) constructor instead.
    :param build_config_file_path: The path to the JSON file defining the build's configuration.
    :param version: see the BuildConfig's constructor parameter by the same name.
    :param name: see the BuildConfig's constructor parameter by the same name.
    :param supported_dbrs: see the BuildConfig's constructor parameter by the same name.
    :param spark_version: see the BuildConfig's constructor parameter by the same name.
    :param cloud: see the BuildConfig's constructor parameter by the same name.
    :param instance_pool_id: see the BuildConfig's constructor parameter by the same name.
    :param single_user_name: see the BuildConfig's constructor parameter by the same name.
    :param workers: see the BuildConfig's constructor parameter by the same name.
    :param libraries: see the BuildConfig's constructor parameter by the same name.
    :param client: see the BuildConfig's constructor parameter by the same name.
    :param source_dir_name: see the BuildConfig's constructor parameter by the same name.
    :param source_repo: see the BuildConfig's constructor parameter by the same name.
    :param readme_file_name: see the BuildConfig's constructor parameter by the same name.
    :param spark_conf: see the BuildConfig's constructor parameter by the same name.
    :param job_arguments: see the BuildConfig's constructor parameter by the same name.
    :param include_solutions: see the BuildConfig's constructor parameter by the same name.
    :param i18n: see the BuildConfig's constructor parameter by the same name.
    :param i18n_language: see the BuildConfig's constructor parameter by the same name.
    :param ignoring: see the BuildConfig's constructor parameter by the same name.
    :param publishing_info: see the BuildConfig's constructor parameter by the same name.
    :param white_list: see the BuildConfig's constructor parameter by the same name.
    :param black_list: see the BuildConfig's constructor parameter by the same name.
    :param notebook_configs: see the BuildConfig's initialize_notebooks() parameter by the same name.
    :param _print_warning: Used to disable printing of the deprecation warning.
    :return: BuildConfig
    """
    import json

    _print_build_config_deprecation_warning(_print_warning=_print_warning)

    validate(build_config_file_path=build_config_file_path).required.str()
    validate(version=version).required.str()

    with open(build_config_file_path) as f:
        config: Dict[str, Any] = json.load(f)

    if "publish_only" not in config:
        publish_only = dict()
    else:
        publish_only: Dict[str, List[str]] = config.get("publish_only")
        del config["publish_only"]  # Delete the entry for later validation.

    bc = BuildConfig(version=version,
                     client=client,
                     name=__load_from_dict(config=config,               name="name",                value=name, expected_type=str),
                     supported_dbrs=__load_from_dict(config=config,     name="supported_dbrs",      value=supported_dbrs, expected_type=List[str]),
                     spark_version=__load_from_dict(config=config,      name="spark_version",       value=spark_version, expected_type=int),
                     cloud=__load_from_dict(config=config,              name="cloud",               value=cloud, expected_type=str),
                     instance_pool_id=__load_from_dict(config=config,   name="instance_pool_id",    value=instance_pool_id, expected_type=str),
                     single_user_name=__load_from_dict(config=config,   name="single_user_name",    value=single_user_name, expected_type=str),
                     workers=__load_from_dict(config=config,            name="workers",             value=workers, expected_type=int),
                     libraries=__load_from_dict(config=config,          name="libraries",           value=libraries, expected_type=List[Dict[str, Any]]),
                     source_dir_name=__load_from_dict(config=config,    name="source_dir_name",     value=source_dir_name, expected_type=str),
                     source_repo=__load_from_dict(config=config,        name="source_repo",         value=source_repo, expected_type=str),
                     readme_file_name=__load_from_dict(config=config,   name="readme_file_name",    value=readme_file_name, expected_type=str),
                     spark_conf=__load_from_dict(config=config,         name="spark_conf",          value=spark_conf, expected_type=Dict[str, Any]),
                     job_arguments=__load_from_dict(config=config,      name="job_arguments",       value=job_arguments, expected_type=Dict[str, Any]),
                     include_solutions=__load_from_dict(config=config,  name="include_solutions",   value=include_solutions, expected_type=bool),
                     i18n=__load_from_dict(config=config,               name="i18n",                value=i18n, expected_type=bool),
                     i18n_language=__load_from_dict(config=config,      name="i18n_language",       value=i18n_language, expected_type=str),
                     ignoring=__load_from_dict(config=config,           name="ignoring",            value=ignoring, expected_type=List[str]),
                     publishing_info=__load_from_dict(config=config,    name="publishing_info",     value=publishing_info, expected_type=Dict[str, Any]),
                     # The following two are not stored in config's root, but rather nested under config.publish_only
                     white_list=__load_from_dict(config=publish_only,   name="white_list", value=white_list, expected_type=List[str]),
                     black_list=__load_from_dict(config=publish_only,   name="black_list", value=black_list, expected_type=List[str]))

    # Once the object is created, we need to initialize, or rather create, all the notebooks instances.
    notebook_configs: Dict[str, Any] =  __load_from_dict(config=config, name="notebook_config",    value=notebook_configs, expected_type=Dict[str, Any])
    bc.initialize_notebooks(notebook_configs)

    for key in config:  # The config dictionary should be empty at this point; any remaining entries are indicative of a bug.
        raise ValueError(f"""Invalid key "{key}" found in the build config file {build_config_file_path}.""")

    for key in publish_only:  # The config dictionary should be empty at this point; any remaining entries are indicative of a bug.
        raise ValueError(f"""Invalid key "publish_only.{key}" found in the build config file {build_config_file_path}.""")

    return bc
