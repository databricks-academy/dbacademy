__all__ = ["BuildConfig"]

from typing import Type, List, Dict, Union, Any, Optional, Callable
from dbacademy.clients.databricks import DBAcademyRestClient


class BuildConfig:

    LANGUAGE_OPTIONS_DEFAULT = "Default"

    VERSION_TEST = "Test"
    VERSION_BUILD = "Build"
    VERSION_TRANSLATION = "Translation"
    VERSIONS_LIST = [VERSION_BUILD, VERSION_TEST, VERSION_TRANSLATION]

    @staticmethod
    def load(file: str, *, version: str, **kwargs) -> Any:
        """
        Loads the configuration for this course from the specified JSON file.
        See also BuildConfig.VERSION_TEST
        See also BuildConfig.VERSION_BUILD
        See also BuildConfig.VERSION_TRANSLATION
        See also BuildConfig.VERSIONS_LIST
        :param file: The path to the JSON config file
        :param version: The current version being published. Expected to be one of BuildConfig.VERSIONS_LIST or an actual version number in the form of "vX.Y.Z"
        :return:
        """
        import json
        from dbacademy.common import validate

        validate.str_value(file=file)
        validate.str_value(version=version)

        with open(file) as f:
            return BuildConfig.load_config(config=json.load(f), version=version, **kwargs)

    @staticmethod
    def load_config(config: Dict[str, Any], version: str, **kwargs) -> Any:
        """
        Called by BuildConfig.load(), this method loads a build configuration from a dictionary
        :param config: The dictionary of configuration parameters
        :param version: The current version being published. Expected to be one of BuildConfig.VERSIONS_LIST or an actual version number in the form of "vX.Y.Z"
        :return:
        """
        from dbacademy.common import validate

        validate.dict_value(config=config)
        validate.str_value(version=version)

        if kwargs is not None:
            for k, v in kwargs.items():
                config[k] = v

        configurations = config.get("notebook_config", dict())
        if "notebook_config" in config:
            del config["notebook_config"]

        publish_only: Dict[str, List[str]] = config.get("publish_only", None)
        if "publish_only" in config:
            del config["publish_only"]

        build_config = BuildConfig(version=version, **config)
        build_config.__initialize_notebooks()

        def validate_code_type(key: str, expected_type: Type, actual_value: Any) -> Any:
            if expected_type == List[str]:
                assert type(actual_value) == list, f"Expected the value for \"{key}\" to be of type \"List[str]\", found \"{type(actual_value)}\"."
                for item in actual_value:
                    assert type(item) == str, f"Expected the elements of \"{key}\" to be of type \"str\", found \"{type(item)}\"."
            else:
                assert type(actual_value) == expected_type, f"Expected the value for \"{key}\" to be of type \"{expected_type}\", found \"{type(actual_value)}\"."

            return actual_value

        for name in configurations:
            assert name in build_config.notebooks, f"The notebook \"{name}\" doesn't exist."
            notebook = build_config.notebooks.get(name)
            notebook_config = configurations.get(name)

            param = "include_solution"
            if param in notebook_config:
                value = validate_code_type(param, bool, notebook_config.get(param))
                notebook.include_solution = value

            param = "test_round"
            if param in notebook_config:
                value = validate_code_type(param, int, notebook_config.get(param))
                notebook.test_round = value

            param = "ignored"
            if param in notebook_config:
                value = validate_code_type(param, bool, notebook_config.get(param))
                notebook.ignored = value

            param = "order"
            if param in notebook_config:
                value = validate_code_type(param, int, notebook_config.get(param))
                notebook.order = value

            param = "ignored_errors"
            if param in notebook_config:
                value = validate_code_type(param, List[str], notebook_config.get(param))
                notebook.ignoring = value

        if publish_only is not None:
            build_config.white_list = publish_only.get("white_list", None)
            assert build_config.white_list is not None, "The white_list must be specified when specifying publish_only"

            build_config.black_list = publish_only.get("black_list", None)
            assert build_config.black_list is not None, "The black_list must be specified when specifying publish_only"

        return build_config

    def __init__(self,
                 # TODO Fix all these types: List, Dict, Optional, etc
                 *,
                 name: str,
                 version: str = 0,
                 supported_dbrs: List[str] = None,
                 spark_version: str = None,
                 cloud: str = None,
                 instance_pool_id: str = None,
                 single_user_name: str = None,
                 workers: int = None,
                 libraries: List[Dict[str, Any]] = None,
                 client: DBAcademyRestClient = None,
                 source_dir: str = None,
                 source_repo: str = None,
                 readme_file_name: str = None,
                 spark_conf: Dict[str, Any] = None,
                 job_arguments: Dict[str, Any] = None,
                 include_solutions: bool = True,
                 i18n: bool = False,
                 i18n_language: str = None,
                 ignoring: List[str] = None,
                 publishing_info: Dict[str, Any] = None):

        import uuid
        import time
        from dbacademy.common import Cloud
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef
        from dbacademy.dbhelper.course_config import CourseConfig
        from dbacademy import dbgems
        from dbacademy.clients.rest.factory import dbrest_factory

        self.__validated = False
        self.__created_notebooks = False
        self.__passing_tests: Dict[str, bool] = dict()

        try:
            self.username = dbgems.sql("SELECT current_user()").first()[0]
        except:
            self.username = "mickey.mouse@disney.com"  # When unit testing

        self.supported_dbrs = supported_dbrs or []

        self.language_options = None
        self.ignoring = list() if ignoring is None else ignoring

        self.i18n = i18n
        self.i18n_language = i18n_language

        self.test_type = None
        self.notebooks: Union[None, Dict[str, NotebookDef]] = None
        self.__client = client or dbrest_factory.current_workspace()

        # The instance of this test run
        self.suite_id = str(time.time()) + "-" + str(uuid.uuid1())

        # The name of the cloud on which these tests were run
        self.cloud = Cloud.current_cloud().value if cloud is None else cloud

        # Course Name
        self.name = name
        assert self.name is not None, "The course's name must be specified."
        self.build_name = CourseConfig.to_build_name(name)

        # The Distribution's version
        assert version is not None, "The course's version must be specified."
        self.version = version
        self.core_version = version

        # The runtime you wish to test against - lazily evaluated via property
        self.__spark_version = spark_version

        # We can use local-mode clusters here
        self.workers = 0 if workers is None else workers

        self.__current_cluster = None
        self.__instance_pool_id = instance_pool_id      # The cluster's instance pool from which to obtain VMs - lazily evaluated via property
        self.__single_user_name = single_user_name      # The cluster's single-user name - lazily evaluated via property

        # Spark configuration parameters
        self.spark_conf = dict() if spark_conf is None else spark_conf
        if self.workers == 0:
            self.spark_conf["spark.master"] = "local[*]"

        # Test-Job's arguments
        self.job_arguments = dict() if job_arguments is None else job_arguments

        # The libraries to be attached to the cluster
        self.__libraries = libraries or list()

        self.source_repo = self.default_source_repo(source_repo)
        self.source_dir = self.default_source_dir(source_repo, source_dir)
        self.source_dir = source_dir or f"{self.source_repo}/Source"

        self.__readme_file_name = readme_file_name or "README.md"

        self.include_solutions = include_solutions

        self.white_list = None
        self.black_list = None

        self.change_log = None
        self.publishing_info = publishing_info or {}

    @property
    def libraries(self) -> List[Dict[str, Any]]:
        return self.__libraries

    @staticmethod
    def default_source_repo(source_repo: str = None) -> str:
        """
        Computes the default value for the source_repo.

        Refactored as a static method so that it can be called from notebooks ultimately overriding the default source_dir during the initialization of the BuildConfig.
        :param source_repo: Usually None, otherwise the path to the repo of the calling Notebook.
        :return: the path to the source repository
        """
        from dbacademy import dbgems
        return dbgems.get_notebook_dir(offset=-2) if source_repo is None else source_repo

    @staticmethod
    def default_source_dir(source_repo: str, source_dir: str = None) -> str:
        """
        Computes the default value for the source_dir given the current source_repo.

        Refactored as a static method so that it can be called from notebooks during the initialization of the BuildConfig.
        :param source_repo: The path to repo; see also default_source_repo()
        :param source_dir: Usually None, otherwise the directory name (not the full path) of the "Source" directory.
        :return: the path to the source directory
        """
        source_dir = source_dir or "Source"
        return f"{source_repo}/{source_dir}"

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    def __initialize_notebooks(self):
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

        self.__created_notebooks = True

        assert self.source_dir is not None, "BuildConfig.source_dir must be specified"

        self.notebooks = dict()
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

            if "includes/" in path.lower():  # Any folder that ends in "includes/"
                test_round = 0  # Never test notebooks in the "includes" folders

            if path.lower() == "includes/reset":
                order = 0                 # Reset needs to run first.
                test_round = 1            # Add to test_round #1
                include_solution = False  # Exclude from the solutions folder

            if path.lower() == "includes/workspace-setup":
                order = 1                 # Reset needs to run first.
                test_round = 1            # Add to test_round #1
                include_solution = False  # Exclude from the solutions folder

            if "wip" in path.lower():
                has_wip = True
                print(f"""** WARNING ** The notebook "{path}" is excluded from the build as a work in progress (WIP)""")
            else:
                replacements = {"supported_dbrs": ", ".join(self.supported_dbrs)}

                # Add our notebook to the set of notebooks to be tested.
                self.notebooks[path] = NotebookDef(build_config=self,
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

    @staticmethod
    def get_lesson_number(notebook_path: str):
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
        from dbacademy.dbbuild.change_log_class import ChangeLog

        if self.version in BuildConfig.VERSIONS_LIST:
            return  # Implies we have an actual version of the form N.N.N
        elif self.i18n_language is not None:
            return  # We are building a translation, presumably days to weeks later, this is not expected to match

        self.change_log = ChangeLog(source_repo=self.source_repo,
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
    def to_translator(self, require_language: bool = True):
        """
        Creates an instance of Translator from the current build configuration.
        :return:
        """
        publisher = self.to_publisher(publishing_mode=None)
        publisher.validate(silent=True)
        return publisher.to_translator(require_language)

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

    def validate_all_tests_passed(self, cloud: str):
        """
        Verifies that tests the for this course, cloud and version have passed and will prohibit progression if the tests have not passed
        :param cloud: One of the three values "AWS", "MSA" or "GCP"
        :return: None
        """
        from dbacademy import common
        from dbacademy.common import validate

        cloud = validate.str_value(cloud=cloud).upper()

        assert self.validated, f"Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()"

        self.__passing_tests[cloud] = True
        common.print_warning("NOT IMPLEMENTED", f"This function has not yet been implemented for {cloud}.")
