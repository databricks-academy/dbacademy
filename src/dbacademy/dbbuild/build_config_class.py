from typing import Type, List, Dict, Union, Any, Optional
from dbacademy import dbgems, common
from dbacademy.rest.factory import dbrest_factory


class BuildConfig:
    from dbacademy.dbrest import DBAcademyRestClient

    LANGUAGE_OPTIONS_DEFAULT = "Default"

    VERSION_TEST = "Test"
    VERSION_BUILD = "Build"
    VERSION_TRANSLATION = "Translation"
    VERSIONS_LIST = [VERSION_BUILD, VERSION_TEST, VERSION_TRANSLATION]

    @staticmethod
    def load(file: str, *, version: str) -> Any:
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

        common.validate_type(file, "file", str)
        common.validate_type(version, "version", str)

        with open(file) as f:
            return BuildConfig.load_config(config=json.load(f), version=version)

    @staticmethod
    def load_config(config: Dict[str, Any], version: str) -> Any:
        """
        Called by BuildConfig.load(), this method loads a build configuration from a dictionary
        :param config: The dictionary of configuration parameters
        :param version: The current version being published. Expected to be one of BuildConfig.VERSIONS_LIST or an actual version number in the form of "vX.Y.Z"
        :return:
        """

        common.validate_type(config, "config", Dict)
        common.validate_type(version, "version", str)

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
                 *,
                 name: str,
                 version: str = 0,
                 supported_dbrs: List[str] = None,
                 spark_version: str = None,
                 cloud: str = None,
                 instance_pool_id: str = None,
                 workers: int = None,
                 libraries: list = None,
                 client: DBAcademyRestClient = None,
                 source_dir: str = None,
                 source_repo: str = None,
                 spark_conf: dict = None,
                 job_arguments: dict = None,
                 include_solutions: bool = True,
                 i18n: bool = False,
                 i18n_language: str = None,
                 i18n_xml_tag_disabled: bool = False,
                 ignoring: list = None,
                 publishing_info: dict = None):

        import uuid, time
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef
        from dbacademy.dbhelper.course_config_class import CourseConfig

        self.__validated = False
        self.__created_notebooks = False
        self.__passing_tests: Dict[str, bool] = dict()

        try:
            self.username = dbgems.sql("SELECT current_user()").first()[0]
        except:
            self.username = "mickey.mouse@disney.com"  # When unit testing

        self.supported_dbrs = supported_dbrs or []

        self.language_options = None
        self.ignoring = [] if ignoring is None else ignoring

        self.i18n = i18n
        self.i18n_xml_tag_disabled = i18n_xml_tag_disabled
        self.i18n_language = i18n_language

        self.test_type = None
        self.notebooks: Union[None, Dict[str, NotebookDef]] = None
        self.__client = client or dbrest_factory.current_workspace()

        # The instance of this test run
        self.suite_id = str(time.time()) + "-" + str(uuid.uuid1())

        # The name of the cloud on which these tests were run
        self.cloud = dbgems.get_cloud() if cloud is None else cloud

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

        # The instance pool from which to obtain VMs - lazily evaluated via property
        self.__instance_pool_id = instance_pool_id

        # Spark configuration parameters
        self.spark_conf = dict() if spark_conf is None else spark_conf
        if self.workers == 0:
            self.spark_conf["spark.master"] = "local[*]"

        # Test-Job's arguments
        self.job_arguments = dict() if job_arguments is None else job_arguments

        # The libraries to be attached to the cluster
        self.libraries = [] if libraries is None else libraries

        self.source_repo = dbgems.get_notebook_dir(offset=-2) if source_repo is None else source_repo
        self.source_dir = f"{self.source_repo}/Source" if source_dir is None else source_dir
        self.source_dir = source_dir or f"{self.source_repo}/Source"

        self.include_solutions = include_solutions

        self.white_list = None
        self.black_list = None

        self.change_log = None
        self.publishing_info = publishing_info or {}

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

    def validate(self, validate_version: bool = True, validate_readme: bool = True):
        """
        Asserts that the build configuration is valid. Upon validating, prints the build parameters
        :param validate_version: Flag to disable validation of the version number
        :param validate_readme: Flag to disable validation of the README file
        :return:
        """

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
        print(f"| libraries:         {self.libraries}")
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
    def instance_pool_id(self) -> str:
        """
        The instance pool to use for testing.
        :return: The instance pool id
        """
        if self.__instance_pool_id is None:  # This may have not been specified upon instantiation
            self.__instance_pool_id = self.client.clusters().get_current_instance_pool_id()
            assert self.__instance_pool_id is not None, f"The current cluster is not configured to use an instance pool which is required for execution of smoke-tests"

        return self.__instance_pool_id

    @property
    def spark_version(self) -> str:
        """
        The spark version to use for testing; defaults to the current cluster's spark-version
        :return: The spark version
        """
        if self.__spark_version is None:  # This may have not been specified upon instantiation
            self.__spark_version = self.client.clusters().get_current_spark_version()

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

        self.change_log = ChangeLog(source_repo=self.source_repo, target_version=None)
        self.change_log.validate(expected_version=self.core_version, date=None)

    def __validate_version(self):
        if self.version not in BuildConfig.VERSIONS_LIST:
            msg = f"The version parameter must be one of {BuildConfig.VERSIONS_LIST} or of the form \"N.N.N\" or \"N.N.N-AA\" where \"N\" is an integral value and \"A\" a two-character language code, found \"{self.version}\"."
            self.version.split(".")

            assert len(self.version.split(".")) == 3, msg
            major, minor, bug = self.version.split(".")
            bug, lang = bug.split("-") if "-" in bug else (bug, "NA")

            assert major.isnumeric(), msg
            assert minor.isnumeric(), msg
            assert bug.isnumeric(), msg
            assert len(lang) == 2 and lang.upper() == lang, msg

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
    def to_translator(self):
        """
        Creates an instance of Translator from the current build configuration.
        :return:
        """
        publisher = self.to_publisher(publishing_mode=None)
        publisher.validate(silent=True)
        return publisher.to_translator()

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
        cloud = common.validate_type(cloud, "cloud", str).upper()

        assert self.validated, f"Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()"

        self.__passing_tests[cloud] = True
        dbgems.print_warning("NOT IMPLEMENTED", f"This function has not yet been implemented for {cloud}.")
