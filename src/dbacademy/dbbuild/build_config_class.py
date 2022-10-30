from typing import Type, List, Dict, Union, Any
from dbacademy import dbgems, common

try:
    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
    __build_config_defined

except NameError:
    __build_config_defined = True

    class BuildConfig:
        from dbacademy.dbbuild.publish.translator_class import Translator
        from dbacademy.dbbuild.publish.publisher_class import Publisher
        from dbacademy.dbbuild.publish.resource_diff_class import ResourceDiff
        from dbacademy.dbbuild.test.test_suite_class import TestSuite

        LANGUAGE_OPTIONS_DEFAULT = "Default"

        VERSION_TEST = "Test"
        VERSION_BUILD = "Build"
        VERSION_TRANSLATION = "Translation"
        VERSIONS_LIST = [VERSION_BUILD, VERSION_TEST, VERSION_TRANSLATION]

        @staticmethod
        def load(file: str, *, version: str) -> Any:
            import json

            common.validate_type(file, "file", str)
            common.validate_type(version, "version", str)

            with open(file) as f:
                return BuildConfig.load_config(config=json.load(f), version=version)

        @staticmethod
        def load_config(config: dict, version: str) -> Any:

            assert type(config) == dict, f"Expected the parameter \"config\" to be of type dict, found {config}."
            assert type(version) == str, f"Expected the parameter \"version\" to be of type str, found {version}."

            configurations = config.get("notebook_config", dict())
            if "notebook_config" in config: del config["notebook_config"]

            publish_only = config.get("publish_only", None)
            if "publish_only" in config: del config["publish_only"]

            build_config = BuildConfig(version=version, **config)

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
                     instance_pool: str = None,
                     workers: int = None,
                     libraries: list = None,
                     client=None,
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

            import uuid, time, re
            from dbacademy.dbrest import DBAcademyRestClient
            from .publish.notebook_def_class import NotebookDef

            self.__validated = False

            try: self.username = dbgems.sql("SELECT current_user()").first()[0]
            except: self.username = "mickey.mouse@disney.com"  # When unit testing

            self.supported_dbrs = supported_dbrs or []

            self.language_options = None
            self.ignoring = [] if ignoring is None else ignoring

            self.i18n = i18n
            self.i18n_xml_tag_disabled = i18n_xml_tag_disabled
            self.i18n_language = i18n_language

            self.test_type = None
            self.notebooks: Union[None, Dict[str, NotebookDef]] = None
            self.client = DBAcademyRestClient.default_client if client is None else client

            # The instance of this test run
            self.suite_id = str(time.time()) + "-" + str(uuid.uuid1())

            # The name of the cloud on which these tests were run
            self.cloud = dbgems.get_cloud() if cloud is None else cloud

            # Course Name
            self.name = name
            assert self.name is not None, "The course's name must be specified."
            self.build_name = re.sub(r"[^a-zA-Z\d]", "-", name).lower()

            # The Distribution's version
            assert version is not None, "The course's version must be specified."
            self.version = version
            self.core_version = version

            # The runtime you wish to test against
            self.spark_version = self.client.clusters().get_current_spark_version() if spark_version is None else spark_version

            # We can use local-mode clusters here
            self.workers = 0 if workers is None else workers

            # The instance pool from which to obtain VMs
            self.instance_pool = self.client.clusters().get_current_instance_pool_id() if instance_pool is None else instance_pool

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

            # We don't want the following function to fail if we are using the "default" path which
            # may or may not exist. The implication being that this will fail if called explicitly
            self.include_solutions = include_solutions
            self.create_notebooks(include_solutions=include_solutions,
                                  fail_fast=source_dir is not None)

            self.white_list = None
            self.black_list = None

            self.change_log = None
            self.publishing_info = publishing_info or {}

        # def get_distribution_name(self, version):
        #     distribution_name = f"{self.name}" if version is None else f"{self.name}-v{version}"
        #     return distribution_name.replace(" ", "-").replace(" ", "-").replace(" ", "-")

        def create_notebooks(self, *, include_solutions: bool, fail_fast: bool):
            from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

            assert self.source_dir is not None, "BuildConfig.source_dir must be specified"

            self.notebooks = dict()
            entities = self.client.workspace().ls(self.source_dir, recursive=True)

            if entities is None and fail_fast is False:
                return  # The directory doesn't exist
            elif entities is None and fail_fast is True:
                raise Exception(f"The specified directory ({self.source_dir}) does not exist (fail_fast={fail_fast}).")

            entities.sort(key=lambda e: e["path"])

            has_wip = False
            for i in range(len(entities)):
                entity = entities[i]
                order = i       # Start with the natural order
                test_round = 2  # Default test_round for all notebooks
                include_solution = include_solutions  # Initialize to the default value
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
            if has_wip: print()

        def validate(self, validate_version: bool = True, validate_readme: bool = True):

            if validate_version: self.__validate_version()
            if validate_readme: self.__validate_readme()

            print("Build Configuration:")
            print(f"| suite_id:          {self.suite_id}")
            print(f"| name:              {self.name}")
            print(f"| version:           {self.version}")
            print(f"| spark_version:     {self.spark_version}")
            print(f"| workers:           {self.workers}")
            print(f"| instance_pool:     {self.instance_pool}")
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
        def validated(self) -> bool:
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
            for path in self.notebooks: max_name_length = len(path) if len(path) > max_name_length else max_name_length

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
                        if "required_dbrs" in replacements: del replacements["required_dbrs"]

                        replacements_copy = replacements.copy()
                        if "supported_dbrs" in replacements_copy: del replacements_copy["supported_dbrs"]

                        if len(replacements_copy) == 0:
                            print(f"{notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements=None")
                        else:
                            print(f"{notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements={{")
                            max_key_length = 0
                            for key in replacements_copy: max_key_length = len(key) if len(key) > max_key_length else max_key_length

                            for key in replacements_copy:
                                value = replacements_copy.get(key)
                                print(f"    {key}", end="")
                                print(f" " * (max_key_length - len(key)), end="")
                                print(f": {value}")
                            print("|   }")

        # Used by notebooks
        def to_resource_diff(self) -> ResourceDiff:
            from dbacademy.dbbuild.publish.resource_diff_class import ResourceDiff
            assert self.validated, f"Cannot diff until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed."

            return ResourceDiff(self)

        # Used by notebooks
        def to_publisher(self) -> Publisher:
            from dbacademy.dbbuild.publish.publisher_class import Publisher
            assert self.validated, f"Cannot publish until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

            return Publisher(self)

        # Used by notebooks
        def to_translator(self) -> Translator:
            publisher = self.to_publisher()
            publisher.validate(silent=True)
            return publisher.to_translator()

        # Used by notebooks
        def to_test_suite(self, test_type: str = None, keep_success: bool = False) -> TestSuite:
            from dbacademy.dbbuild.test.test_suite_class import TestSuite

            assert self.validated, f"Cannot test until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

            return TestSuite(build_config=self,
                             test_dir=self.source_dir,
                             test_type=test_type,
                             keep_success=keep_success)
