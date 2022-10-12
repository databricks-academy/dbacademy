from typing import Type, List, Dict, Union
from dbacademy import dbgems
from .build_utils_class import BuildUtils


class BuildConfig:

    LANGUAGE_OPTIONS_DEFAULT = "Default"

    VERSION_TEST = "Test"
    VERSION_BUILD = "Build"
    VERSION_TRANSLATION = "Translation"
    VERSIONS_LIST = [VERSION_BUILD, VERSION_TEST, VERSION_TRANSLATION]

    CHANGE_LOG_TAG = "## Change Log"
    CHANGE_LOG_VERSION = "### Version "

    @staticmethod
    def load(file: str, *, version: str):
        import json

        BuildUtils.validate_type(file, "file", str)
        BuildUtils.validate_type(version, "version", str)

        with open(file) as f:
            return BuildConfig.load_config(config=json.load(f), version=version)

    @staticmethod
    def load_config(config: dict, version: str):

        assert type(config) == dict, f"Expected the parameter \"config\" to be of type dict, found {config}."
        assert type(version) == str, f"Expected the parameter \"version\" to be of type str, found {version}."

        configurations = config.get("notebook_config", dict())
        if "notebook_config" in config: del config["notebook_config"]

        publish_only = config.get("publish_only", None)
        if "publish_only" in config: del config["publish_only"]

        build_config = BuildConfig(version=version, **config)

        def validate_code_type(key: str, expected_type: Type, actual_value):
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
                 required_dbrs: List[str] = None,
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

        self.required_dbrs = required_dbrs or []

        self.language_options = None
        self.ignoring = [] if ignoring is None else ignoring

        self.i18n = i18n
        self.i18n_xml_tag_disabled = i18n_xml_tag_disabled
        self.i18n_language = i18n_language

        self.test_type = None
        self.notebooks: Union[None, Dict[str, NotebookDef]] = None
        self.client = DBAcademyRestClient() if client is None else client

        # The instance of this test run
        self.suite_id = str(time.time()) + "-" + str(uuid.uuid1())

        # The name of the cloud on which this tests was ran
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

        self.change_log = []
        self.publishing_info = publishing_info or {}

    # def get_distribution_name(self, version):
    #     distribution_name = f"{self.name}" if version is None else f"{self.name}-v{version}"
    #     return distribution_name.replace(" ", "-").replace(" ", "-").replace(" ", "-")

    def create_notebooks(self, *, include_solutions: bool, fail_fast: bool):
        from dbacademy.dbbuild import NotebookDef

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
            path = entity["path"][len(self.source_dir) + 1:]  # Get the notebook's path relative too the source root

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
                replacements = {"required_dbrs": ", ".join(self.required_dbrs)}

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

        if validate_version: self._validate_version()
        if validate_readme: self._validate_readme()

        print("Build Configuration")
        print(f"suite_id:          {self.suite_id}")
        print(f"name:              {self.name}")
        print(f"version:           {self.version}")
        print(f"spark_version:     {self.spark_version}")
        print(f"workers:           {self.workers}")
        print(f"instance_pool:     {self.instance_pool}")
        print(f"spark_conf:        {self.spark_conf}")
        print(f"cloud:             {self.cloud}")
        print(f"libraries:         {self.libraries}")
        print(f"source_repo:       {self.source_repo}")
        print(f"source_dir:        {self.source_dir}")
        print(f"required_dbrs:     " + ", ".join(self.required_dbrs))
        print(f"i18n:              {self.i18n}")
        print(f"i18n_language:     " + (self.i18n_language if self.i18n_language else "None (English)"))

        if len(self.notebooks) == 0:
            print(f"notebooks:         none")
        else:
            print(f"notebooks:         {len(self.notebooks)}")
            self._index_notebooks()

        self.__validated = True

    @property
    def validated(self) -> bool:
        return self.__validated

    def _validate_readme(self):
        import os
        from datetime import datetime

        if self.version in BuildConfig.VERSIONS_LIST:
            return  # Implies we have an actual version of the form N.N.N
        elif self.i18n_language is not None:
            return  # We are building a translation, presumably days to weeks later, this is not expected to match

        readme_path = f"/Workspace/{self.source_repo}/README.md"
        assert os.path.exists(readme_path), f"The README.md file was not found at {readme_path}"

        with open(readme_path, "r") as f:
            lines = f.readlines()

        change_log_index: Union[None, int] = None
        version_index: Union[None, int] = None

        for i, line in enumerate(lines):
            line = line.strip()
            if line == BuildConfig.CHANGE_LOG_TAG:
                change_log_index = i
            elif change_log_index and i > change_log_index and line == "":
                pass  # Just an empty line
            elif change_log_index and i > change_log_index and not version_index:

                version_index = i
                assert line.startswith(BuildConfig.CHANGE_LOG_VERSION), f"The next change log entry ({BuildConfig.CHANGE_LOG_VERSION}...) was not found at {readme_path}:{i + 1}\n{line}"

                parts = line.split(" ")  # "### Version 1.0.2 (01-21-2022)"
                assert len(parts) == 4, f"Expected the change log entry to contain 4 parts and of the form \"### Version vN.N.N (M-D-YYYY)\", found \"{line}\"."
                assert parts[0] == "###", f"Part 1 of the change long entry is not \"###\", found \"{parts[0]}\""
                assert parts[1] == "Version", f"Part 2 of the change long entry is not \"Version\", found \"{parts[1]}\""

                version = parts[2]
                v_parts = version.split(".")
                assert len(v_parts) == 3, f"The change long entry's version field is not of the form \"vN.N.N\" where \"N\" is an integral value, found {len(v_parts)} parts: \"{version}\"."
                assert v_parts[0].isnumeric(), f"The change long entry's Major version field is not an integral value, found \"{version}\"."
                assert v_parts[1].isnumeric(), f"The change long entry's Minor version field is not an integral value, found \"{version}\"."
                assert v_parts[2].isnumeric(), f"The change long entry's Bug-Fix version field is not an integral value, found \"{version}\"."

                assert version == self.core_version, f"The change log entry's version is not \"{self.core_version}\", found \"{version}\"."

                date = parts[3]
                assert date.startswith("(") and date.endswith(")"), f"Expected the change log entry's date field to be of the form \"(M-D-YYYY)\", found \"{date}\"."

                date = date[1:-1]
                d_parts = date.split("-")
                assert len(d_parts) == 3, f"The change long entry's date field is not of the form \"(M-D-YYYY)\", found {date}\"."
                assert d_parts[0].isnumeric(), f"The change long entry's month field is not an integral value, found \"{date}\"."
                assert d_parts[1].isnumeric(), f"The change long entry's day field is not an integral value, found \"{date}\"."
                assert d_parts[2].isnumeric(), f"The change long entry's year field is not an integral value, found \"{date}\"."

                current_date = datetime.today().strftime("%-m-%-d-%Y")
                assert date == f"{current_date}", f"The change log entry's date is not \"{current_date}\", found \"{date}\"."

            elif version_index and i > version_index and not line.startswith("#"):
                self.change_log.append(line)

            elif version_index and i > version_index and line.startswith("#"):
                print("\nChange Log:")
                for entry in self.change_log:
                    print(f"  {entry}")
                return

        assert len(self.change_log) > 0, f"The Change Log section was not found in {readme_path}"

    def _validate_version(self):
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

    def _index_notebooks(self):
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

                    if len(replacements.keys()) == 0:
                        print(f"  {notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements={replacements}")
                    else:
                        print(f"  {notebook.order: >3}: {path}   ignored={ignored}   include_solution={include_solution}   replacements={{")
                        max_key_length = 0
                        for key in replacements: max_key_length = len(key) if len(key) > max_key_length else max_key_length

                        for key in replacements:
                            value = replacements[key]
                            print(f"        {key}", end="")
                            print(" " * (max_key_length - len(key)), end="")
                            print(f": {value}")
                        print("      }")

    def to_resource_diff(self):
        from dbacademy.dbbuild import ResourceDiff
        assert self.validated, f"Cannot diff until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed."

        return ResourceDiff(self)

    def to_translator(self):
        from dbacademy.dbbuild import Translator
        assert self.validated, f"Cannot translate until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

        return Translator(self)

    def to_publisher(self):
        from dbacademy.dbbuild import Publisher
        assert self.validated, f"Cannot publish until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

        return Publisher(self)

    def to_test_suite(self, test_type: str = None, keep_success: bool = False):
        from dbacademy.dbbuild import TestSuite

        assert self.validated, f"Cannot test until the build configuration passes validation. Ensure that BuildConfig.validate() was called and that all assignments passed"

        return TestSuite(build_config=self,
                         test_dir=self.source_dir,
                         test_type=test_type,
                         keep_success=keep_success)

    @dbgems.deprecated(reason="Corresponding logic has been moved to the class Translator and its related capabilities")
    def select_i18n_language(self):
        resources_folder = f"{self.source_repo}/Resources"

        resources = self.client.workspace().ls(resources_folder)
        self.language_options = [r.get("path").split("/")[-1] for r in resources]
        self.language_options.sort()
        self.language_options.insert(0, BuildConfig.LANGUAGE_OPTIONS_DEFAULT)

        dbgems.dbutils.widgets.dropdown("i18n_language",
                                        BuildConfig.LANGUAGE_OPTIONS_DEFAULT,
                                        self.language_options,
                                        "i18n Language")

        self.i18n_language = dbgems.dbutils.widgets.get("i18n_language")
        self.i18n_language = None if self.i18n_language == BuildConfig.LANGUAGE_OPTIONS_DEFAULT else self.i18n_language

        assert self.i18n_language is None or self.i18n_language.endswith(self.version), f"The build version ({self.version}) and the selected language ({self.i18n_language}) do not correspond to each other."

        for notebook in self.notebooks.values():
            notebook.i18n_language = self.i18n_language

        if self.i18n_language is not None:
            # Include the i18n code in the version.
            # This hack just happens to work for japanese and korean
            code = self.i18n_language[0:2].upper()
            self.version = f"{self.version}-{code}"
            self.core_version = self.version if "-" not in self.version else self.version.split("-")[0]
