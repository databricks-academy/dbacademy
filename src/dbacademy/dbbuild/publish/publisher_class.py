from typing import List, Optional


class Publisher:
    from dbacademy.dbbuild.build_config_class import BuildConfig
    from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

    VERSION_INFO_NOTEBOOK = "Version Info"

    PUBLISHING_MODE_MANUAL = "manual"
    PUBLISHING_MODE_AUTOMATIC = "automatic"
    PUBLISHING_MODES = [PUBLISHING_MODE_MANUAL, PUBLISHING_MODE_AUTOMATIC]

    KEEPERS = [".gitignore", "README.md", "LICENSE", "docs"]

    def __init__(self, build_config: BuildConfig, publishing_mode: Optional[str]):
        from dbacademy import common
        from dbacademy.dbbuild.build_config_class import BuildConfig

        # Various validation steps
        self.__validated = False
        self.__validated_repo_reset = True
        self.__changes_in_source_repo = None
        self.__generated_notebooks = False
        self.__changes_in_target_repo = None
        self.__created_docs = False
        self.__created_dbcs = False
        self.__validated_artifacts = False
        self.__publishing_mode = None

        self.build_config = common.validate_type(build_config, "build_config", BuildConfig)

        self.client = build_config.client
        self.version = build_config.version
        self.core_version = build_config.core_version
        self.build_name = build_config.build_name

        self.source_dir = build_config.source_dir
        self.source_repo = build_config.source_repo

        self.target_dir = f"{self.source_repo}/Published/{self.build_config.name} - v{self.build_config.version}"
        self.target_repo_url = None

        self.temp_repo_dir = f"/Repos/Temp"
        self.temp_work_dir = f"/Workspace/Users/{build_config.username}/Temp"
        self.username = build_config.username

        self.i18n = build_config.i18n
        self.i18n_resources_dir = f"{self.source_repo}/Resources/{build_config.i18n_language}"
        self.i18n_language = build_config.i18n_language

        self.publishing_mode = publishing_mode

        if build_config.i18n_language is None:
            self.common_language = "english"
        else:
            # Include the i18n code in the version.
            # This hack just happens to work for japanese and korean
            self.common_language = build_config.i18n_language.split("-")[0]

        self.notebooks = []
        self.__init_notebooks(build_config.notebooks.values())

        self.white_list = build_config.white_list
        self.black_list = build_config.black_list
        self.__validate_white_black_list()

    @property
    def publishing_mode(self) -> Optional[str]:
        """
        Indicates which mode the publisher is operating under. Expected values include None or one of Publisher.PUBLISHING_MODES. When testing
        the mode is expected to be None. When actually publishing a specific version of a course "manual" (Publisher.PUBLISHING_MODE_MANUAL) indicates
        that the DBC will be exported and published manually where "automatic" (Publisher.PUBLISHING_MODE_AUTOMATIC) indicates that the DBC,
        slides, etc. will be published to the distribution system via the build scripts.
        :return: the current publishing mode
        """
        return self.__publishing_mode

    @publishing_mode.setter
    def publishing_mode(self, publishing_mode: Optional[str]) -> None:
        from dbacademy.dbbuild import BuildConfig

        if self.version in BuildConfig.VERSIONS_LIST:
            # Building, Testing or Translating
            assert publishing_mode is None, f"Expected the parameter \"publishing_mode\" to be None when the version is one of {BuildConfig.VERSIONS_LIST}, found \"{self.version}\""
        else:
            assert publishing_mode in Publisher.PUBLISHING_MODES, f"Expected the parameter \"publishing_mode\" to be one of {Publisher.PUBLISHING_MODES}, found \"{publishing_mode}\""

        self.__publishing_mode = publishing_mode

    def __init_notebooks(self, notebooks) -> None:
        from datetime import datetime
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

        for notebook in notebooks:
            assert type(notebook) == NotebookDef, f"Expected the parameter \"notebook\" to be of type \"NotebookDef\", found \"{type(notebook)}\"."

            # Update the built_on and version_number - typically only found in the Version Info notebook.
            notebook.replacements["course_name"] = self.build_config.name
            notebook.replacements["version_number"] = self.version
            notebook.replacements["built_on"] = datetime.now().strftime("%b %-d, %Y at %H:%M:%S UTC")

            self.notebooks.append(notebook)

    def __validate_white_black_list(self) -> None:
        if self.white_list or self.black_list:
            assert self.white_list is not None, "The white_list must be specified when specifying a black_list"
            assert self.black_list is not None, "The black_list must be specified when specifying a white_list"

            notebook_paths = [n.path for n in self.notebooks]

            # Validate white and black lists
            for path in self.white_list:
                assert path not in self.black_list, f"The white-list path \"{path}\" was also found in the black-list."
                assert path in notebook_paths, f"The white-list path \"{path}\" does not exist in the complete set of notebooks.\n{notebook_paths}"

            for path in self.black_list:
                assert path not in self.white_list, f"The black-list path \"{path}\" was also found in the white-list."
                assert path in notebook_paths, f"The black-list path \"{path}\" does not exist in the complete set of notebooks.\n{notebook_paths}"

            for path in notebook_paths:
                assert path in self.white_list or path in self.black_list, f"The notebook \"{path}\" was not found in either the white-list or black-list."

    def create_resource_bundle(self, folder_name: str = None, target_dir: str = None) -> str:
        """
        Only applicable to translated courses, this method creates a "resource bundle" consisting of MD files of the notebook's various MD cells
        :param folder_name: The name of the folder for the resource bundle as a subfolder of target_dir
        :param target_dir: The directory under which resource bundles are located.
        :return: The HTML results that should be rendered with displayHTML() from the calling notebook
        """

        assert self.i18n_language is None, f"Resource bundles are created for the English translations only, found {self.i18n_language}"

        folder_name = folder_name or f"english-v{self.build_config.version}"
        target_dir = target_dir or f"{self.source_repo}/Resources"

        for notebook in self.notebooks:
            notebook.create_resource_bundle(folder_name, self.source_dir, target_dir)

        return f"""<html><body style="font-size:16px">
            <p><a href="/#workspace{target_dir}/{folder_name}/{Publisher.VERSION_INFO_NOTEBOOK}.md" target="_blank">Resource Bundle: {folder_name}</a></p>
        </body></html>"""

    def assert_notebooks_generated(self) -> None:
        """
        Asserts that the notebooks have been generated - used to control program flow.
        :return: None
        """
        assert self.__generated_notebooks, "The notebooks have not yet been generated. See Publisher.generate_notebooks()"

    def generate_notebooks(self, *, skip_generation: bool = False, verbose=False, debugging=False) -> Optional[str]:
        """
        Generates the publishable notebooks from the source notebooks
        :param skip_generation: Overrides the default behavior and skips generation of the notebook
        :param verbose: True of verbose logging
        :param debugging: True for debug logging
        :return: The HTML results that should be rendered with displayHTML() from the calling notebook
        """
        from dbacademy import common, dbgems
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef
        from dbacademy.dbbuild.build_utils_class import BuildUtils
        from dbacademy.dbbuild import BuildConfig

        if self.version in BuildConfig.VERSIONS_LIST:
            self.assert_validated_config()
        else:
            self.assert_no_changes_in_source_repo()

        if self.publishing_mode == Publisher.PUBLISHING_MODE_MANUAL:
            # This is a manual publish so target repo will be empty
            self.__changes_in_target_repo = 0

        if skip_generation:
            self.__generated_notebooks = True
            common.print_warning(f"SKIPPING GENERATION", "Skipping the generation of notebooks")
            return None

        found_version_info = False
        main_notebooks: List[NotebookDef] = []

        for notebook in self.notebooks:
            if self.black_list is None or notebook.path not in self.black_list:
                found_version_info = True if notebook.path == Publisher.VERSION_INFO_NOTEBOOK else found_version_info
                main_notebooks.append(notebook)

        assert found_version_info, f"The required notebook \"{Publisher.VERSION_INFO_NOTEBOOK}\" was not found."

        print(f"Source: {self.source_dir}")
        print(f"Target: {self.target_dir}")
        print()
        print("Arguments:")
        print(f"  verbose =   {verbose}")
        print(f"  debugging = {debugging}")

        if self.black_list is None:
            print(f"  exclude:    none")
        else:
            self.black_list.sort()
            print(f"\n  exclude:    {self.black_list[0]}")
            for path in self.black_list[1:]:
                print(f"              {path}")

        if self.white_list is None:
            print(f"  include:    none")
        else:
            self.white_list.sort()
            print(f"\n  include:    {self.white_list[0]}")
            for path in self.white_list[1:]:
                print(f"              {path}")

        # Now that we backed up the version-info, we can delete everything.
        target_status = self.client.workspace().get_status(self.target_dir)
        if target_status is not None:
            BuildUtils.print_if(verbose, "-" * 80)
            BuildUtils.clean_target_dir(self.client, self.target_dir, verbose)

        errors = 0
        warnings = 0

        for notebook in main_notebooks:
            notebook.publish(source_dir=self.source_dir,
                             target_dir=self.target_dir,
                             i18n_resources_dir=self.i18n_resources_dir,
                             verbose=verbose, 
                             debugging=debugging,
                             other_notebooks=self.notebooks)
            errors += len(notebook.errors)
            warnings += len(notebook.warnings)

        print("-"*80)
        print(f"All done!")
        print()
        print(f"Found {warnings} warnings")
        print(f"Found {errors} errors")

        html = f"""<html><body style="font-size:16px">
                         <div><a href="{dbgems.get_workspace_url()}#workspace{self.target_dir}/{Publisher.VERSION_INFO_NOTEBOOK}" target="_blank">See Published Version</a></div>"""
        for notebook in main_notebooks:
            errors += len(notebook.errors)
            warnings += len(notebook.warnings)

            if len(notebook.warnings) > 0:
                html += f"""<div style="font-weight:bold; margin-top:1em">{notebook.path}</div>"""
                for warning in notebook.warnings:
                    html += f"""<div style="margin-top:1em; white-space: pre-wrap">{warning.message}</div>"""
        html += """</body></html>"""

        self.__generated_notebooks = True
        return html

    def create_published_message(self) -> str:
        """
        Convenience method to aid in creating the publishing email and Slack message.
        :return: The HTML results that should be rendered with displayHTML() from the calling notebook
        """
        from dbacademy.dbbuild.publish.advertiser import Advertiser
        from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo

        self.assert_validate_artifacts()

        advertiser = Advertiser(source_repo=self.source_repo,
                                name=self.build_config.name,
                                version=self.build_config.version,
                                change_log=self.build_config.change_log,
                                publishing_info=PublishingInfo(self.build_config.publishing_info),
                                common_language=None)
        return advertiser.html

    def validate(self, silent: bool = False) -> None:
        """
        Validates that the publisher's configuration is valid
        :param silent: Suppresses output of the validation operation
        :return: None
        """
        if not silent:
            print(f"Source: {self.source_dir}")
            print(f"Target: {self.target_dir}")
            print()

            if self.build_config.change_log is not None:
                self.build_config.change_log.print()
                print()

        self.__validated = True

    def assert_validated_config(self) -> None:
        """
        Asserts that the publishing configuration was validated - used to control program flow.
        :return: None
        """
        assert self.__validated, f"The publisher's configuration has not yet been validated. See Publisher.validate()"
        assert self.__validated_repo_reset, f"Failed to configure target repo. See Publisher.configure_target_repo()"

    def configure_target_repo(self, target_dir: str = None, target_repo_url: str = None, branch: str = "published") -> None:
        """
        Configures the build to publish to a public GitHub repository. In most cases, the default values should be used
        :param target_dir: The directory under /Repos that the GitHub will be cloned to
        :param target_repo_url: The locations of the GitHub public GitHub repo
        :param branch: The name of the branch to publish to
        :return: None
        """
        from dbacademy import common
        from dbacademy.dbbuild.build_utils_class import BuildUtils

        # Assume for now that we have failed. This overrides the default
        # of True meaning we have to succeed here to continue
        self.__validated_repo_reset = False

        new_target_dir = f"/Repos/Temp/{self.build_name}" if not self.i18n else f"/Repos/Temp/{self.build_name}-{self.common_language}"
        if target_dir == new_target_dir:
            common.print_warning(title="DEPRECATION WARNING", message=f"The value of the parameter \"target_dir\" is the same as the default value.\nConsider removing the parameter.")
        target_dir = target_dir or new_target_dir

        prefix = "https://github.com/databricks-academy"
        new_target_repo_url = f"{prefix}/{self.build_name}.git" if not self.i18n else f"{prefix}/{self.build_name}-{self.common_language}.git"
        if target_repo_url == new_target_repo_url:
            common.print_warning(title="DEPRECATION WARNING", message=f"The value of the parameter \"target_repo_url\" is the same as the default value.\nConsider removing the parameter.")
        target_repo_url = target_repo_url or new_target_repo_url

        self.target_dir = common.validate_type(target_dir, "target_dir", str)
        self.target_repo_url = common.validate_type(target_repo_url, "target_repo_url", str)

        BuildUtils.reset_git_repo(client=self.client,
                                  directory=self.target_dir,
                                  repo_url=self.target_repo_url,
                                  branch=branch,
                                  which=None)

        self.__validated_repo_reset = True

    def generate_published_docs(self) -> str:
        """
        Generates the HTML docs and writes them to the GitHub repo under /docs
        :return: The HTML results that should be rendered with displayHTML() from the calling notebook
        """
        import os, shutil

        source_docs_path = f"{self.source_repo}/docs"
        target_docs_path = f"/dbfs/mnt/resources.training.databricks.com/distributions/{self.build_name}/v{self.build_config.version}-PENDING/site"

        print(f"Source: {source_docs_path}")
        print(f"Target: {target_docs_path}")

        if os.path.exists(target_docs_path):
            shutil.rmtree(target_docs_path)

        shutil.copytree(src=f"/Workspace/{source_docs_path}",
                        dst=target_docs_path)

        print("-" * 80)
        for file in os.listdir(target_docs_path):
            print(file)

        html = f"""<html><body style="font-size:16px">
                         <div>Contents written to...</div>
                         <div>{target_docs_path}</div>
                   </body></html>"""
        return html

    # Used by notebooks
    # TODO Cannot define return type without circular dependencies
    def to_translator(self):
        """
        Creates an instance of the Translator class from this class
        :return: Translator
        """
        from dbacademy.dbbuild.publish.translator_class import Translator
        self.assert_validated_config()

        return Translator(self)

    # Used by notebooks
    # TODO Cannot define return type without circular dependencies
    def to_test_suite(self, test_type: str = None, keep_success: bool = False):
        """
        Creates an instance of the TestSuite class from this class. Typically, the parameters are not specified
        :param test_type: See TestSuite.test_type
        :param keep_success: See TestSuite.keep_success
        :return: TestSuite
        """
        from dbacademy.dbbuild.test.test_suite_class import TestSuite

        return TestSuite(build_config=self.build_config,
                         test_dir=self.target_dir,
                         test_type=test_type,
                         keep_success=keep_success)

    def __generate_html(self, notebook: NotebookDef) -> None:
        import time
        from dbacademy import dbgems

        if notebook.test_round < 2:
            return  # Skip for rounds 0 & 1

        start = int(time.time())

        path = f"../Source/{notebook.path}"
        dbgems.dbutils.notebook.run(path, timeout_seconds=60 * 5, arguments={
            "version": self.build_config.version,
            dbgems.GENERATING_DOCS: "true"
        })

        print(f"Generated docs for \"{notebook.path}\"...({int(time.time()) - start} seconds)")

    def generate_source_docs(self, asynchronous: bool = True) -> None:
        """
        For courses like DAWD that produce HTML output, run each notebook to generate those assets and write them to the /docs sub-folder in the source repository.
        :param asynchronous: True to generate docs asynchronously, False to process them serially
        :return:
        """
        from multiprocessing.pool import ThreadPool

        if asynchronous:
            with ThreadPool(len(self.build_config.notebooks)) as pool:
                pool.map(self.__generate_html, self.build_config.notebooks.values())
        else:
            for notebook in self.build_config.notebooks.values():
                self.__generate_html(notebook)

    def assert_created_dbcs(self) -> None:
        """
        Asserts that the various DBC files have been created - used to control program flow.
        :return: None
        """
        assert self.__created_dbcs, "The DBCS have not yet been created. See Publisher.create_dbcs()"

    def create_dbcs(self) -> str:
        """
        Exports the DBC files from the published directory and then saves the corresponding DBC file.
        Depending on the configuration, one copy can be written to a version-specific folder of the distribution system,
        one copy can be written to the "latest" folder of the distribution system, one copy will be written to dbfs:/FileStore
        for direct download from the calling notebook
        :return: The HTML results that should be rendered with displayHTML() from the calling notebook
        """
        import os, json, shutil, datetime
        from dbacademy import dbgems
        from dbacademy.dbbuild.build_utils_class import BuildUtils

        if self.target_repo_url is None:
            # If there is no repo, we need to make sure the notebooks were generated.
            self.assert_notebooks_generated()
        else:
            # With a target repo, we need to validate that there are no uncommitted changes
            self.assert_no_changes_in_target_repo()

        print(f"Exporting DBC from \"{self.target_dir}\"")
        data = self.build_config.client.workspace.export_dbc(self.target_dir)

        # The root directory for all versions of this course
        base_dir = f"/dbfs/mnt/resources.training.databricks.com/distributions/{self.build_config.build_name}"
        version_dir = f"{base_dir}/v{self.build_config.version}-PENDING"

        try:
            dbgems.dbutils.fs.ls(version_dir.replace("/dbfs/", "dbfs:/"))
        except:
            # It doesn't really exist, just delete here to clear the file cache.
            shutil.rmtree(version_dir, ignore_errors=True)

        assert not os.path.exists(version_dir), f"Cannot publish v{self.version}; it already exists."

        meta = {
            "created_at": str(datetime.datetime.now(datetime.timezone.utc)),
            "created_by": self.username,
            "version": self.version,
        }
        meta_bytes = bytearray()
        meta_bytes.extend(map(ord, json.dumps(meta, indent=4)))

        BuildUtils.write_file(data=meta_bytes,
                              overwrite=False,
                              target_name="Distributions System (versioned)",
                              target_file=f"{version_dir}/_meta.json")

        BuildUtils.write_file(data=data,
                              overwrite=False,
                              target_name="Distributions System (versioned)",
                              target_file=f"{version_dir}/{self.build_config.build_name}.dbc")

        # Provided simply for convenient download
        BuildUtils.write_file(data=data,
                              overwrite=True,
                              target_name="Workspace-Local FileStore",
                              target_file=f"dbfs:/FileStore/tmp/{self.build_config.build_name}-v{self.build_config.version}/{self.build_config.build_name}-v{self.build_config.version}-notebooks.dbc")

        url = f"/files/tmp/{self.build_config.build_name}-v{self.build_config.version}/{self.build_config.build_name}-v{self.build_config.version}-notebooks.dbc"

        self.__created_dbcs = True

        return f"""<html><body style="font-size:16px"><div><a href="{url}" target="_blank">Download DBC</a></div></body></html>"""

    def assert_created_docs(self) -> None:
        """
        Asserts that the published form of the Google-Docs (e.g. Slides Decks) were created - used to control program flow.
        :return: None
        """
        assert self.__created_docs, "The docs have not yet been created. See Publisher.create_docs()"

    def create_docs(self) -> str:
        """
        Published the Google-Docs specified by the build config by exporting them as PDFs and then saving them to the distribution system and then
        by copying those raw Google-Docs to the Published folder found in Databricks to facilitate instructor usage during class.
        :return:
        """
        from dbacademy.dbbuild.publish.docs_publisher import DocsPublisher
        from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo

        self.assert_created_dbcs()

        info = PublishingInfo(self.build_config.publishing_info)
        translation = info.translations.get(self.common_language)
        if translation is None:
            self.__created_docs = True
            return f"""<html><body style="font-size:16px">No documents to produce.</body></html>"""
        else:
            docs_publisher = DocsPublisher(build_name=self.build_config.build_name,
                                           version=self.version,
                                           translation=translation)
            docs_publisher.process_pdfs()
            print()
            docs_publisher.process_google_slides()
            html = docs_publisher.to_html()

            self.__created_docs = True
            return html

    def assert_validate_artifacts(self) -> None:
        """
        Asserts Publisher.validate_artifacts() was called  - used to control program flow.
        :return: None
        """
        assert self.__validated_artifacts, "The published artifacts have not been verified. See Publisher.validate_artifacts()"

    def validate_artifacts(self) -> None:
        """
        Asserts that all assets were valid. For Google-Docs this means to verify that they are in the distribution system. For DBCs, this means
        to import each DBC, locate the Version Info notebook, and then to validate that it contains the correct version number - used to control program flow.
        :return: None
        """
        from dbacademy.dbbuild.publish.artifact_validator_class import ArtifactValidator

        self.assert_created_docs()

        ArtifactValidator.from_publisher(self).validate_publishing_processes()

        self.__validated_artifacts = True

    def assert_no_changes_in_source_repo(self) -> None:
        """
        Asserts that there are no uncommitted changes in the source repository - used to control program flow.
        :return: None
        """
        method = "Publisher.validate_no_changes_in_source_repo()"
        assert self.__changes_in_source_repo is not None, f"The source repository was not tested for changes. Please run {method} to update the build state."
        assert self.__changes_in_source_repo == 0, f"Found {self.__changes_in_source_repo} changes(s) in the source repository. Please commit any changes before continuing and re-run {method} to update the build state."

    def validate_no_changes_in_source_repo(self, skip_validation=False) -> None:
        """
        Exports the source repository to a temp directory, enumerates all the files in that temp directory and then compares it
        to an enumeration of all files in the source directory. Reports on any uncommitted files
        :param skip_validation: True to override validation
        :return: None
        """
        from dbacademy import common
        from dbacademy.dbbuild.build_utils_class import BuildUtils

        if skip_validation:
            common.print_warning(f"SKIPPING VALIDATION", "The source directory is not being evaluated for pending changes")
            self.__changes_in_source_repo = 0

        else:
            repo_name = self.source_repo.split("/")[-1]
            results = BuildUtils.validate_no_changes_in_repo(client=self.client,
                                                             build_name=self.build_name,
                                                             repo_url=f"https://github.com/databricks-academy/{repo_name}",
                                                             directory=self.source_repo)
            self.__changes_in_source_repo = len(results)
            self.assert_no_changes_in_source_repo()

    def assert_no_changes_in_target_repo(self) -> None:
        """
        Asserts that there are no uncommitted changes in the target repository - used to control program flow.
        :return: None
        """
        method = "Publisher.validate_no_changes_in_target_repo()"
        assert self.__changes_in_target_repo is not None, f"The target repository was not tested for changes. Please run {method} to update the build state."
        assert self.__changes_in_target_repo == 0, f"Found {self.__changes_in_target_repo} changes(s) in the target repository. Please commit any changes before continuing and re-run {method} to update the build state."

    def validate_no_changes_in_target_repo(self, skip_validation=False) -> None:
        """
        Exports the target repository to a temp directory, enumerates all the files in that temp directory, then compares it
        to an enumeration of all files in the actual target directory and finally reports on any uncommitted files
        :param skip_validation: True to override validation
        :return: None
        """
        from dbacademy import common
        from dbacademy.dbbuild.build_utils_class import BuildUtils

        if skip_validation:
            common.print_warning(f"SKIPPING VALIDATION", "The target directory is not being evaluated for pending changes")
            self.__changes_in_target_repo = 0

        elif self.target_repo_url is None:
            self.assert_notebooks_generated()
            self.__changes_in_target_repo = 0
            msg = "Aborting build to force manual-confirmation before publishing."
            common.print_warning(f"SKIPPING VALIDATION", f"This course is not being published to a GitHub repo.\n{msg}")
            raise Exception(msg)

        else:
            results = BuildUtils.validate_no_changes_in_repo(client=self.client,
                                                             build_name=self.build_name,
                                                             repo_url=self.target_repo_url,
                                                             directory=self.target_dir)
            self.__changes_in_target_repo = len(results)
            self.assert_no_changes_in_target_repo()
