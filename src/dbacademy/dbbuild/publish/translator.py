__all__ = ["Translator"]

from typing import Optional
from dbacademy.common import validate
from dbacademy.dbbuild.build_config_data import BuildConfigData


class Translator:

    def __init__(self, *, build_config: BuildConfigData, require_i18n_selection: bool):
        self.__build_config = validate(build_config=build_config).required.as_type(BuildConfigData)

        # By default, we are not validated
        self.__validated = False
        self.__changes_in_source_repo = None
        self.__generated_notebooks = False
        self.__changes_in_target_repo = None
        self.__created_dbcs = False
        self.__created_docs = False
        self.__validated_artifacts = False

        # Defined in select_language
        self.lang_code = None
        self.version = None
        self.core_version = None
        self.common_language = None
        self.resources_folder = None

        # Defined in rest_repo
        self.source_branch = None
        self.source_dir = None
        self.source_repo_url = None

        self.target_branch = None
        self.target_dir = None
        self.target_repo_url = None

        self.temp_repo_dir = f"/Repos/Temp"
        self.temp_work_dir = f"/Workspace/Users/{self.build_config.username}/Temp"

        self.errors = []
        self.warnings = []

        if validate(require_i18n_selection=require_i18n_selection).required.bool():
            self.__select_i18n_language(self.build_config.source_repo)

    @property
    def validated(self):
        return self.__validated

    @property
    def build_config(self) -> BuildConfigData:
        return self.__build_config

    def update_i18n_guids(self, *, source_dir: str, add_guid: bool) -> None:
        """
        Used predominately by Notebook-based scripts, this command adds GUIDs when missing or moves the GUID from the %md line to the title.
        :param source_dir: The directory of the notebooks to be updated.
        :param add_guid: True if the GUID should be added, False if it should be removed.
        :return: None
        """
        import uuid
        from dbacademy.common import validate
        from dbacademy.dbbuild import dbb_constants
        from dbacademy.dbbuild.publish.notebook_def import NotebookDef
        from dbacademy.common import print_warning
        from dbacademy.dbbuild.publish import pub_utils

        add_guid = validate(add_guid=add_guid).required.bool()

        print_warning("USE WITH CAUTION", ("Use this method with caution as it has undergone only minimal testing.\n"
                                           "Most notably, moving GUIDs from %md commands into the title.\n"
                                           "The results can be validated by comparing diffs while committing."))

        source_files = [n.get("path") for n in self.build_config.client.workspace.ls(source_dir, recursive=True)]

        for source_notebook_path in source_files:

            print("=" * 100)
            print(f"Processing {source_notebook_path}")

            source_info = self.build_config.client.workspace.get_status(source_notebook_path)
            language = source_info.get("language")
            cmd_delim = NotebookDef.get_cmd_delim(language)
            cm = NotebookDef.get_comment_marker(language)

            raw_source = self.build_config.client.workspace.export_notebook(source_notebook_path)
            raw_lines = raw_source.split("\n")

            assert raw_lines[0] == f"{cm} {dbb_constants.NOTEBOOKS.DATABRICKS_NOTEBOOK_SOURCE}", f"""Expected line zero to be "{dbb_constants.NOTEBOOKS.DATABRICKS_NOTEBOOK_SOURCE}"."""
            del raw_lines[0]

            source = "\n".join(raw_lines)
            commands = source.split(cmd_delim)
            new_commands = list()

            for i, command in enumerate(commands):
                command = command.strip()
                lines = command.split("\n")
                line_zero = lines[0].strip()

                if pub_utils.is_markdown(cm=cm, command=command):
                    del lines[0]  # Remove the title or %md, add it back later
                    guid = pub_utils.extract_i18n_guid(i=i, cm=cm, command=command, scan_line=line_zero)

                    magic_md = f"{cm} MAGIC %md"
                    magic_md_sandbox = f"{cm} MAGIC %md-sandbox"

                    if pub_utils.is_not_titled(cm=cm, command=command):
                        next_line = None
                        pos = line_zero.find("--i18n-")

                        if pos >= 0:
                            line_zero = line_zero[:pos].strip()

                        elif line_zero in [magic_md, magic_md_sandbox]:
                            pass  # We can leave as-is

                        elif line_zero.startswith(magic_md):
                            next_line = f"{cm} MAGIC {line_zero[len(magic_md):].strip()}"
                            line_zero = magic_md

                        elif line_zero.startswith(magic_md_sandbox):
                            next_line = f"{cm} MAGIC {line_zero[len(magic_md_sandbox):].strip()}"
                            line_zero = magic_md_sandbox

                        # Add the "next_line" if it exists.
                        if next_line is not None:
                            lines.insert(0, next_line)

                        # Add line_zero
                        lines.insert(0, line_zero)

                    # Add the title back in
                    if guid is None:
                        guid = uuid.uuid4()
                        if add_guid:
                            print(f"Cmd #{i+1} | Adding GUID: {guid}")
                    else:
                        guid = guid[7:]
                        if not add_guid:
                            print(f"Cmd #{i+1} | Removing GUID: {guid}")

                    # Add the title back to the command
                    if add_guid:
                        # If we are removing the GUID, we just don't insert that line.
                        lines.insert(0, f"# {dbb_constants.NOTEBOOKS.DBTITLE} 1,--i18n-{guid}")

                new_command = "\n".join(lines)
                new_commands.append(new_command)

            new_source = f"{cm} {dbb_constants.NOTEBOOKS.DATABRICKS_NOTEBOOK_SOURCE}\n"
            new_source += f"\n{cmd_delim}\n".join(new_commands)

            self.build_config.client.workspace.import_notebook(language=language.upper(),
                                                               path=source_notebook_path,
                                                               content=new_source,
                                                               overwrite=True)
            print()

    def __select_i18n_language(self, source_repo: str):
        from dbacademy import dbgems

        self.resources_folder = f"{source_repo}/Resources"

        resources = self.build_config.client.workspace.ls(self.resources_folder) or list()
        language_options = [r.get("path").split("/")[-1] for r in resources]
        language_options = [p for p in language_options if not p.startswith("english-") and not p.startswith("_")]
        language_options.sort()

        if len(language_options) == 0:
            language_options.append("")

        default_langauge = language_options[0] if len(language_options) > 0 else ""
        print(f"""Default Language: "{default_langauge}".""")
        print(f"""Language Options: "{language_options}".""")

        dbgems.dbutils.widgets.dropdown("i18n_language",
                                        default_langauge,
                                        language_options,
                                        "i18n Language")

        self.i18n_language = dbgems.get_parameter("i18n_language", None)
        assert self.i18n_language is not None, f"The i18n language must be specified."
        assert self.i18n_language in language_options, f"The selected version must be one of {language_options}, found \"{self.i18n_language}\"."

        for notebook in self.build_config.notebooks.values():
            notebook.i18n_language = self.i18n_language

        # Include the i18n code in the version.
        # This hack just happens to work for japanese and korean
        self.lang_code = self.i18n_language[0:2].upper()
        self.common_language, self.core_version = self.i18n_language.split("-")
        self.core_version = self.core_version[1:]
        self.version = f"{self.core_version}-{self.lang_code}"

        # Include the i18n code in the version.
        # This hack just happens to work for japanese and korean
        self.common_language = self.i18n_language.split("-")[0]

    def __reset_published_repo(self):
        from dbacademy.dbbuild.build_utils import BuildUtils

        BuildUtils.reset_git_repo(client=self.build_config.client,
                                  directory=self.source_dir,
                                  repo_url=self.source_repo_url,
                                  branch=self.source_branch,
                                  which="published")

    def __reset_target_repo(self):
        from dbacademy.dbbuild.build_utils import BuildUtils

        BuildUtils.reset_git_repo(client=self.build_config.client,
                                  directory=self.target_dir,
                                  repo_url=self.target_repo_url,
                                  branch=self.target_branch,
                                  which="target")

    def create_published_message(self) -> str:
        from dbacademy.dbbuild.publish.advertiser import Advertiser
        from dbacademy.dbbuild.change_log import ChangeLog
        from dbacademy.dbbuild.publish.publishing_info import PublishingInfo

        self.assert_validated_artifacts()

        change_log = self.build_config.change_log or ChangeLog(source_repo=self.build_config.source_repo,
                                                               readme_file_name=self.build_config.readme_file_name,
                                                               target_version=self.core_version)

        advertiser = Advertiser(source_repo=self.build_config.source_repo,
                                name=self.build_config.name,
                                version=self.version,
                                change_log=change_log,
                                publishing_info=PublishingInfo(self.build_config.publishing_info),
                                common_language=self.common_language)
        return advertiser.html

    def assert_validated_artifacts(self):
        assert self.__validated_artifacts, "The artifacts have not yet been validated. See Translator.validate_artifacts()"

    def validate_artifacts(self):
        from dbacademy.dbbuild.publish import artifact_validator

        self.assert_created_docs()

        artifact_validator.from_translator(self).validate_publishing_processes()

        self.__validated_artifacts = True

    def validate(self,
                 source_dir: str = None,
                 source_repo_url: str = None,
                 source_branch: str = None,
                 target_dir: str = None,
                 target_repo_url: str = None,
                 target_branch: str = None):
        from dbacademy import common

        username = common.clean_string(self.build_config.username, replacement="_")
        self.source_branch = source_branch or f"published-v{self.core_version}"
        self.source_dir = source_dir or f"/Repos/Temp/{username}-{self.build_config.build_name}-english-{self.source_branch}"
        self.source_repo_url = source_repo_url or f"https://github.com/databricks-academy/{self.build_config.build_name}-english.git"

        username = common.clean_string(self.build_config.username, replacement="_")
        self.target_branch = target_branch or "published"
        self.target_dir = target_dir or f"/Repos/Temp/{username}-{self.build_config.build_name}-{self.common_language}"
        self.target_repo_url = target_repo_url or f"https://github.com/databricks-academy/{self.build_config.build_name}-{self.common_language}.git"

        print("Translation Details:")
        print(f"| Version:          {self.version}")
        print(f"| Core_version:     {self.core_version}")
        print(f"| Common_language:  {self.common_language}")
        print(f"| Resources_folder: {self.resources_folder}")
        print()
        print(f"| Source Dir:       {self.source_dir}")
        print(f"| Source Repo URL:  {self.source_repo_url}")
        print(f"| Source Branch:    {self.source_branch}")
        print()
        print(f"| Target Dir:       {self.target_dir}")
        print(f"| Target Repo URL:  {self.target_repo_url}")
        print(f"| Target Branch:    {self.target_branch}")
        print()

        self.__reset_published_repo()
        print()
        self.__reset_target_repo()

        self.__validated = True

    def _load_i18n_source(self, path):
        import os

        if path.startswith("Solutions/"):
            path = path[10:]
        if path.startswith("Includes/"):
            return ""

        i18n_source_path = f"/Workspace{self.resources_folder}/{self.i18n_language}/{path}.md"
        assert os.path.exists(i18n_source_path), f"Cannot find {i18n_source_path}"

        with open(f"{i18n_source_path}") as f:
            source = f.read()
            source = source.replace("<hr />\n--i18n-", "<hr>--i18n-")
            source = source.replace("<hr sandbox />\n--i18n-", "<hr sandbox>--i18n-")
            return source

    # noinspection PyMethodMayBeStatic
    def _load_i18n_guid_map(self, path: str, i18n_source: str):
        import re
        from dbacademy.dbbuild.publish.notebook_def import NotebookDef

        if i18n_source is None:
            return dict()

        i18n_guid_map = dict()

        # parts = re.split(r"^<hr>--i18n-", i18n_source, flags=re.MULTILINE)
        parts = re.split(r"^<hr>--i18n-|^<hr sandbox>--i18n-", i18n_source, flags=re.MULTILINE)

        name = parts[0].strip()[3:]
        path = path[10:] if path.startswith("Solutions/") else path
        if not path.startswith("Includes/"):
            assert name == path, f"Expected the notebook \"{path}\", found \"{name}\""

        for part in parts[1:]:
            guid, value = NotebookDef.parse_guid_and_value(part)
            i18n_guid_map[guid] = value

        return i18n_guid_map

    def assert_no_changes_in_source_repo(self):
        method = "Translator.validate_no_changes_in_source_repo()"
        assert self.__changes_in_source_repo is not None, f"The source repository was not tested for changes. Please run {method} to update the build state."
        assert self.__changes_in_source_repo == 0, f"Found {self.__changes_in_source_repo} changes(s) in the source repository. Please commit any changes before continuing and re-run {method} to update the build state."

    def validate_no_changes_in_source_repo(self, skip_validation=False) -> None:
        from dbacademy import common
        from dbacademy.dbbuild.build_utils import BuildUtils

        self.assert_validated()

        if skip_validation:
            common.print_warning(f"SKIPPING VALIDATION", "The source directory is not being evaluated for pending changes")
            self.__changes_in_source_repo = 0

        else:
            repo_name = f"{self.build_config.build_name}-source.git"
            results = BuildUtils.validate_no_changes_in_repo(client=self.build_config.client,
                                                             build_name=self.build_config.build_name,
                                                             repo_url=f"https://github.com/databricks-academy/{repo_name}",
                                                             directory=self.build_config.source_repo)
            print()
            self.__changes_in_source_repo = len(results)
            self.assert_no_changes_in_source_repo()

    def assert_no_changes_in_target_repo(self):
        method = "Translator.validate_no_changes_in_target_repo()"
        assert self.__changes_in_target_repo is not None, f"The target repository was not tested for changes. Please run {method} to update the build state."
        assert self.__changes_in_target_repo == 0, f"Found {self.__changes_in_target_repo} changes(s) in the target repository. Please commit any changes before continuing and re-run {method} to update the build state."

    def validate_no_changes_in_target_repo(self, skip_validation=False):
        from dbacademy import common
        from dbacademy.dbbuild.build_utils import BuildUtils

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
            results = BuildUtils.validate_no_changes_in_repo(client=self.build_config.client,
                                                             build_name=self.build_config.build_name,
                                                             repo_url=self.target_repo_url,
                                                             directory=self.target_dir)
            self.__changes_in_target_repo = len(results)
            self.assert_no_changes_in_target_repo()

    def assert_validated(self):
        assert self.validated, f"Cannot publish until the validator's configuration passes validation. Ensure that Translator.validate() was called and that all assignments passed"

    def assert_notebooks_generated(self):
        assert self.__generated_notebooks, f"The notebooks have not been published. See Translator.publish_notebooks()"

    def generate_notebooks(self, skip_generation: bool = False) -> Optional[str]:
        from datetime import datetime
        from dbacademy.dbbuild.build_utils import BuildUtils
        from dbacademy.dbbuild.publish.notebook_def import NotebookDef
        from dbacademy.dbbuild.publish.publisher import Publisher
        from dbacademy import dbgems, common
        from dbacademy.dbbuild.publish import pub_utils
        from dbacademy.dbbuild import dbb_constants

        self.assert_no_changes_in_source_repo()

        if skip_generation:
            self.__generated_notebooks = True
            common.print_warning(f"SKIPPING GENERATION", "Skipping the generation of notebooks")
            return None

        print(f"Publishing translated version of {self.build_config.name}, {self.version}")

        start = dbgems.clock_start()
        print(f"| Removing files from target directories", end="...")
        BuildUtils.clean_target_dir(self.build_config.client, self.target_dir, verbose=False)
        print(dbgems.clock_stopped(start))

        start = dbgems.clock_start()
        print(f"| Enumerating files", end="...")
        prefix = len(self.source_dir) + 1
        source_files = [f.get("path")[prefix:] for f in self.build_config.client.workspace.ls(self.source_dir, recursive=True)]
        print(dbgems.clock_stopped(start))

        # We have to first create the directory before writing to it.
        # Processing them first, once and only once, avoids duplicate REST calls.
        start = dbgems.clock_start()
        print(f"| Pre-creating directory structures", end="...")
        processed_directory = []
        for file in source_files:
            target_notebook_path = f"{self.target_dir}/{file}"
            if target_notebook_path not in processed_directory:
                processed_directory.append(target_notebook_path)
                target_notebook_dir = "/".join(target_notebook_path.split("/")[:-1])
                self.build_config.client.workspace.mkdirs(target_notebook_dir)
        print(dbgems.clock_stopped(start))

        print(f"\nProcessing {len(source_files)} notebooks:")
        for file in source_files:
            print(f"/{file}")

            source = self._load_i18n_source(file)
            i18n_guid_map = self._load_i18n_guid_map(file, source)

            # Compute the source and target directories
            source_notebook_path = f"{self.source_dir}/{file}"
            target_notebook_path = f"{self.target_dir}/{file}"

            source_info = self.build_config.client.workspace.get_status(source_notebook_path)
            language = source_info["language"].lower()
            cmd_delim = NotebookDef.get_cmd_delim(language)
            cm = NotebookDef.get_comment_marker(language)

            raw_source = self.build_config.client.workspace.export_notebook(source_notebook_path)
            raw_lines = raw_source.split("\n")
            header = raw_lines.pop(0)
            source = "\n".join(raw_lines)

            if file.startswith("Includes/"):
                # Write the original notebook to the target directory
                self.build_config.client.workspace.import_notebook(language=language.upper(),
                                                                   path=target_notebook_path,
                                                                   content=raw_source,
                                                                   overwrite=True)
                continue

            commands = source.split(cmd_delim)
            new_commands = [commands.pop(0)]  # Should be the header directives(?)

            for i, command in enumerate(commands):
                command = command.strip()
                line_zero = command.strip().split("\n")[0]
                guid = pub_utils.extract_i18n_guid(i=i, cm=cm, command=command, scan_line=line_zero)

                if guid is None:
                    new_commands.append(command)                            # No GUID, it's %python or other type of command, not MD
                else:
                    if guid not in i18n_guid_map.keys():
                        for key in i18n_guid_map.keys():
                            print(f"| {key}")
                        raise AssertionError(f"Cmd #{i+2} | The GUID \"{guid}\" was not found in \"{file}\".")

                    replacements = i18n_guid_map[guid].strip().split("\n")  # Get the replacement text for the specified GUID
                    cmd_lines = [f"{cm} MAGIC {x}" for x in replacements]   # Prefix the magic command to each line

                    lines = [line_zero]                                     # The first line doesn't exist in the guid map
                    if dbb_constants.NOTEBOOKS.DBTITLE in command:
                        # This is the new format, add %md or %md-sandbox
                        lines.append("%md-sandbox" if "%md-sandbox" in command else "%md")

                    lines.extend(cmd_lines)                                 # Convert to a set of lines and append
                    new_command = "\n".join(lines)                          # Combine all the lines into a new command
                    new_commands.append(new_command.strip())                # Append the new command to set of commands

            new_source = f"{header}\n"                           # Add the Databricks Notebook Header
            new_source += f"\n{cmd_delim}\n".join(new_commands)  # Join all the new_commands into one

            # Update the built_on and version_number - typically only found in the Version Info notebook.
            new_source = new_source.replace("{{course_name}}", self.build_config.name)
            new_source = new_source.replace("{{version_number}}", self.version)
            new_source = new_source.replace("{{built_on}}", datetime.now().strftime("%b %-d, %Y at %H:%M:%S UTC"))

            # Write the new notebook to the target directory
            self.build_config.client.workspace.import_notebook(language=language.upper(),
                                                               path=target_notebook_path,
                                                               content=new_source,
                                                               overwrite=True)
        self.__generated_notebooks = True

        return f"""<html><body style="font-size:16px">
                     <div><a href="{dbgems.get_workspace_url()}#workspace{self.target_dir}/{Publisher.VERSION_INFO_NOTEBOOK}" target="_blank">See Published Version</a></div>
                   </body></html>"""

    def assert_created_docs(self):
        assert self.__created_docs, "The docs have not yet been created. See Translator.create_docs()"

    def create_docs(self) -> str:
        from dbacademy.dbbuild.publish.docs_publisher import DocsPublisher
        from dbacademy.dbbuild.publish.publishing_info import PublishingInfo

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

    def assert_created_dbcs(self):
        assert self.__created_dbcs, "The DBCs have not yet been created. See Translator.create_dbcs()"

    def create_dbcs(self):
        from dbacademy import dbgems
        from dbacademy.dbbuild.build_utils import BuildUtils

        if self.target_repo_url is None:
            # If there is no repo, we need to make sure the notebooks were generated.
            self.assert_notebooks_generated()
        else:
            # With a target repo, we need to validate that there are no uncommitted changes
            self.assert_no_changes_in_target_repo()

        print(f"Exporting DBC from \"{self.target_dir}\"")
        data = self.build_config.client.workspace.export_dbc(self.target_dir)

        BuildUtils.write_file(data=data,
                              overwrite=False,
                              target_name="Distributions System (versioned)",
                              target_file=f"dbfs:/mnt/resources.training.databricks.com/distributions/{self.build_config.build_name}/v{self.version}-PENDING/{self.build_config.build_name}-v{self.version}-notebooks.dbc")

        BuildUtils.write_file(data=data,
                              overwrite=True,
                              target_name="Workspace-Local FileStore",
                              target_file=f"dbfs:/FileStore/tmp/{self.build_config.build_name}-v{self.version}-PENDING/{self.build_config.build_name}-v{self.version}-notebooks.dbc")

        url = f"/files/tmp/{self.build_config.build_name}-v{self.version}/{self.build_config.build_name}-v{self.version}-notebooks.dbc"
        dbgems.display_html(f"""<html><body style="font-size:16px"><div><a href="{url}" target="_blank">Download DBC</a></div></body></html>""")

        self.__created_dbcs = True
