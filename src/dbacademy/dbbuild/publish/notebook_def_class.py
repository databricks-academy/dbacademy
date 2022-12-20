from typing import Callable, Union, List


class NotebookError:
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class NotebookDef:
    from dbacademy.dbbuild.build_config_class import BuildConfig

    D_TODO = "TODO"
    D_ANSWER = "ANSWER"
    D_SOURCE_ONLY = "SOURCE_ONLY"
    D_DUMMY = "DUMMY"
    D_TROUBLESHOOTING_CONTENT = "TROUBLESHOOTING_CONTENT"
    D_INSTALL_LIBRARIES = "INSTALL_LIBRARIES"

    D_INCLUDE_HEADER_TRUE = "INCLUDE_HEADER_TRUE"
    D_INCLUDE_HEADER_FALSE = "INCLUDE_HEADER_FALSE"
    D_INCLUDE_FOOTER_TRUE = "INCLUDE_FOOTER_TRUE"
    D_INCLUDE_FOOTER_FALSE = "INCLUDE_FOOTER_FALSE"

    SUPPORTED_DIRECTIVES = [D_SOURCE_ONLY, D_ANSWER, D_TODO, D_DUMMY, D_TROUBLESHOOTING_CONTENT, D_INSTALL_LIBRARIES,
                            D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, ]

    def __init__(self,
                 *,
                 build_config: BuildConfig,
                 path: str,
                 replacements: dict,
                 include_solution: bool,
                 test_round: int,
                 ignored: bool,
                 order: int,
                 i18n: bool,
                 i18n_language: Union[None, str],
                 ignoring: list,
                 version: str):
        from dbacademy.dbbuild.build_config_class import BuildConfig

        assert type(build_config) == BuildConfig, f"""Expected the parameter "build_config" to be of type "BuildConfig", found "{type(build_config)}" """
        assert type(path) == str, f"""Expected the parameter "path" to be of type "str", found "{type(path)}" """
        assert type(replacements) == dict, f"""Expected the parameter "replacements" to be of type "dict", found "{type(replacements)}" """
        assert type(include_solution) == bool, f"""Expected the parameter "include_solution" to be of type "bool", found "{type(include_solution)}" """

        self.build_config = build_config
        self.client = build_config.client
        self.path = path
        self.replacements = replacements or dict()

        self.include_solution = include_solution
        self.errors: List[NotebookError] = list()
        self.warnings: List[NotebookError] = list()

        self.test_round = test_round
        self.ignored = ignored
        self.order = order

        self.i18n = i18n
        self.i18n_language = i18n_language
        self.i18n_guids = list()

        self.ignoring = ignoring
        self.version = version

    def __str__(self):
        result = self.path
        result += f"\n - include_solution = {self.include_solution}"
        result += f"\n - replacements = {self.replacements}"
        return result or ""

    def test(self, assertion: Callable[[], bool], message: str) -> bool:
        if assertion is None or not assertion():
            self.errors.append(NotebookError(message))
            return False
        else:
            return True

    def warn(self, assertion: Callable[[], bool], message: str) -> bool:
        if assertion is None or not assertion():
            self.warnings.append(NotebookError(message))
            return False
        else:
            return True

    def assert_no_warnings(self) -> None:
        if len(self.warnings) > 0:
            what = "warning was" if len(self.warnings) == 1 else "warnings were"
            print(f"CAUTION: {len(self.warnings)} {what} found while publishing")
            for warning in self.warnings:
                print("-" * 80)
                print(warning.message)
            print()

    def assert_no_errors(self, print_warnings) -> None:
        if len(self.errors) > 0:
            what = "error was" if len(self.errors) == 1 else "errors were"
            print(f"ABORTING: {len(self.errors)} {what} found while publishing")
            for error in self.errors:
                print("-" * 80)
                print(error.message)
            raise Exception("Publish aborted - see previous errors for more information")

        if print_warnings:
            self.assert_no_warnings()

    def test_notebook_exists(self, i, what, original_target, target, other_notebooks):
        if not target.startswith("../") and not target.startswith("./"):
            self.warn(lambda: False, f"Cmd #{i+1} | Found unexpected, relative, {what} target: \"{original_target}\" resolved as \"{target}\"".strip())
            return

        all_paths = set()
        for other in other_notebooks:
            # Add the original notebook's path
            all_paths.add(other.path)

            # Get the notebook's directory
            directory = '/'.join(other.path.split("/")[:-1])
            all_paths.add(directory)

            # While there are still parent directories, keep processing
            while directory.count("/") > 0:
                directory = '/'.join(directory.split("/")[:-1])
                all_paths.add(directory)

        offset = -1

        if target.startswith("../"):
            while target.startswith("../"):
                offset -= 1
                target = target[3:] 

        elif target.startswith("./"):
            target = target[2:]

        if "/" in self.path:
            parent = '/'.join(self.path.split("/")[:offset])
            target = f"{parent}/{target}"

        if target.startswith("/"):
            target = target[1:]

        notebooks = [n for n in all_paths if target == n]

        message = f"Cmd #{i+1} | Cannot find notebook for the {what} target: \"{original_target}\" resolved as \"{target}\""
        # self.test(lambda: len(notebooks) != 0, message)
        self.test(lambda: len(notebooks) != 0, message)

    def test_pip_cells(self, language: str, command: str, i: int) -> str:
        """
        Validates %pip cells, mostly to ensure that dbacademy-* resources are fixed to a specific version
        :param language: The language of the corresponding notebook
        :param command: The %run command string to be evaluated
        :param i: The zero-based index to the command within the notebook
        :return: None
        """

        # First verify that the specified command is a %pip cell
        cm = self.get_comment_marker(language)
        prefix = f"{cm} MAGIC %pip"
        if not command.startswith(prefix):
            return command

        # command = self.update_git_commit(command, "git+https://github.com/databricks-academy/dbacademy-gems", "published")
        # command = self.update_git_commit(command, "git+https://github.com/databricks-academy/dbacademy-rest", "published")
        # command = self.update_git_commit(command, "git+https://github.com/databricks-academy/dbacademy-helper", "published")

        for repo in ["dbacademy-helper", "dbacademy-rest", "dbacademy-gems", "dbacademy-courseware"]:
            full_repo = f"https://github.com/databricks-academy/{repo}"
            message = f"Cmd #{i + 1} | Using unsupported repo, {full_repo} is no longer supported; please use https://github.com/databricks-academy/dbacademy instead."
            self.test(lambda: full_repo not in command, message)

        return command

    def test_run_cells(self, language: str, command: str, i: int, other_notebooks: list) -> None:
        """
        Validates %run cells meet specific requirements
        :param language: The language of the corresponding notebook
        :param command: The %run command string to be evaluated
        :param i: The zero-based index to the command within the notebook
        :param other_notebooks: A complete list of notebooks for cross-validation
        :return: None
        """

        # First verify that the specified command is a %run cell
        cm = self.get_comment_marker(language)
        prefix = f"{cm} MAGIC %run"
        if not command.startswith(prefix):
            return

        line_zero = command.split("\n")[0]
        link = line_zero[len(prefix):].strip()

        if link.startswith("\""):
            link = link[1:]
            pos = link.find("\"")
            if pos < 0:
                self.warn(lambda: False, f"Cmd #{i+1} | Missing closing quote in %run target")
                return
            else:
                link = link[:pos]
        else:
            pos = link.find(" ")
            if pos > 0:
                link = link[:pos]

        self.test_notebook_exists(i, "%run", link, link, other_notebooks)

    # def validate_single_tick(self, i, command):
    #     """Test for usage of single-ticks that should also be bolded"""
    #
    #     import re
    #
    #     for result in re.findall(r"[^\*]`[^\s]*`[^\*]", command):
    #         if "single-tick" not in self.ignoring:
    #             self.warn(lambda: False, f"Cmd #{i+1} | Found a single-tick block, expected the **`xx`** pattern: \"{result}\"")

    def validate_md_link(self, i, command, other_notebooks):
        """Test for MD links to be replaced with html links"""

        import re

        # TODO Fix this error after a proper unit tests is created.
        # noinspection RegExpRedundantEscape
        for link in re.findall(r"(?<!!)\[.*?\]\(.*?\)", command):

            # If this is a relative link, we can ignore it.
            match = re.search(r"\(\$.*\)", link)

            if match:
                original_target = match.group()[1:-1]
                target = original_target[1:]
                self.test_notebook_exists(i, "MD link", original_target, target, other_notebooks)
            else:
                pass
                # This is not a notebook link, need to validate that the link exists.

    @staticmethod
    def parse_html_links(command):
        import re
        return re.findall(r"<a .*?</a>", command)

    def validate_html_link(self, i, command):
        """Test all HTML links to ensure they have a target set to _blank"""

        for link in self.parse_html_links(command):
            if "target=\"_blank\"" not in link:
                self.warn(lambda: False, f"Cmd #{i+1} | Found HTML link without the required target=\"_blank\": {link}")

            # Need to validate that the link exists.

    def test_source_for(self, command: str, i: int, what: str, template: str) -> bool:
        if what in command:
            pos = command.find(what)
            pos_a = command.rfind("\n", 0, pos)
            pos_a = 0 if pos_a == -1 else pos_a

            pos_b = command.find("\n", pos)
            pos_b = len(command)-1 if pos_b == -1 else pos_b

            line = command[pos_a:pos_b].strip()

            prefix = f"Cmd #{i+1} "
            padding = " "*len(prefix)
            return self.warn(lambda: False, template.format(prefix=prefix, what=what, padding=padding, line=line))

    def test_source_cells(self, language: str, command: str, i: int):

        if language not in ["python", "scala", "sql", "java", "r"]:
            return command

        datasets_template = "{prefix}| Course includes prohibited use of {what}:\n| {line}"
        self.test_source_for(command, i, "\"/mnt/training\"", template=datasets_template)
        self.test_source_for(command, i, "\"/databricks-datasets\"", template=datasets_template)

        replace_dbfs_template = "{prefix}| Course includes prohibited use of {what};\n| Consider creating DA.paths.working_path & DA.paths.datasets_path with DA.paths.to_vm_path(path) instead\n| {line}"
        if self.test_source_for(command, i, """replace("dbfs:/", "/dbfs/")""", template=replace_dbfs_template):

            dbfs_template = "{prefix}| Course includes prohibited use of {what};\n| {line}"
            self.test_source_for(command, i, "\"dbfs:/\"", template=dbfs_template)
            self.test_source_for(command, i, "\"/dbfs/\"", template=dbfs_template)

        return command

    def replace_guid(self, cm: str, command: str, i: int, i18n_guid_map: dict):
        lines = command.strip().split("\n")
        line_0 = lines[0][7+len(cm):]

        parts = line_0.strip().split(" ")
        for index, part in enumerate(parts):
            if part.strip() == "":
                del parts[index]

        md_tag = None if len(parts) < 1 else parts[0]
        guid = None if len(parts) < 2 else parts[1].strip()

        debug_info = line_0

        passed = self.test(lambda: len(lines) > 1, f"Cmd #{i + 1} | Expected MD to have more than 1 line of code with i18n enabled: {debug_info}")

        if len(parts) == 1:
            passed = passed and self.test(lambda: False, f"Cmd #{i + 1} | Missing the i18n directive: {debug_info}")
        else:
            passed = passed and self.test(lambda: len(parts) == 2, f"Cmd #{i + 1} | Expected the first line of MD to have only two words, found {len(parts)}: {debug_info}")
            passed = passed and self.test(lambda: parts[0] in ["%md", "%md-sandbox"], f"Cmd #{i + 1} | Expected word[0] of the first line of MD to be \"%md\" or \"%md-sandbox\", found {parts[0]}: {debug_info}")
            passed = passed and self.test(lambda: guid.startswith("--i18n-"), f"Cmd #{i + 1} | Expected word[1] of the first line of MD to start with \"--i18n-\", found {guid}: {debug_info}")

        if passed:
            passed = passed and self.test(lambda: guid not in self.i18n_guids, f"Cmd #{i + 1} | Duplicate i18n GUID found: {guid}")

        if passed:
            self.i18n_guids.append(guid)

            if not self.i18n_language:
                # This is a "standard" publish, just remove the i18n directive
                del lines[0]  # Remove the i18n directive
            else:
                # We must confirm that the replacement GUID actually exists
                if self.warn(lambda: guid in i18n_guid_map, f"The GUID \"{guid}\" was not found for the translation of {self.i18n_language}"):
                    lines = i18n_guid_map.get(guid).split("\n")

            if self.build_config.i18n_xml_tag_disabled:
                lines.insert(0, f"{cm} MAGIC {md_tag}")
            else:
                lines.insert(0, f"{cm} MAGIC {md_tag} <i18n value=\"{guid[7:]}\"/>")

            command = "\n".join(lines)

        return command

    def update_md_cells(self, language: str, command: str, i: int, i18n_guid_map: dict, other_notebooks: list):

        # First verify that the specified command is a mark-down cell
        cm = self.get_comment_marker(language)
        if not command.startswith(f"{cm} MAGIC %md"):
            return command
            
        # No longer enforcing this requirement
        # self.validate_single_tick(i, command)

        self.validate_md_link(i, command, other_notebooks)
        self.validate_html_link(i, command)

        if not self.i18n:
            self.test(lambda: "--i18n-" not in command, f"Cmd #{i+1} | Found the \"--i18n-\" marker but i18n processing is disabled.")
            return command
        else:
            return self.replace_guid(cm=cm,
                                     command=command,
                                     i=i,
                                     i18n_guid_map=i18n_guid_map)

    def create_resource_bundle(self, natural_language: str, source_dir: str, target_dir: str) -> None:
        natural_language = None if natural_language is None else natural_language.lower()

        assert type(natural_language) == str, f"""Expected the parameter "natural_language" to be of type "str", found "{type(natural_language)}" """
        assert type(source_dir) == str, f"""Expected the parameter "source_dir" to be of type "str", found "{type(source_dir)}" """
        assert type(target_dir) == str, f"""Expected the parameter "target_dir" to be of type "str", found "{type(target_dir)}" """

        print("-" * 80)
        print(f".../{self.path}")

        source_notebook_path = f"{source_dir}/{self.path}"

        source_info = self.client.workspace().get_status(source_notebook_path)
        language = source_info["language"].lower()

        raw_source = self.client.workspace().export_notebook(source_notebook_path)

        cmd_delim = self.get_cmd_delim(language)
        commands = raw_source.split(cmd_delim)

        md_commands = list()

        for i in range(len(commands)):
            command = commands[i].lstrip()

            cm = self.get_comment_marker(language)
            if command.startswith(f"{cm} MAGIC %md"):
                md_commands.append(command)

        if len(md_commands) == 0:
            print(f"Skipping resource - 0 MD cells: {self.path}")
        else:
            # self.publish_resource(language, md_commands, resource_root, resource_path)
            self.publish_resource(language, md_commands, target_dir, natural_language)

    def load_i18n_source(self, i18n_resources_dir):
        import os

        i18n_source_path = f"/Workspace{i18n_resources_dir}/{self.path}.md"
        if os.path.exists(i18n_source_path):
            with open(f"{i18n_source_path}") as f:
                source = f.read()
                source = source.replace("<hr />\n--i18n-", "<hr>--i18n-")
                source = source.replace("<hr sandbox />\n--i18n-", "<hr sandbox>--i18n-")
                return source

        # i18n_language better be None if the file doesn't exist, or it's in the "ignored" round zero or one
        self.warn(lambda: self.i18n_language is None or self.test_round in [0, 1], f"Resource not found ({self.test_round}): {i18n_source_path}")

        return None

    def load_i18n_guid_map(self, i18n_source: str):
        import re

        if i18n_source is None:
            return dict()

        i18n_guid_map = dict()

        # parts = re.split(r"^<hr>--i18n-", i18n_source, flags=re.MULTILINE)
        parts = re.split(r"^<hr>--i18n-|^<hr sandbox>--i18n-", i18n_source, flags=re.MULTILINE)

        name = parts[0].strip()[3:]
        self.test(lambda: name == self.path, f"Expected the notebook \"{self.path}\" but found\n                      \"{name}\"")

        for part in parts[1:]:
            guid, value = self.parse_guid_and_value(part)

            i18n_guid_map[guid] = value

            # sandbox_parts = re.split(r"^<hr sandbox>--i18n-", value, flags=re.MULTILINE)
            # i18n_guid_map[guid] = sandbox_parts[0]

            # for sandbox_part in sandbox_parts[1:]:
            #     guid, value = self.parse_guid_and_value(sandbox_part)
            #     i18n_guid_map[guid] = value

        return i18n_guid_map

    @staticmethod
    def parse_guid_and_value(part):
        pos = part.find("\n")
        pos = pos if pos >= 0 else len(part)

        guid = f"--i18n-{part[0:pos]}".strip()
        value = part[pos+1:]

        return guid, value

    @staticmethod
    def append_both(students_commands: List[str], solutions_commands: List[str], command: str):
        students_commands.append(command)
        solutions_commands.append(command)

    def build_troubleshooting_cells(self, students_commands: List[str], solutions_commands: List[str]):
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper

        self.append_both(students_commands, solutions_commands, """
%md ## Trouble Shooting
The following section provides general information about troubleshooting courseware produced by Databricks Academy. Each section correlates to a specific error message that an end-user may receive with direct links to more advanced content.""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Admin Required\n
Databricks Academy often requires the student to have admin privileges in a workspace to effect changes. In some cases, this is a byproduct of working through the courseware. However, in many cases, this is to configure the environment for users that would otherwise not require admin privileges.\n
For more current information, please <a href="https://files.training.databricks.com/static/troubleshooting.html#admin-required" target="_blank">Troubleshooting Admin Requirements</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Workspace Not Configured\n
Some of the content produced by Databricks Academy requires configuration (or re-configuration) of the workspace and its assets. This configuration includes complete tasks like creating cluster policies, cluster pools, granting specific user permissions, and other such tasks.\n
This error generally indicates that the notebook [Workspace-Setup]($./Includes/Workspace-Setup) was not executed.\n
For more current information, please <a href="https://files.training.databricks.com/static/troubleshooting.html#workspace-setup" target="_blank">Troubleshooting Workspace Setup</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Spark Version\n
To ensure that all labs execute as expected, Databricks Academy requires that the content provided be executed against a cluster configured with a specific runtime. Typically this is an LTS (long-term supported) version.\n
This course is tested against and requires one of the following DBRs (Databricks Runtime): **{{supported_dbrs}}**.\n
For more current information, please <a href="https://files.training.databricks.com/static/troubleshooting.html#spark-version" target="_blank">Troubleshooting the Spark Version</a>""".strip())

        self.append_both(students_commands, solutions_commands, f"""
%md ### Cannot write to DBFS\n
DBFS stands for Databricks File System and is a virtual file system that Databricks Academy uses to read data from and write data to. There are two primary locations, and depending on the courseware, some tertiary locations.\n 
The first location is **{DBAcademyHelper.get_dbacademy_datasets_path()}/**, which is a storage location for datasets used by this course. Datasets are downloaded from our data repositories and copied to this location to provide faster, in-region access to the course's datasets. Files written to this location are expected to be treated as read-only and shared by all consumers of this courseware.\n
The second location is **{DBAcademyHelper.get_dbacademy_users_path()}/**, a storage location for user-specific datasets and misc files generated by working through the courseware. This folder is subdivided by user and then by course, creating a per-user-per-course working directory.\n
In some cases, tertiary data may be written to **dbfs:/user/** as a default storage location for Spark and Spark tables. Like dbacademy-users, this folder is subdivided by user.\n
For more current information, please <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-write-dbfs" target="_blank">Troubleshooting DBFS Writes</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Cannot Install Libraries\n
Databricks Academy provides custom libraries to facilitate the development of its courseware. Your cluster requires access to these remotely hosted libraries so that they can be installed into your cluster.\n
Various companies restrict access to external sites to limit data exfiltration and address other security concerns, such as leaking potentially sensitive or proprietary information.\n
For more current information, please see <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-install-libraries" target="_blank">Troubleshooting Library Installation</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Requires Unity Catalog\n
Unity Catalog (UC) is a service provided by Databricks. This courseware requires that the workspace be UC Enabled. However, not all workspaces have (or can have) this feature enabled.\n
For more current information, please see <a href="https://files.training.databricks.com/static/troubleshooting.html#requires-unity-catalog" target="_blank">Troubleshooting Unity Catalog Requirements</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Cannot Create Schema\n
This course will require you to create a schema (aka database) which in turn requires that you have the necessary permissions. Sometimes, this is done as part of building a more complex example. But in most cases, the schema being created is a user-specific schema used to provide user-level isolation as multiple students work through various lessons in the same workspace.\n
For more current information, please see <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-create-schema" target="_blank">Troubleshooting Creating Schemas</a>""".strip())

        self.append_both(students_commands, solutions_commands, """
%md ### Cannot Create Catalog\n
This course will require you to create a catalog (typically in conjunction with using Unity Catalog) which in turn requires that you have the necessary permissions. In most cases, the new catalog is a user-specific catalog used to provide user-level isolation as multiple students work through various lessons in the same workspace.\n
For more current information, please see <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-create-catalog" target="_blank">Troubleshooting Creating Catalogs</a>""".strip())

    def build_install_libraries_cell(self, command, i):
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper

        lines = [line for line in command.split("\n") if line.strip().startswith("version =") or line.strip().startswith("version=")]
        if self.test(lambda: len(lines) == 1, f"Expected one and only one line that starts with \"version =\", found {len(lines)}."):
            version_line = lines[0]
            pos_a = version_line.find("\"")
            if self.test(lambda: pos_a >= 0, f"Cmd #{i+1} | Unable to parse the dbacademy library version for the INSTALL_LIBRARIES directive: {version_line}."):
                pos_b = version_line.find("\"", pos_a+1)
                if self.test(lambda: pos_b >= 0, f"Cmd #{i+1} | Unable to parse the dbacademy library version for the INSTALL_LIBRARIES directive: {version_line}."):
                    version = version_line[pos_a+1:pos_b]
                    template = DBAcademyHelper.TROUBLESHOOT_ERROR_TEMPLATE.replace("\"", "\\\"")
                    source = NotebookDef.SOURCE_INSTALL_LIBRARIES.format(template=template, version=version)
                    return source

        return command

    def publish(self, source_dir: str, target_dir: str, i18n_resources_dir: str, verbose: bool, debugging: bool, other_notebooks: list) -> None:
        from ..build_utils_class import BuildUtils

        assert type(source_dir) == str, f"""Expected the parameter "source_dir" to be of type "str", found "{type(source_dir)}" """
        assert type(target_dir) == str, f"""Expected the parameter "target_dir" to be of type "str", found "{type(target_dir)}" """
        assert type(i18n_resources_dir) == str, f"""Expected the parameter "resources_dir" to be of type "str", found "{type(i18n_resources_dir)}" """
        assert type(verbose) == bool, f"""Expected the parameter "verbose" to be of type "bool", found "{type(verbose)}" """
        assert type(debugging) == bool, f"""Expected the parameter "debugging" to be of type "bool", found "{type(debugging)}" """

        assert type(other_notebooks) == list, f"""Expected the parameter "other_notebooks" to be of type "list", found "{type(other_notebooks)}" """
        for i, notebook in enumerate(other_notebooks):
            assert type(other_notebooks[i]) == NotebookDef, f"""Expected the parameter "other_notebooks[{i}]" to be of type "NotebookDef", found "{type(other_notebooks[i])}" """

        self.errors = list()
        self.warnings = list()
        self.i18n_guids = list()

        print()
        print("=" * 80)
        print(f".../{self.path}")

        source_notebook_path = f"{source_dir}/{self.path}"
        source_info = self.client.workspace().get_status(source_notebook_path)
        language = source_info["language"].lower()

        raw_source = self.client.workspace().export_notebook(source_notebook_path)

        i18n_source = self.load_i18n_source(i18n_resources_dir)
        i18n_guid_map = self.load_i18n_guid_map(i18n_source)

        skipped = 0
        students_commands = []
        solutions_commands = []

        cmd_delim = self.get_cmd_delim(language)
        commands = raw_source.split(cmd_delim)

        todo_count = 0
        answer_count = 0

        include_header = False
        found_header_directive = False

        include_footer = False
        found_footer_directive = False

        for i in range(len(commands)):
            if debugging:
                print("\n" + ("=" * 80))
                print(f"Debug Command {i + 1}")

            command = commands[i].lstrip()

            self.test(lambda: "DBTITLE" not in command, f"Cmd #{i+1} | Unsupported Cell-Title found")

            # Misc tests for language specific cells
            command = self.test_source_cells(language, command, i)

            # Misc tests specific to %md cells along with i18n specific rewrites
            command = self.update_md_cells(language, command, i, i18n_guid_map, other_notebooks)

            # Misc tests specific to %run cells
            self.test_run_cells(language, command, i, other_notebooks)

            # Misc tests specific to %pip cells
            command = self.test_pip_cells(language, command, i)

            # Extract the leading comments and then the directives
            leading_comments = self.get_leading_comments(language, command.strip())
            directives = self.parse_directives(i, leading_comments)

            if debugging:
                if len(leading_comments) > 0:
                    print("   |-LEADING COMMENTS --" + ("-" * 57))
                    for comment in leading_comments:
                        print("   |" + comment)
                else:
                    print("   |-NO LEADING COMMENTS --" + ("-" * 54))

                if len(directives) > 0:
                    print("   |-DIRECTIVES --" + ("-" * 62))
                    for directive in directives:
                        print("   |" + directive)
                else:
                    print("   |-NO DIRECTIVES --" + ("-" * 59))

            # Update flags to indicate if we found the required header and footer directives
            include_header = True if NotebookDef.D_INCLUDE_HEADER_TRUE in directives else include_header
            found_header_directive = True if NotebookDef.D_INCLUDE_HEADER_TRUE in directives or NotebookDef.D_INCLUDE_HEADER_FALSE in directives else found_header_directive

            include_footer = True if NotebookDef.D_INCLUDE_FOOTER_TRUE in directives else include_footer
            found_footer_directive = True if NotebookDef.D_INCLUDE_FOOTER_TRUE in directives or NotebookDef.D_INCLUDE_FOOTER_FALSE in directives else found_footer_directive

            # Make sure we have one and only one directive in this command (ignoring the header directives)
            directive_count = 0
            for directive in directives:
                if directive not in [NotebookDef.D_INCLUDE_HEADER_TRUE, NotebookDef.D_INCLUDE_HEADER_FALSE, NotebookDef.D_INCLUDE_FOOTER_TRUE, NotebookDef.D_INCLUDE_FOOTER_FALSE]:
                    directive_count += 1
            self.test(lambda: directive_count <= 1, f"Cmd #{i+1} | Found multiple directives ({directive_count}): {directives}")

            # Process the various directives
            if command.strip() == "":
                skipped += self.skipping(i, "Empty Cell")
            elif NotebookDef.D_SOURCE_ONLY in directives:
                skipped += self.skipping(i, None)
            elif NotebookDef.D_INCLUDE_HEADER_TRUE in directives:
                skipped += self.skipping(i, None)
            elif NotebookDef.D_INCLUDE_HEADER_FALSE in directives:
                skipped += self.skipping(i, None)
            elif NotebookDef.D_INCLUDE_FOOTER_TRUE in directives:
                skipped += self.skipping(i, None)
            elif NotebookDef.D_INCLUDE_FOOTER_FALSE in directives:
                skipped += self.skipping(i, None)

            elif NotebookDef.D_TODO in directives:
                # This is a TO-DO cell, exclude from solution notebooks
                todo_count += 1
                command = self.clean_todo_cell(language, command, i)
                students_commands.append(command)

            elif NotebookDef.D_ANSWER in directives:
                # This is an ANSWER cell, exclude from lab notebooks
                answer_count += 1
                solutions_commands.append(command)

            elif NotebookDef.D_DUMMY in directives:
                students_commands.append(command)
                solutions_commands.append(command.replace("DUMMY", "DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive"))

            elif NotebookDef.D_INSTALL_LIBRARIES in directives:
                new_command = self.build_install_libraries_cell(command, i)
                students_commands.append(new_command)
                solutions_commands.append(new_command)

            elif NotebookDef.D_TROUBLESHOOTING_CONTENT in directives:
                self.build_troubleshooting_cells(students_commands, solutions_commands)

            else:
                # Not a TO-DO or ANSWER, just append to both
                self.append_both(students_commands, solutions_commands, command)

            # Check the command for BDC markers
            bdc_tokens = ["IPYTHON_ONLY", "DATABRICKS_ONLY",
                          "AMAZON_ONLY", "AZURE_ONLY", "TEST", "PRIVATE_TEST", "INSTRUCTOR_NOTE", "INSTRUCTOR_ONLY",
                          "SCALA_ONLY", "PYTHON_ONLY", "SQL_ONLY", "R_ONLY"
                                                                   "VIDEO", "ILT_ONLY", "SELF_PACED_ONLY", "INLINE",
                          "NEW_PART", "{dbr}"]

            for token in bdc_tokens:
                self.test(lambda: token not in command, f"""Cmd #{i+1} | Found the token "{token}" """)

            cm = self.get_comment_marker(language)
            if not command.startswith(f"{cm} MAGIC %md"):
                if language.lower() == "python":
                    if "lang-python" not in self.ignoring:
                        self.warn(lambda: "%python" not in command, f"""Cmd #{i+1} | Found "%python" in a Python notebook""")
                elif language.lower() == "sql":
                    if "lang-sql" not in self.ignoring:
                        self.warn(lambda: "%sql" not in command, f"""Cmd #{i+1} | Found "%sql" in a SQL notebook""")
                elif language.lower() == "scala":
                    if "lang-scala" not in self.ignoring:
                        self.warn(lambda: "%scala" not in command, f"""Cmd #{i+1} | Found "%scala" in a Scala notebook""")
                elif language.lower() == "r":
                    # We have to check both cases so as not to catch %run by accident
                    if "lang-r" not in self.ignoring:
                        self.warn(lambda: "%r " not in command,  f"""Cmd #{i+1} | Found "%r" in an R notebook""")
                        self.warn(lambda: "%r\n" not in command, f"""Cmd #{i+1} | Found "%r" in an R notebook""")
                else:
                    raise Exception(f"The language {language} is not supported")

            for year in range(2017, 2999):
                tag = f"{year} Databricks, Inc"
                self.test(lambda: tag not in command, f"""Cmd #{i+1} | Found copyright ({tag}) """)

        self.test(lambda: found_header_directive, f"One of the two header directives ({NotebookDef.D_INCLUDE_HEADER_TRUE} or {NotebookDef.D_INCLUDE_HEADER_FALSE}) were not found.")
        self.test(lambda: found_footer_directive, f"One of the two footer directives ({NotebookDef.D_INCLUDE_FOOTER_TRUE} or {NotebookDef.D_INCLUDE_FOOTER_FALSE}) were not found.")
        self.test(lambda: answer_count >= todo_count, f"Found more {NotebookDef.D_TODO} commands ({todo_count}) than {NotebookDef.D_ANSWER} commands ({answer_count})")

        if include_header is True:
            students_commands.insert(0, self.get_header_cell(language))
            solutions_commands.insert(0, self.get_header_cell(language))

        if include_footer is True:
            students_commands.append(self.get_footer_cell(language))
            solutions_commands.append(self.get_footer_cell(language))

        for key in ["\"", "*", "<", ">", "?", "\\", "|", ":"]:
            # Not checking for forward slash as the platform itself enforces this.
            self.warn(lambda: key not in self.path,  f"Found invalid character {key} in notebook name: {self.path}")

        # Create the student's notebooks
        students_notebook_path = f"{target_dir}/{self.path}"
        BuildUtils.print_if(verbose, students_notebook_path)
        BuildUtils.print_if(verbose, f"...publishing {len(students_commands)} commands")
        self.publish_notebook(language, students_commands, students_notebook_path, print_warnings=True)

        # Create the solutions notebooks
        if self.include_solution:
            solutions_notebook_path = f"{target_dir}/Solutions/{self.path}"
            BuildUtils.print_if(verbose, solutions_notebook_path)
            BuildUtils.print_if(verbose, f"...publishing {len(solutions_commands)} commands")
            self.publish_notebook(language, solutions_commands, solutions_notebook_path, print_warnings=False)

    def publish_resource(self, language: str, md_commands: list, target_dir: str, natural_language: str) -> None:
        import os

        m = self.get_comment_marker(language)
        target_path = f"{target_dir}/{natural_language}/{self.path}"

        final_source = f"# /{self.path}\n"

        # Processes all commands except the last
        for md_command in md_commands:
            md_command = md_command.replace(f"{m} MAGIC ", "")
            md_command = md_command.replace(f"%md-sandbox --i18n-", f"<hr sandbox>--i18n-")
            md_command = md_command.replace(f"%md --i18n-", f"<hr>--i18n-")
            final_source += md_command
            final_source += "\n"

        final_source = self.replace_contents(final_source)

        target_file = "/Workspace"+target_path+".md"
        target_dir = "/".join(target_file.split("/")[:-1])
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        if os.path.exists(target_file):
            os.remove(target_file)

        with open(target_file, "w") as w:
            w.write(final_source)

    def publish_notebook(self, language: str, commands: list, target_path: str, print_warnings: bool) -> None:
        m = self.get_comment_marker(language)
        final_source = f"{m} Databricks notebook source\n"

        # Processes all commands except the last
        for command in commands[:-1]:
            final_source += command
            final_source += self.get_cmd_delim(language)

        # Process the last command
        m = self.get_comment_marker(language)
        final_source += commands[-1]
        final_source += "" if commands[-1].startswith(f"{m} MAGIC") else "\n\n"

        final_source = self.replace_contents(final_source)

        self.assert_no_errors(print_warnings)

        parent_dir = "/".join(target_path.split("/")[0:-1])
        self.client.workspace().mkdirs(parent_dir)
        self.client.workspace().import_notebook(language.upper(), target_path, final_source)

    def clean_todo_cell(self, source_language, command, i):
        new_command = ""
        lines = command.split("\n")
        source_m = self.get_comment_marker(source_language)

        first = 0
        prefix = source_m

        for test_a in ["%r", "%md", "%sql", "%python", "%scala"]:
            test_b = f"{source_m} MAGIC {test_a}"
            if len(lines) > 1 and (lines[0].startswith(test_a) or lines[0].startswith(test_b)):
                first = 1
                cell_m = self.get_comment_marker(test_a)
                prefix = f"{source_m} MAGIC {cell_m}"

        for index in range(len(lines)):
            line = lines[index]

            if index == 0 and first == 1:
                # This is the first line, but the first is a magic command
                new_command += line

            elif (index == first) and line.strip() not in [f"{prefix} {NotebookDef.D_TODO}"]:
                self.test(lambda: False, f"""Cmd #{i + 1} | Expected line #{index + 1} to be the "{NotebookDef.D_TODO}" directive: "{line}" """)

            elif not line.startswith(prefix) and line.strip() != "" and line.strip() != f"{source_m} MAGIC":
                self.test(lambda: False, f"""Cmd #{i + 1} | Expected line #{index + 1} to be commented out: "{line}" with prefix "{prefix}" """)

            elif line.strip().startswith(f"{prefix} {NotebookDef.D_TODO}"):
                # Add as-is
                new_command += line

            elif line.strip() == "" or line.strip() == f"{source_m} MAGIC":
                # No comment, do not process
                new_command += line

            elif line.strip().startswith(f"{prefix} "):
                # Remove comment and space
                length = len(prefix) + 1
                new_command += line[length:]

            else:
                # Remove just the comment
                length = len(prefix)
                new_command += line[length:]

            # Add new line for all but the last line
            if index < len(lines) - 1:
                new_command += "\n"

        return new_command

    def replace_contents(self, contents: str):
        import re

        for key in self.replacements:
            old_value = "{{" + key + "}}"
            new_value = self.replacements[key]
            contents = contents.replace(old_value, new_value)

        # TODO Fix this error after a proper unit tests is created.
        # noinspection RegExpDuplicateCharacterInClass
        mustache_pattern = re.compile(r"{{[a-zA-Z\-\\_\\#\\/]*}}")
        result = mustache_pattern.search(contents)
        if result is not None:
            self.test(lambda: False, f"A mustache pattern was detected after all replacements were processed: {result}")

        for icon in [":HINT:", ":CAUTION:", ":BESTPRACTICE:", ":SIDENOTE:", ":NOTE:"]:
            if icon in contents:
                self.test(lambda: False, f"The deprecated {icon} pattern was found after all replacements were processed.")

        # No longer supported
        # replacements[":HINT:"] =         """<img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp;**Hint:**"""
        # replacements[":CAUTION:"] =      """<img src="https://files.training.databricks.com/images/icon_warn_24.png"/>"""
        # replacements[":BESTPRACTICE:"] = """<img src="https://files.training.databricks.com/images/icon_best_24.png"/>"""
        # replacements[":SIDENOTE:"] =     """<img src="https://files.training.databricks.com/images/icon_note_24.png"/>"""

        return contents

    @staticmethod
    def get_comment_marker(language):
        language = language.replace("%", "")

        if language.lower() in "python":
            return "#"
        elif language.lower() in "sql":
            return "--"
        elif language.lower() in "md":
            return "--"
        elif language.lower() in "r":
            return "#"
        elif language.lower() in "scala":
            return "//"
        else:
            raise ValueError(f"The language {language} is not supported.")

    @staticmethod
    def get_cmd_delim(language):
        marker = NotebookDef.get_comment_marker(language)
        return f"\n{marker} COMMAND ----------\n"

    def get_leading_comments(self, language, command) -> list:
        leading_comments = []
        lines = command.split("\n")

        source_m = self.get_comment_marker(language)
        first_line = lines[0].lower()

        if first_line.startswith(f"{source_m} magic %md"):
            cell_m = self.get_comment_marker("md")
        elif first_line.startswith(f"{source_m} magic %sql"):
            cell_m = self.get_comment_marker("sql")
        elif first_line.startswith(f"{source_m} magic %python"):
            cell_m = self.get_comment_marker("python")
        elif first_line.startswith(f"{source_m} magic %scala"):
            cell_m = self.get_comment_marker("scala")
        elif first_line.startswith(f"{source_m} magic %run"):
            cell_m = source_m  # Included to preclude trapping for R language below
        elif first_line.startswith(f"{source_m} magic %r"):
            cell_m = self.get_comment_marker("r")
        else:
            cell_m = source_m

        for il in range(len(lines)):
            line = lines[il]

            # Start by removing any "source" prefix
            if line.startswith(f"{source_m} MAGIC"):
                length = len(source_m) + 6
                line = line[length:].strip()

            elif line.startswith(f"{source_m} COMMAND"):
                length = len(source_m) + 8
                line = line[length:].strip()

            # Next, if it starts with a magic command, remove it.
            if line.strip().startswith("%"):
                # Remove the magic command from this line
                pos = line.find(" ")
                if pos == -1:
                    line = ""
                else:
                    line = line[pos:].strip()

            # Finally process the refactored-line for any comments.
            if line.strip() == cell_m or line.strip() == "":
                # empty comment line, don't break, just ignore
                pass

            elif line.strip().startswith(cell_m):
                # append to our list
                comment = line.strip()[len(cell_m):].strip()
                leading_comments.append(comment)

            else:
                # All done, this is a non-comment
                return leading_comments

        return leading_comments

    def parse_directives(self, i, comments):
        import re

        directives = list()

        for line in comments:
            if line == line.upper():
                # The comment is in all upper case,
                # must be one or more directives
                directive = line.strip()
                mod_directive = re.sub("[^-a-zA-Z_]", "_", directive)

                if directive in ["SELECT", "FROM", "AS", "AND"]:
                    pass  # not a real directive, but flagged as one because of its SQL syntax

                elif directive in [NotebookDef.D_TODO,
                                   NotebookDef.D_ANSWER,
                                   NotebookDef.D_SOURCE_ONLY,
                                   NotebookDef.D_INCLUDE_HEADER_TRUE,
                                   NotebookDef.D_INCLUDE_HEADER_FALSE,
                                   NotebookDef.D_INCLUDE_FOOTER_TRUE,
                                   NotebookDef.D_INCLUDE_FOOTER_FALSE]:
                    directives.append(line)

                elif "FILL-IN" in directive or "FILL_IN" in directive:
                    pass  # Not a directive, just a random chance

                elif directive != mod_directive:
                    if mod_directive in [f"__{NotebookDef.D_TODO}", f"___{NotebookDef.D_TODO}"]:
                        self.test(lambda: False, f"Cmd #{i+1} | Found double-comment of TODO directive")

                    # print(f"Skipping directive: {directive} vs {mod_directive}")
                    pass  # Number and symbols are not used in directives

                else:
                    result_a = self.warn(lambda: " " not in directive, f"""Cmd #{i+1} | Whitespace found in directive "{directive}": {line}""")
                    result_b = self.warn(lambda: "-" not in directive, f"""Cmd #{i+1} | Hyphen found in directive "{directive}": {line}""")
                    result_c = self.warn(lambda: directive in NotebookDef.SUPPORTED_DIRECTIVES, f"""Cmd #{i+1} | Unsupported directive "{directive}", see dbacademy.dbpublish.help_html() for more information.""")
                    if result_a and result_b and result_c:
                        directives.append(line)

        return directives

    @staticmethod
    def skipping(i, label):
        if label:
            print(f"Cmd #{i+1} | Skipping: {label}")
        return 1

    def get_header_cell(self, language):
        m = self.get_comment_marker(language)
        return f"""
    {m} MAGIC
    {m} MAGIC %md-sandbox
    {m} MAGIC
    {m} MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
    {m} MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
    {m} MAGIC </div>
    """.strip()

    def get_footer_cell(self, language):
        from datetime import date

        m = self.get_comment_marker(language)
        return f"""
    {m} MAGIC %md-sandbox
    {m} MAGIC &copy; {date.today().year} Databricks, Inc. All rights reserved.<br/>
    {m} MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
    {m} MAGIC <br/>
    {m} MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
    """.strip()

    SOURCE_INSTALL_LIBRARIES = """
def __validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {{response.status_code}} for {{site}})."
        assert response.status_code == 200, "{template}".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({{site}})."
        raise AssertionError("{template}".format(error=error, section="Cannot Install Libraries")) from e

def __install_libraries():
    global pip_command
    
    specified_version = f"{version}"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print("** Dependency Version Overridden *******************************************************************")
        print(f"* This course was built for {{specified_version}} of the DBAcademy Library, but it is being overridden via the Spark")
        print(f"* configuration variable \\"{{key}}\\". The use of version {version} is not advised as we")
        print(f"* cannot guarantee compatibility with this version of the course.")
        print("****************************************************************************************************")

    try:
        from dbacademy import dbgems  
        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
        else:
            print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {{version}}, found {{installed_version}}.")
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{{version}}"
        else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{{version}}/dbacademy-{{version[1:]}}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {{library_url}}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(f"WARNING: Using alternative library installation:\\n| default: %pip {{default_command}}\\n| current: %pip {{pip_command}}")
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()

__install_libraries()
""".strip()
