from dbacademy.dbrest import DBAcademyRestClient

D_TODO = "TODO"
D_ANSWER = "ANSWER"
D_SOURCE_ONLY = "SOURCE_ONLY"
D_DUMMY = "DUMMY"

D_INCLUDE_HEADER_TRUE = "INCLUDE_HEADER_TRUE"
D_INCLUDE_HEADER_FALSE = "INCLUDE_HEADER_FALSE"
D_INCLUDE_FOOTER_TRUE = "INCLUDE_FOOTER_TRUE"
D_INCLUDE_FOOTER_FALSE = "INCLUDE_FOOTER_FALSE"

SUPPORTED_DIRECTIVES = [D_SOURCE_ONLY, D_ANSWER, D_TODO, D_DUMMY,
                        D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, ]


class NotebookDef:
    def __init__(self, source_dir: str, target_dir: str, path: str, replacements: dict, include_solution: bool):
        assert type(source_dir) == str, f"""Expected the parameter "source_dir" to be of type "str", found "{type(source_dir)}" """
        assert type(target_dir) == str, f"""Expected the parameter "target_dir" to be of type "str", found "{type(target_dir)}" """
        assert type(path) == str, f"""Expected the parameter "path" to be of type "str", found "{type(path)}" """
        assert type(replacements) == dict, f"""Expected the parameter "replacements" to be of type "dict", found "{type(replacements)}" """
        assert type(include_solution) == bool, f"""Expected the parameter "include_solution" to be of type "bool", found "{type(include_solution)}" """

        self.path = path
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.replacements = replacements
        self.include_solution = include_solution

    def __str__(self):
        result = self.path
        result += f"\n - include_solution = {self.include_solution}"
        result += f"\n - source_dir = {self.source_dir}"
        result += f"\n - target_dir = {self.target_dir}"
        result += f"\n - replacements = {self.replacements}"
        return result

    def publish(self) -> None:
        print("-" * 80)

        source_notebook_path = f"{self.source_dir}/{self.path}"
        print(source_notebook_path)

        client = DBAcademyRestClient()

        source_info = client.workspace().get_status(source_notebook_path)
        language = source_info["language"].lower()

        raw_source = client.workspace().export_notebook(source_notebook_path)

        skipped = 0
        students_commands = []
        solutions_commands = []

        cmd_delim = get_cmd_delim(language)
        commands = raw_source.split(cmd_delim)

        todo_count = 0
        answer_count = 0

        include_header = False
        found_header_directive = False

        include_footer = False
        found_footer_directive = False

        for i in range(len(commands)):
            debugging = False

            if debugging:
                print("\n" + ("=" * 80))
                print(f"Debug Command {i + 1}")

            command = commands[i].lstrip()

            if "DBTITLE" in command:
                raise Exception(f"Unsupported Cell-Title found in Cmd #{i + 1}")

            # Extract the leading comments and then the directives
            leading_comments = get_leading_comments(language, command.strip())
            directives = parse_directives(i, leading_comments)

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
            include_header = True if D_INCLUDE_HEADER_TRUE in directives else include_header
            found_header_directive = True if D_INCLUDE_HEADER_TRUE in directives or D_INCLUDE_HEADER_FALSE in directives else found_header_directive

            include_footer = True if D_INCLUDE_FOOTER_TRUE in directives else include_footer
            found_footer_directive = True if D_INCLUDE_FOOTER_TRUE in directives or D_INCLUDE_FOOTER_FALSE in directives else found_footer_directive

            # Make sure we have one and only one directive in this command (ignoring the header directives)
            directive_count = 0
            for directive in directives:
                if directive not in [D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE,
                                     D_INCLUDE_FOOTER_FALSE]:
                    directive_count += 1
            assert directive_count <= 1, f"Found multiple directives ({directive_count}) in Cmd #{i + 1}: {directives}"

            # Process the various directives
            if command.strip() == "":
                skipped += skipping(i, "Empty Cell")
            elif D_SOURCE_ONLY in directives:
                skipped += skipping(i, "Source-Only")
            elif D_INCLUDE_HEADER_TRUE in directives:
                skipped += skipping(i, "Including Header")
            elif D_INCLUDE_HEADER_FALSE in directives:
                skipped += skipping(i, "Excluding Header")
            elif D_INCLUDE_FOOTER_TRUE in directives:
                skipped += skipping(i, "Including Footer")
            elif D_INCLUDE_FOOTER_FALSE in directives:
                skipped += skipping(i, "Excluding Footer")

            elif D_TODO in directives:
                # This is a TODO cell, exclude from solution notebooks
                todo_count += 1
                command = clean_todo_cell(language, command, i)
                students_commands.append(command)

            elif D_ANSWER in directives:
                # This is an ANSWER cell, exclude from lab notebooks
                answer_count += 1
                solutions_commands.append(command)

            elif D_DUMMY in directives:
                students_commands.append(command)
                solutions_commands.append(command.replace("DUMMY",
                                                          "DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive"))

            else:
                # Not a TODO or ANSWER, just append to both
                students_commands.append(command)
                solutions_commands.append(command)

            # Check the command for BDC markers
            bdc_tokens = ["IPYTHON_ONLY", "DATABRICKS_ONLY",
                          "AMAZON_ONLY", "AZURE_ONLY", "TEST", "PRIVATE_TEST", "INSTRUCTOR_NOTE", "INSTRUCTOR_ONLY",
                          "SCALA_ONLY", "PYTHON_ONLY", "SQL_ONLY", "R_ONLY"
                                                                   "VIDEO", "ILT_ONLY", "SELF_PACED_ONLY", "INLINE",
                          "NEW_PART", "{dbr}"]

            for token in bdc_tokens:
                assert token not in command, f"""Found the token "{token}" in command #{i + 1}"""

            if language.lower() == "python":
                assert "%python" not in command, f"""Found "%python" in command #{i + 1}"""
            elif language.lower() == "sql":
                assert "%sql" not in command, f"""Found "%sql" in command #{i + 1}"""
            elif language.lower() == "scala":
                assert "%scala" not in command, f"""Found "%scala" in command #{i + 1}"""
            elif language.lower() == "r":
                assert "%r " not in command, f"""Found "%r" in command #{i + 1}"""
                assert "%r\n" not in command, f"""Found "%r" in command #{i + 1}"""
            else:
                raise Exception(f"The language {language} is not supported")

            for year in range(2017, 2999):
                tag = f"{year} Databricks, Inc"
                assert tag not in command, f"""Found copyright ({tag}) in command #{i + 1}"""

        assert found_header_directive, f"One of the two header directives ({D_INCLUDE_HEADER_TRUE} or {D_INCLUDE_HEADER_FALSE}) were not found."
        assert found_footer_directive, f"One of the two footer directives ({D_INCLUDE_FOOTER_TRUE} or {D_INCLUDE_FOOTER_FALSE}) were not found."
        assert answer_count >= todo_count, f"Found more {D_TODO} commands ({todo_count}) than {D_ANSWER} commands ({answer_count})"

        if include_header is True:
            students_commands.insert(0, get_header_cell(language))
            solutions_commands.insert(0, get_header_cell(language))

        if include_footer is True:
            students_commands.append(get_footer_cell(language))
            solutions_commands.append(get_footer_cell(language))

        # Create the student's notebooks
        students_notebook_path = f"{self.target_dir}/{self.path}"
        print(students_notebook_path)
        print(f"...publishing {len(students_commands)} commands")
        publish_notebook(language, students_commands, students_notebook_path, self.replacements)

        # Create the solutions notebooks
        if self.include_solution:
            solutions_notebook_path = f"{self.target_dir}/Solutions/{self.path}"
            print(solutions_notebook_path)
            print(f"...publishing {len(solutions_commands)} commands")
            publish_notebook(language, solutions_commands, solutions_notebook_path, self.replacements)


def publish_notebook(language: str, commands: list, target_path: str, replacements: dict = None) -> None:
    replacements = dict() if replacements is None else replacements

    m = get_comment_marker(language)
    final_source = f"{m} Databricks notebook source\n"

    # Processes all commands except the last
    for command in commands[:-1]:
        final_source += command
        final_source += get_cmd_delim(language)

    # Process the last command
    m = get_comment_marker(language)
    final_source += commands[-1]
    final_source += "" if commands[-1].startswith(f"{m} MAGIC") else "\n\n"

    final_source = replace_contents(final_source, replacements)

    client = DBAcademyRestClient()
    parent_dir = "/".join(target_path.split("/")[0:-1])
    client.workspace().mkdirs(parent_dir)
    client.workspace().import_notebook(language.upper(), target_path, final_source)


def replace_contents(contents: str, replacements: dict):
    import re

    for key in replacements:
        old_value = "{{" + key + "}}"
        new_value = replacements[key]
        contents = contents.replace(old_value, new_value)

    mustache_pattern = re.compile(r"{{[a-zA-Z\\-\\_\\#\\/]*}}")
    result = mustache_pattern.search(contents)
    if result is not None:
        raise Exception(f"A mustache pattern was detected after all replacements were processed: {result}")

    for icon in [":HINT:", ":CAUTION:", ":BESTPRACTICE:", ":SIDENOTE:", ":NOTE:"]:
        if icon in contents: raise Exception(
            f"The deprecated {icon} pattern was found after all replacements were processed.")

    # replacements[":HINT:"] =         """<img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp;**Hint:**"""
    # replacements[":CAUTION:"] =      """<img src="https://files.training.databricks.com/images/icon_warn_24.png"/>"""
    # replacements[":BESTPRACTICE:"] = """<img src="https://files.training.databricks.com/images/icon_best_24.png"/>"""
    # replacements[":SIDENOTE:"] =     """<img src="https://files.training.databricks.com/images/icon_note_24.png"/>"""

    return contents


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
        raise Exception(f"The language {language} is not supported.")


def get_cmd_delim(language):
    marker = get_comment_marker(language)
    return f"\n{marker} COMMAND ----------\n"


def get_leading_comments(language, command) -> list:
    leading_comments = []
    lines = command.split("\n")

    source_m = get_comment_marker(language)
    first_line = lines[0].lower()

    if first_line.startswith(f"{source_m} magic %md"):
        cell_m = get_comment_marker("md")
    elif first_line.startswith(f"{source_m} magic %sql"):
        cell_m = get_comment_marker("sql")
    elif first_line.startswith(f"{source_m} magic %python"):
        cell_m = get_comment_marker("python")
    elif first_line.startswith(f"{source_m} magic %scala"):
        cell_m = get_comment_marker("scala")
    elif first_line.startswith(f"{source_m} magic %run"):
        cell_m = source_m  # Included to preclude traping for R language below
    elif first_line.startswith(f"{source_m} magic %r"):
        cell_m = get_comment_marker("r")
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


def parse_directives(i, comments):
    import re

    directives = list()
    for line in comments:
        if line == line.upper():
            # The comment is in all upper case,
            # must be one or more directives
            directive = line.strip()
            mod_directive = re.sub("[^a-zA-Z_]", "_", directive)

            if directive in [D_TODO, D_ANSWER, D_SOURCE_ONLY, D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE,
                             D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE]:
                directives.append(line)

            elif "FILL-IN" in directive or "FILL_IN" in directive:
                # print("Skipping directive: FILL-IN")
                pass  # Not a directive, just a random chance

            elif directive != mod_directive:
                if mod_directive in [f"__{D_TODO}", f"___{D_TODO}"]:
                    raise Exception(f"Double-Comment of TODO directive found in Cmd #{i + 1}")

                # print(f"Skipping directive: {directive} vs {mod_directive}")
                pass  # Number and symbols are not used in directives

            else:
                # print(f"""Processing "{directive}" in Cmd #{i+1} """)
                if " " in directive:
                    raise ValueError(f"""Whitespace found in directive "{directive}", Cmd #{i + 1}: {line}""")
                if "-" in directive:
                    raise ValueError(f"""Hyphen found in directive "{directive}", Cmd #{i + 1}: {line}""")
                if directive not in SUPPORTED_DIRECTIVES:
                    raise ValueError(
                        f"""Unsupported directive "{directive}" in Cmd #{i + 1} {SUPPORTED_DIRECTIVES}: {line}""")
                directives.append(line)

    return directives


def skipping(i, label):
    print(f"Skipping Cmd #{i + 1} - {label}")
    return 1


def clean_todo_cell(source_language, command, cmd):
    new_command = ""
    lines = command.split("\n")
    source_m = get_comment_marker(source_language)

    first = 0
    prefix = source_m

    for test_a in ["%r", "%md", "%sql", "%python", "%scala"]:
        test_b = f"{source_m} MAGIC {test_a}"
        if len(lines) > 1 and (lines[0].startswith(test_a) or lines[0].startswith(test_b)):
            first = 1
            cell_m = get_comment_marker(test_a)
            prefix = f"{source_m} MAGIC {cell_m}"

    # print(f"Clean TODO cell, Cmd {cmd+1}")

    for i in range(len(lines)):
        line = lines[i]

        if i == 0 and first == 1:
            # print(f" - line #{i+1}: First line is a magic command")
            # This is the first line, but the first is a magic command
            new_command += line

        elif (i == first) and line.strip() not in [f"{prefix} {D_TODO}"]:
            raise Exception(
                f"""Expected line #{i + 1} in Cmd #{cmd + 1} to be the "{D_TODO}" directive: "{line}"\n{"-" * 80}\n{command}\n{"-" * 80}""")

        elif not line.startswith(prefix) and line.strip() != "" and line.strip() != f"{source_m} MAGIC":
            raise Exception(f"""Expected line #{i + 1} in Cmd #{cmd + 1} to be commented out: "{line}" with prefix "{prefix}" """)

        elif line.strip().startswith(f"{prefix} {D_TODO}"):
            # print(f""" - line #{i+1}: Processing TODO line ({prefix}): "{line}" """)
            # Add as-is
            new_command += line

        elif line.strip() == "" or line.strip() == f"{source_m} MAGIC":
            # print(f""" - line #{i+1}: Empty line, just add: "{line}" """)
            # No comment, do not process
            new_command += line

        elif line.strip().startswith(f"{prefix} "):
            # print(f""" - line #{i+1}: Removing comment and space ({prefix}): "{line}" """)
            # Remove comment and space
            length = len(prefix) + 1
            new_command += line[length:]

        else:
            # print(f""" - line #{i+1}: Removing comment only ({prefix}): "{line}" """)
            # Remove just the comment
            length = len(prefix)
            new_command += line[length:]

        # Add new line for all but the last line
        if i < len(lines) - 1:
            new_command += "\n"

    return new_command


def get_header_cell(language):
    m = get_comment_marker(language)
    return f"""
{m} MAGIC
{m} MAGIC %md-sandbox
{m} MAGIC
{m} MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
{m} MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
{m} MAGIC </div>
""".strip()


def get_footer_cell(language):
    from datetime import date

    m = get_comment_marker(language)
    return f"""
{m} MAGIC %md-sandbox
{m} MAGIC &copy; {date.today().year} Databricks, Inc. All rights reserved.<br/>
{m} MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
{m} MAGIC <br/>
{m} MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
""".strip()
