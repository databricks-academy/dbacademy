from dbacademy.dbpublish.notebook_def_class import NotebookDef
from dbacademy.dbpublish.publisher_class import Publisher
from datetime import date
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


def help_html():
    docs = {
        D_SOURCE_ONLY: f"Indicates that this cell is used in the source notebook only and is not to be included in the published version.",
        D_TODO: f"Indicates that this cell is an exercise for students - the entire cell is expected to be commented out.",
        D_ANSWER: f"Indicates that this cell is the solution to a preceding {D_TODO} cell. The build will fail if there total number of {D_TODO} cells is less than  the total number of {D_ANSWER} cells",
        # D_DUMMY: f"{D_DUMMY}: A directive that replaces itself with a nice little message for you - used in unit tests for the build engine",

        D_INCLUDE_HEADER_TRUE: f"Indicates that this notebook should include the default header - to be included in the first cell of the notebook.",
        D_INCLUDE_HEADER_FALSE: f"Indicates that this notebook should NOT include the default header - to be included in the first cell of the notebook.",
        D_INCLUDE_FOOTER_TRUE: f"Indicates that this notebook should include the default footer - to be included in the first cell of the notebook.",
        D_INCLUDE_FOOTER_FALSE: f"Indicates that this notebook should NOT include the default footer - to be included in the first cell of the notebook.",
    }

    html = "<html><body>"
    html += f"<h1>Publishing Help</h1>"
    html += f"<h2>Supported directives</h2>"
    for directive in SUPPORTED_DIRECTIVES:
        if directive in docs:
            doc = docs[directive]
            html += f"<div><b>{directive}</b>: {doc}</div>"
        else:
            html += f"<div><b>{directive}</b>: Undocumented</div>"

    html += "</body>"
    return html


def from_test_config(test_config, target_dir, include_solutions=True):
    publisher = Publisher(client=test_config.client,
                          version=test_config.version,
                          source_dir=test_config.source_dir,
                          target_dir=target_dir,
                          include_solutions=include_solutions)
    publisher.add_all(test_config.notebooks)
    return publisher


def replace_contents(contents: str, replacements: dict):
    import re

    for key in replacements:
        old_value = "{{" + key + "}}"
        new_value = replacements[key]
        contents = contents.replace(old_value, new_value)

    mustache_pattern = re.compile(r"{{[a-zA-Z\\-\\_\\#\\/]*}}")
    result = mustache_pattern.search(contents)
    if result is not None:
        raise Exception(f"A mustach pattern was detected after all replacements were processed: {result}")

    for icon in [":HINT:", ":CAUTION:", ":BESTPRACTICE:", ":SIDENOTE:", ":NOTE:"]:
        if icon in contents: raise Exception(
            f"The deprecated {icon} pattern was found after all replacements were processed.")

    # replacements[":HINT:"] =         """<img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp;**Hint:**"""
    # replacements[":CAUTION:"] =      """<img src="https://files.training.databricks.com/images/icon_warn_24.png"/>"""
    # replacements[":BESTPRACTICE:"] = """<img src="https://files.training.databricks.com/images/icon_best_24.png"/>"""
    # replacements[":SIDENOTE:"] =     """<img src="https://files.training.databricks.com/images/icon_note_24.png"/>"""

    return contents


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
    m = get_comment_marker(language)
    return f"""
{m} MAGIC %md-sandbox
{m} MAGIC &copy; {date.today().year} Databricks, Inc. All rights reserved.<br/>
{m} MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
{m} MAGIC <br/>
{m} MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
""".strip()


def get_cmd_delim(language):
    marker = get_comment_marker(language)
    return f"\n{marker} COMMAND ----------\n"


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


def update_and_validate_git_branch(client, path, target_branch="published"):
    repo_id = client.workspace().get_status(path)["object_id"]

    a_repo = client.repos().get(repo_id)
    a_branch = a_repo["branch"]
    assert a_branch == target_branch, f"""Expected the branch to be "{target_branch}", found "{a_branch}" """

    b_repo = client.repos().update(repo_id, target_branch)

    print(f"""Path:   {path}""")
    print(f"""Before: {a_repo["branch"]}  |  {a_repo["head_commit_id"]}""")
    print(f"""After:  {b_repo["branch"]}  |  {b_repo["head_commit_id"]}""")
