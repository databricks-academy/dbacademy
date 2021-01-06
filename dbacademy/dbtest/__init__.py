from dbacademy import dbgems
from dbacademy.dbrest import *

sc, spark, dbutils = dbgems.init_locals()
username = dbgems.get_username()


def get_leading_comments(command) -> []:
    leading_comments = []
    lines = command.split("\n")

    for line in lines:
        if line.strip().startswith("%"):
            pass  # skip magic commands
        elif line.strip().startswith("#"):
            # append to our list
            leading_comments.append(line)
        else:
            # All done, this is a non-comment
            return leading_comments

    return leading_comments


def is_answer_cell(comments) -> bool:
    for tag in ["#answer", "# answer"]:
        for comment in comments:
            if comment.strip().lower().startswith(tag):
                return True
    return False


def is_source_only_cell(comments) -> bool:
    for tag in ["#source-only", "# source-only"]:
        for comment in comments:
            if comment.strip().lower().startswith(tag):
                return True
    return False


def publish(source_project, target_project, notebook_name) -> None:
    print("-" * 80)

    source_notebook_path = f"{source_project}/{notebook_name}"
    print(source_notebook_path)

    cmd_delim = "\n# COMMAND ----------\n"
    target_notebook_path = f"{target_project}/{notebook_name}"
    print(target_notebook_path)

    raw_source = get_notebook(source_notebook_path)

    # source_blocks = map(lambda b: b.strip(), raw_source.split(cmd_delim))
    # source_blocks = list(source_blocks)

    skipped = 0
    command_blocks = []
    commands = raw_source.split(cmd_delim)

    found_setup = False
    setup_prefix = "# magic %run ./_includes/setup-"

    for i in range(len(commands)):
        command = commands[i].strip()
        leading_comments = get_leading_comments(command)

        if is_source_only_cell(leading_comments):
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Source-Only")

        elif is_answer_cell(leading_comments):
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Answer Cell")

        elif command.strip() == "":
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Empty Cell")

        elif command.strip().lower().startswith(setup_prefix):
            if found_setup:
                raise Exception(f"Duplicate call to setup in command #{i + 1}")
            else:
                found_setup = True
                command_blocks.append(command)
        else:
            command_blocks.append(command)

    final_source = ""

    for command in command_blocks[:-1]:
        final_source += command
        final_source += "\n"
        final_source += cmd_delim
        final_source += "\n"

    final_source += command_blocks[-1]

    import_notebook("PYTHON", target_notebook_path, final_source)

    # print(final_source)
