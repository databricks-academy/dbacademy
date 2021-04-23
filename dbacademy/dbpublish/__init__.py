# Databricks notebook source
DIRECTIVE_TODO = "TODO"
DIRECTIVE_ANSWER = "ANSWER"
DIRECTIVE_SOURCE_ONLY = "SOURCE-ONLY"
SUPPORTED_DIRECTIVES = [DIRECTIVE_SOURCE_ONLY, DIRECTIVE_ANSWER, DIRECTIVE_TODO]


def replace_contents(contents:str, replacements:dict):
  for old_value in replacements:
    new_value = replacements[old_value]
    contents = contents.replace(old_value, new_value)
  
  return contents
  
    
def get_leading_comments(command) -> []:
    leading_comments = []
    lines = command.split("\n")

    for line in lines:
        if line.strip().startswith("%"):
            # Remove the magic command from this line
            pos = line.find(" ")
            if pos != -1 and line[pos:].strip().startswith("#"):
              # append to our list
              comment = line[pos:].strip()[1:].strip()
              leading_comments.append(comment)
              
        elif line.strip().startswith("#"):
            # append to our list
            comment = line.strip()[1:].strip()
            leading_comments.append(comment)
            
        else:
            # All done, this is a non-comment
            return leading_comments

    return leading_comments
  
def parse_directives(comments):
  directives = list()
  for line in comments:
    if line == line.upper():
      # The comment is in all upper case, must be one or more directives
      if " " in line: raise ValueError(f"Whitespace found in directive: {line}")
      if "_" in line: raise ValueError(f"Underscore found in directive: {line}")
      
      directive = line.strip()
      if directive not in SUPPORTED_DIRECTIVES: raise ValueError(f"Unspported directive {directive}: {SUPPORTED_DIRECTIVES}")
      directives.append(line)
      
  return directives

# COMMAND ----------

from dbacademy.dbrest import DBAcademyRestClient


def publish(source_project:str, target_project:str, notebook_name:str, replacements:dict = {}) -> None:
    print("-" * 80)

    source_notebook_path = f"{source_project}/{notebook_name}"
    print(source_notebook_path)

    cmd_delim = "\n# COMMAND ----------\n"
    target_notebook_path = f"{target_project}/{notebook_name}"
    print(target_notebook_path)

    client = DBAcademyRestClient()
    raw_source = client.workspace().export_notebook(source_notebook_path)

    skipped = 0
    command_blocks = []
    commands = raw_source.split(cmd_delim)

    found_setup = False
    setup_prefix = "# magic %run ./_includes/setup-"

    for i in range(len(commands)):
        command = commands[i].strip()
        leading_comments = get_leading_comments(command)
        directives = parse_directives(leading_comments)

        if DIRECTIVE_SOURCE_ONLY in directives:
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Source-Only")

        elif DIRECTIVE_ANSWER in directives:
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
    final_source = replace_contents(final_source, replacements)

    client = DBAcademyRestClient()
    client.workspace().import_notebook("PYTHON", target_notebook_path, final_source)
