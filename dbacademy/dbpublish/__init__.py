# Databricks notebook source
DIRECTIVE_TODO = "TODO"
DIRECTIVE_ANSWER = "ANSWER"
DIRECTIVE_SOURCE_ONLY = "SOURCE_ONLY"
DIRECTIVE_SELF_PACED_ONLY = "SELF_PACED_ONLY"
DIRECTIVE_ILT_ONLY = "ILT_ONLY"

SUPPORTED_DIRECTIVES = [DIRECTIVE_SOURCE_ONLY, DIRECTIVE_ANSWER, DIRECTIVE_TODO, DIRECTIVE_SELF_PACED_ONLY, DIRECTIVE_ILT_ONLY, ]

def replace_contents(contents:str, replacements:dict):
  for old_value in replacements:
    new_value = replacements[old_value]
    contents = contents.replace(old_value, new_value)
  
  return contents
  
    
def get_leading_comments(command) -> []:
    leading_comments = []
    lines = command.split("\n")

    for line in lines:
        if line.startswith("# MAGIC"):
          line = line[7:].strip()
        elif line.startswith("# COMMAND"):
          line = line[9:].strip()
        
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
  
def parse_directives(i, comments):
  directives = list()
  for line in comments:
    if line != line.upper():
        # Directives only in the header. Now that
        # there is other content, we can move on.
        return directives
    else:
      # The comment is in all upper case, must be one or more directives
      directive = line.strip()

      if directive in ["TODO", "ANSWER", "SOURCE_ONLY"]:
          pass
      else:
          print(f"""Processing "{directive}" in Cmd #{i} """)
          if " " in directive: 
            raise ValueError(f"""Whitespace found in directive "{directive}", Cmd #{i}: {line}""")
          if "-" in directive: 
            raise ValueError(f"""Hyphen found in directive "{directive}", Cmd #{i}: {line}""")
          if directive not in SUPPORTED_DIRECTIVES: 
            raise ValueError(f"""Unspported directive "{directive}" in Cmd #{i} {SUPPORTED_DIRECTIVES}: {line}""")
          directives.append(line)

  return directives

# COMMAND ----------

from dbacademy.dbrest import DBAcademyRestClient

found_setup = False
cmd_delim = "\n# COMMAND ----------\n"


def assert_only_one_setup_cell(command, index):
    global found_setup
    setup_prefix = "# magic %run ./_includes/setup-"
    
    if command.strip().lower().startswith(setup_prefix):
        # This is a setup cell - just want to make sure we don't have duplicates, 
        # a problem Jacob creates when he is testing the setup notebooks
        if found_setup:
            raise Exception(f"Duplicate call to setup in command #{index + 1}")
        else:
            found_setup = True
            
            
def publish_notebook(commands:list, target_path:str, replacements:dict = {}) -> None:
    final_source = ""
    
    for command in commands[:-1]:
        final_source += command
        final_source += "\n"
        final_source += cmd_delim
        final_source += "\n"

    final_source += commands[-1]
    final_source = replace_contents(final_source, replacements)

    client = DBAcademyRestClient()
    parent_dir = "/".join(target_path.split("/")[0:-1])
    client.workspace().mkdirs(parent_dir)
    client.workspace().import_notebook("PYTHON", target_path, final_source)
    
    
def publish(source_project:str, target_project:str, notebook_name:str, replacements:dict = {}, include_solution=False) -> None:
    global found_setup
    print("-" * 80)

    source_notebook_path = f"{source_project}/{notebook_name}"
    print(source_notebook_path)

    client = DBAcademyRestClient()
    raw_source = client.workspace().export_notebook(source_notebook_path)

    skipped = 0
    students_commands = []
    solutions_commands = []
    commands = raw_source.split(cmd_delim)

    found_setup = False
    todo_count = 0
    answ_count = 0

    for i in range(len(commands)):
        command = commands[i].strip()
        leading_comments = get_leading_comments(command)
        directives = parse_directives(i, leading_comments)

        if DIRECTIVE_SOURCE_ONLY in directives:
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Source-Only")

        elif command.strip() == "":
            skipped += 1
            print(f"Skipping Cmd #{i + 1} - Empty Cell")

        elif DIRECTIVE_TODO in directives:
            # This is a TODO cell, exclude from solution notebooks
            todo_count += 1
            assert_only_one_setup_cell(command, i)
            students_commands.append(command)

        elif DIRECTIVE_ANSWER in directives:
            # This is an ANSWER cell, exclude from lab notebooks
            answ_count += 1
            assert_only_one_setup_cell(command, i)
            solutions_commands.append(command)

        else:
            # Not a TODO or ANSWER, just append to both
            assert_only_one_setup_cell(command, i)
            students_commands.append(command)
            solutions_commands.append(command)

        # Check the command for BDC markers
        bdc_tokens = ["IPYTHON_ONLY","DATABRICKS_ONLY","SCALA_ONLY","PYTHON_ONLY","SQL_ONLY","R_ONLY","AMAZON_ONLY","AZURE_ONLY","TEST","PRIVATE_TEST",
                  "VIDEO","INSTRUCTOR_NOTE","INSTRUCTOR_ONLY","ILT_ONLY","SELF_PACED_ONLY","ALL_NOTEBOOKS","INLINE","NEW_PART","%python","%r"]
        
        for token in bdc_tokens:
            assert token not in command, f"Found {token} in command #{i}"
            
    if todo_count > answ_count:
        raise Exception(f"Found more {DIRECTIVE_TODO} commands ({todo_count}) than {DIRECTIVE_ANSWER} commands ({answ_count})")
            
    # Create the student's notebooks
    students_notebook_path = f"{target_project}/{notebook_name}"
    print(students_notebook_path)
    print(f"...publishing {len(students_commands)} commands")
    publish_notebook(students_commands, students_notebook_path, replacements)
    
    # Create the solutions notebooks
    if include_solution:
        solutions_notebook_path = f"{target_project}/Solutions/{notebook_name}"
        print(solutions_notebook_path)
        print(f"...publishing {len(solutions_commands)} commands")
        publish_notebook(solutions_commands, solutions_notebook_path, replacements)
            
