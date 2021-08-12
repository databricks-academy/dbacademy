# Databricks notebook source
D_TODO = "TODO"
D_ANSWER = "ANSWER"
D_SOURCE_ONLY = "SOURCE_ONLY"
D_SELF_PACED_ONLY = "SELF_PACED_ONLY"
D_ILT_ONLY = "ILT_ONLY"

D_INCLUDE_HEADER_TRUE = "INCLUDE_HEADER_TRUE"
D_INCLUDE_HEADER_FALSE = "INCLUDE_HEADER_FALSE"
D_INCLUDE_FOOTER_TRUE = "INCLUDE_FOOTER_TRUE"
D_INCLUDE_FOOTER_FALSE = "INCLUDE_FOOTER_FALSE"

# Only for backwards compatibility!
D_INSTRUCTOR_NOTE = "INSTRUCTOR_NOTE" 

SUPPORTED_DIRECTIVES = [D_SOURCE_ONLY, D_ANSWER, D_TODO, D_SELF_PACED_ONLY, D_ILT_ONLY, 
                        D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, ]

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
              
        elif line.strip() == "#":
            # empty comment line, don't break, just ignore
            pass
            
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
    if line == line.upper():
      # The comment is in all upper case,
      # must be one or more directives
      directive = line.strip()

      if directive in [D_TODO, D_ANSWER, D_SOURCE_ONLY, D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, D_INSTRUCTOR_NOTE]:
          directives.append(line)
        
      elif D_TODO in directives:
          # This is already a TODO cell, no point
          # in trying to process this any further.
          pass
    
      else:
          print(f"""Processing "{directive}" in Cmd #{i+1} """)
          if " " in directive: 
            raise ValueError(f"""Whitespace found in directive "{directive}", Cmd #{i+1}: {line}""")
          if "-" in directive: 
            raise ValueError(f"""Hyphen found in directive "{directive}", Cmd #{i+1}: {line}""")
          if directive not in SUPPORTED_DIRECTIVES: 
            raise ValueError(f"""Unspported directive "{directive}" in Cmd #{i+1} {SUPPORTED_DIRECTIVES}: {line}""")
          directives.append(line)

  return directives

# COMMAND ----------

from datetime import date
from dbacademy.dbrest import DBAcademyRestClient

found_setup = False
cmd_delim = "\n# COMMAND ----------\n"

header_cell = """# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>"""

footer_cell = f"""
# MAGIC %md-sandbox
# MAGIC &copy; {date.today().year} Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>"""

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
    
def skipping(i, label):
    print(f"Skipping Cmd #{i + 1} - {label}")
    return 1;

def clean_todo_cell(command, cmd):
    new_command = ""
    lines = command.split("\n")
    
    for i in range(len(lines)):
        line = lines[i]
        if i==0 and line.strip().replace(" ", "") not in ["#TODO", "##TODO"]:
            raise Exception(f"""Expected line #1 in Cmd #{cmd+1} to be the {D_TODO} directive: "{line}" """)
        elif not line.startswith("#") and line.strip() != "":
            raise Exception(f"""Expected line #{i+1} in Cmd #{cmd+1} to be commented out: "{line}" """)
        elif i==0 or line.strip() == "":
            # No comment, do not process
            new_command += line
            new_command += "\n"
        elif line.strip().startswith("# "):
            # Remove comment and space
            new_command += line[2:]
            new_command += "\n"
        else:
            # Remove just the comment
            new_command += line[1:]
            new_command += "\n"
    
    return new_command

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

    include_header = False
    found_header_directive = False

    include_footer = False
    found_footer_directive = False
    
    for i in range(len(commands)):
        command = commands[i].strip()
        leading_comments = get_leading_comments(command)
        directives = parse_directives(i, leading_comments)

        # Print statements for debugging parsing.
        # print(f"\nCommand {i}")
        # if len(leading_comments) > 0:
        #     print("   |-LEADING COMMENTS --"+("-"*57))
        #     for comment in leading_comments:
        #         print("   |"+comment)
        # if len(directives) > 0:
        #     print("   |-DIRECTIVES --"+("-"*62))
        #     for directive in directives:
        #         print("   |"+directive)
        
        include_header = True if D_INCLUDE_HEADER_TRUE in directives else include_header
        found_header_directive = True if D_INCLUDE_HEADER_TRUE in directives or D_INCLUDE_HEADER_FALSE in directives else found_header_directive

        include_footer = True if D_INCLUDE_FOOTER_TRUE in directives else include_footer
        found_footer_directive = True if D_INCLUDE_FOOTER_TRUE in directives or D_INCLUDE_FOOTER_FALSE in directives else found_footer_directive

        if command.strip() == "":                  skipped += skipping(i, "Empty Cell")
        elif D_SOURCE_ONLY in directives:          skipped += skipping(i, "Source-Only")
        elif D_INSTRUCTOR_NOTE in directives:      skipped += skipping(i, "Instructor Note")
        elif D_INCLUDE_HEADER_TRUE in directives:  skipped += skipping(i, "Including Header")
        elif D_INCLUDE_HEADER_FALSE in directives: skipped += skipping(i, "Excluding Header")
        elif D_INCLUDE_FOOTER_TRUE in directives:  skipped += skipping(i, "Including Footer")
        elif D_INCLUDE_FOOTER_FALSE in directives: skipped += skipping(i, "Excluding Footer")

        elif D_TODO in directives:
            # This is a TODO cell, exclude from solution notebooks
            todo_count += 1
            assert_only_one_setup_cell(command, i)
            command = clean_todo_cell(command, i)
            students_commands.append(command)

        elif D_ANSWER in directives:
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
        bdc_tokens = ["%python","IPYTHON_ONLY","DATABRICKS_ONLY","SCALA_ONLY","PYTHON_ONLY","SQL_ONLY","R_ONLY",
                      "AMAZON_ONLY","AZURE_ONLY","TEST","PRIVATE_TEST",
                      "VIDEO","INSTRUCTOR_ONLY","ILT_ONLY","SELF_PACED_ONLY","INLINE","NEW_PART","INSTRUCTOR_NOTE",
                      ":BESTPRACTICE:", "{{dbr}}"]

        for token in bdc_tokens:
            assert token not in command, f"Found {token} in command #{i+1}"

    assert found_header_directive, f"One of the two header directives ({D_INCLUDE_HEADER_TRUE} or {D_INCLUDE_HEADER_FALSE}) were not found."
    assert found_footer_directive, f"One of the two footer directives ({D_INCLUDE_FOOTER_TRUE} or {D_INCLUDE_FOOTER_FALSE}) were not found."
    assert answ_count >= todo_count, f"Found more {D_TODO} commands ({todo_count}) than {D_ANSWER} commands ({answ_count})"

    if include_header is True:
        students_commands.insert(0, header_cell)
        solutions_commands.insert(0, header_cell)
    
    if include_footer is True:
        students_commands.append(footer_cell)
        solutions_commands.append(footer_cell)
    
    # Augment the replacements to duplicate the :NOTE:, :CAUTION:, etc features from BDC
    replacements[":HINT:"] =         """<img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp;**Hint:**"""
    replacements[":CAUTION:"] =      """<img src="https://files.training.databricks.com/images/icon_warn_24.png"/>"""
    # replacements[":BESTPRACTICE:"] = """<img src="https://files.training.databricks.com/images/icon_best_24.png"/>"""
    replacements[":SIDENOTE:"] =     """<img src="https://files.training.databricks.com/images/icon_note_24.png"/>"""

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
            
