# Databricks notebook source
D_TODO = "TODO"
D_ANSWER = "ANSWER"
D_SOURCE_ONLY = "SOURCE_ONLY"
# D_SELF_PACED_ONLY = "SELF_PACED_ONLY"
# D_ILT_ONLY = "ILT_ONLY"

D_INCLUDE_HEADER_TRUE = "INCLUDE_HEADER_TRUE"
D_INCLUDE_HEADER_FALSE = "INCLUDE_HEADER_FALSE"
D_INCLUDE_FOOTER_TRUE = "INCLUDE_FOOTER_TRUE"
D_INCLUDE_FOOTER_FALSE = "INCLUDE_FOOTER_FALSE"

SUPPORTED_DIRECTIVES = [D_SOURCE_ONLY, D_ANSWER, D_TODO, 
                        # D_SELF_PACED_ONLY, D_ILT_ONLY, 
                        D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, ]
 

class NotebookDef:
    def __init__(self, source_dir:str, target_dir:str, path:str, replacements:dict, include_solution:bool):
      assert type(source_dir) == str, f"""Expected the parameter "source_dir" to be of type "str", found "{type(source_dir)}" """
      assert type(target_dir) == str, f"""Expected the parameter "target_dir" to be of type "str", found "{type(target_dir)}" """
      assert type(path) == str, f"""Expected the parameter "path" to be of type "str", found "{type(path)}" """

      assert type(replacements) == dict, f"""Expected the parameter "replacements" to be of type "dict", found "{type(notebook)}" """
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
    
    def publish(self):
      publish(self.source_dir, self.target_dir, self.path, self.replacements, self.include_solution)
     

class Publisher:
    def __init__(self, client, version:str, source_dir:str, target_dir:str, include_solutions:bool=False):
        self.client = client
        self.version = version
        self.version_info_notebook_name = "Version Info"

        self.source_dir = source_dir
        self.target_dir = target_dir
        
        self.include_solutions = include_solutions
        self.notebooks = []
        
    def add(self, path, replacements:dict = None, include_solution = None):
      from datetime import datetime

      # Configure our various default values.
      include_solution = self.include_solutions if include_solution is None else include_solution
      replacements = dict() if replacements is None else replacements
      
      notebook = NotebookDef(self.source_dir, self.target_dir, path, replacements, include_solution)
      
      # Add the universal replacements
      notebook.replacements["{{verison_number}}"] = self.version
      notebook.replacements["{{built_on}}"] = datetime.now().strftime("%b %-d, %Y at %H:%M:%S UTC")

      self.add_notebook_def(notebook)
        
    def add_path(self, path, replacements:dict = None, include_solution = None):
      self.add(path, replacements, include_solution)
        
    def add_notebook_def(self, notebook):
      assert type(notebook) == NotebookDef, f"""Expected the parameter "notebook" to be of type "NotebookDef", found "{type(notebook)}" """

      self.notebooks.append(notebook)
        
    def publish(self, testing, mode=None):
      version_info_notebook = None
      main_notebooks = []
      
      for notebook in self.notebooks:
        if notebook.path == self.version_info_notebook_name: version_info_notebook = notebook
        else: main_notebooks.append(notebook)
          
      assert version_info_notebook is not None, f"""The required notebook "{self.version_info_notebook_name}" was not found."""

      # Backup the version info in case we are just testing
      try: version_info_source = self.client.workspace().export_notebook(f"{version_info_notebook.target_dir}/{version_info_notebook.path}")
      except:
        version_info_source = None
        print("**** version_info_source was not found ****")

      # Now that we backed up the version-info, we can delete everything.
      if self.client.workspace().get_status(self.target_dir) is None:
        pass # Who care, it doesn't already exist.
      elif str(mode).lower() == "delete":
        self.client.workspace().delete_path(self.target_dir)
      elif str(mode).lower() != "overwrite":
        raise Exception("Expected mode to be one of None, DELETE or OVERWRITE")

      # Determine if we are in test mode or not.
      try: testing = version_info_source is not None and testing
      except: testing = False
      
      for notebook in main_notebooks:
        notebook.publish()
        
      if testing:
        print("-"*80) # We are in test-mode, write back the original Version Info notebook
        print(f"RESTORING: {self.target_dir}/Version Info")
        self.client.workspace().import_notebook("PYTHON", f"{self.target_dir}/Version Info", version_info_source)
      else:
        version_info_notebook.publish()


def replace_contents(contents:str, replacements:dict):
    for old_value in replacements:
        new_value = replacements[old_value]
        contents = contents.replace(old_value, new_value)
  
    return contents
    
def get_leading_comments(source_language, command) -> []:
    leading_comments = []
    lines = command.split("\n")

    if source_language == "python":
      lead = "#"
    elif source_language == "sql":
      lead = "--"
    elif source_language == "r":
      lead = "#"
    elif source_language == "scala":
      lead = "//"
    else:
      raise Exception(f"The language {source_language} is not supported.")

    is_md_cell =  lines[0].lower().startswith(f"{lead} magic %md")
    is_sql_cell = lines[0].lower().startswith(f"{lead} magic %sql")

    mark = "--" if is_md_cell or is_sql_cell else lead

    for line in lines:
        if line.startswith("# MAGIC"):
          line = line[7:].strip()
        elif line.startswith("# COMMAND"):
          line = line[9:].strip()
        
        if line.strip().startswith("%"):
            # Remove the magic command from this line
            pos = line.find(" ")
            if pos != -1 and line[pos:].strip().startswith(mark):
              # append to our list
              comment = line[pos:].strip()[len(mark):].strip()
              leading_comments.append(comment)
              
        elif line.strip() == mark:
            # empty comment line, don't break, just ignore
            pass
            
        elif line.strip().startswith(mark):
            # append to our list
            comment = line.strip()[len(mark):].strip()
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

      if directive in [D_TODO, D_ANSWER, D_SOURCE_ONLY, D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE]:
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

cmd_delim = "\n# COMMAND ----------\n"

header_cell = """# MAGIC
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>"""

footer_cell = f"""# MAGIC %md-sandbox
# MAGIC &copy; {date.today().year} Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>"""

def publish_notebook(commands:list, target_path:str, replacements:dict = None) -> None:
    replacements = dict() if replacements is None else replacements
    final_source = "# Databricks notebook source\n"

    # Processes all commands except the last
    for command in commands[:-1]:
        final_source += command
        final_source += cmd_delim

    # Process the last command
    final_source += commands[-1]
    final_source += "" if commands[-1].startswith("# MAGIC") else "\n\n"

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
        if (i==0) and line.strip().replace(" ", "") not in ["#TODO", "##TODO","%sql","#MAGIC%sql"]:
            raise Exception(f"""Expected line #{i+1} in Cmd #{cmd+1} to be the {D_TODO} directive: "{line}"\n{"-"*80}\n{command}\n{"-"*80}""")
        elif not line.startswith("#") and line.strip() != "":
            raise Exception(f"""Expected line #{i+1} in Cmd #{cmd+1} to be commented out: "{line}" """)
        elif i==0 or line.strip() == "":
            # No comment, do not process
            new_command += line
        elif line.strip().startswith("# "):
            # Remove comment and space
            new_command += line[2:]
        else:
            # Remove just the comment
            new_command += line[1:]
        
        if i < len(lines)-1:
            # Add new line for all but the last line
            new_command += "\n"

    
    return new_command

def publish(source_project:str, target_project:str, notebook_name:str, replacements:dict = {}, include_solution=False, ) -> None:
    import re

    print("-" * 80)

    source_notebook_path = f"{source_project}/{notebook_name}"
    print(source_notebook_path)

    client = DBAcademyRestClient()

    source_info = client.workspace().get_status(source_notebook_path) 
    source_language = source_info["language"].lower()

    raw_source = client.workspace().export_notebook(source_notebook_path)

    skipped = 0
    students_commands = []
    solutions_commands = []
    commands = raw_source.split(cmd_delim)

    todo_count = 0
    answ_count = 0

    include_header = False
    found_header_directive = False

    include_footer = False
    found_footer_directive = False
    
    for i in range(len(commands)):
        command = commands[i].lstrip()
        leading_comments = get_leading_comments(source_language, command.strip())
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
        elif D_INCLUDE_HEADER_TRUE in directives:  skipped += skipping(i, "Including Header")
        elif D_INCLUDE_HEADER_FALSE in directives: skipped += skipping(i, "Excluding Header")
        elif D_INCLUDE_FOOTER_TRUE in directives:  skipped += skipping(i, "Including Footer")
        elif D_INCLUDE_FOOTER_FALSE in directives: skipped += skipping(i, "Excluding Footer")

        elif D_TODO in directives:
            # This is a TODO cell, exclude from solution notebooks
            todo_count += 1
            command = clean_todo_cell(command, i)
            students_commands.append(command)

        elif D_ANSWER in directives:
            # This is an ANSWER cell, exclude from lab notebooks
            answ_count += 1
            solutions_commands.append(command)

        else:
            # Not a TODO or ANSWER, just append to both
            students_commands.append(command)
            solutions_commands.append(command)

        # Check the command for BDC markers
        bdc_tokens = ["IPYTHON_ONLY","DATABRICKS_ONLY",
                      "AMAZON_ONLY","AZURE_ONLY","TEST","PRIVATE_TEST","INSTRUCTOR_NOTE","INSTRUCTOR_ONLY",
                      "SCALA_ONLY","PYTHON_ONLY","SQL_ONLY","R_ONLY"
                      "VIDEO","ILT_ONLY","SELF_PACED_ONLY","INLINE","NEW_PART", "{dbr}"]

        for token in bdc_tokens:
            assert token not in command, f"""Found the token "{token}" in command #{i+1}"""

        if source_language == "python":
            assert "%python" not in command, f"""Found "%python" in command #{i+1}"""
        elif source_language == "sql":
            assert "%sql" not in command, f"""Found "%sql" in command #{i+1}"""
        # elif source_language == "scala":
        #     assert "%scala" not in command, f"""Found "%scala" in command #{i+1}"""
        else:
          raise Exception(f"The source notebook language {source_language} is not supported")

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
    replacements[":BESTPRACTICE:"] = """<img src="https://files.training.databricks.com/images/icon_best_24.png"/>"""
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
            
def update_and_validate_git_branch(client, path, target_branch="published"):
  repo_id = client.workspace().get_status(path)["object_id"]

  a_repo = client.repos().get(repo_id)
  a_branch = a_repo["branch"]
  assert a_branch == target_branch, f"""Expected the branch to be "{target_branch}", found "{a_branch}" """

  b_repo = client.repos().update(repo_id, target_branch)
  b_branch = b_repo["branch"]

  print(f"""Path:   {path}""")
  print(f"""Before: {a_repo["branch"]}  |  {a_repo["head_commit_id"]}""")
  print(f"""After:  {b_repo["branch"]}  |  {b_repo["head_commit_id"]}""")