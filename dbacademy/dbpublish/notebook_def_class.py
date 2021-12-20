from dbacademy.dbpublish import get_cmd_delim, get_leading_comments, parse_directives, D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE, skipping, D_SOURCE_ONLY, D_TODO, clean_todo_cell, D_ANSWER, D_DUMMY, get_header_cell, get_footer_cell, publish_notebook
from dbacademy.dbrest import DBAcademyRestClient


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
