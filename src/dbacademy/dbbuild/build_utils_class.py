from typing import Union, List, Dict, Optional
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy import dbgems


class ChangeLog:
    def __init__(self):
        self.entries = []
        self.version: Optional[str] = None
        self.date: Optional[str] = None

    def print(self):
        print(f"Change Log: v{self.version} ({self.date})")
        for entry in self.entries:
            print(f"  {entry}")


class BuildUtils:
    CHANGE_LOG_TAG = "## Change Log"
    CHANGE_LOG_VERSION = "### Version "

    def __init__(self):
        pass

    @staticmethod
    def to_job_url(*, job_id: str, run_id: str):
        return f"{dbgems.get_workspace_url()}#job/{job_id}/run/{run_id}"

    @staticmethod
    def validate_type(actual_value, name, expected_type):
        assert type(actual_value) == expected_type, f"Expected the parameter {name} to be of type {expected_type}, found {type(actual_value)}"
        return actual_value

    @staticmethod
    def print_if(condition, text):
        if condition:
            print(text)

    @staticmethod
    def clean_target_dir(client, target_dir: str, verbose):
        from dbacademy.dbbuild import Publisher

        if verbose: print(f"Cleaning {target_dir}...")

        keepers = [f"{target_dir}/{k}" for k in Publisher.KEEPERS]

        for path in [p.get("path") for p in client.workspace.ls(target_dir) if p.get("path") not in keepers]:
            if verbose: print(f"...{path}")
            client.workspace().delete_path(path)

    # noinspection PyUnusedLocal
    @staticmethod
    def write_file(*, data: bytearray, target_file: str, overwrite: bool, target_name):
        import os
        print(f"\nWriting DBC to {target_name}:\n   {target_file}")

        target_file = target_file.replace("dbfs:/", "/dbfs/")

        if os.path.exists(target_file):
            # assert overwrite, f"Cannot overwrite existing file: {target_file}"
            # print(f"Removing existing file: {target_file}")
            os.remove(target_file)

        course_dir = "/".join(target_file.split("/")[:-2])
        if not os.path.exists(course_dir): os.mkdir(course_dir)

        version_dir = "/".join(target_file.split("/")[:-1])
        if not os.path.exists(version_dir): os.mkdir(version_dir)

        with open(target_file, "wb") as f:
            # print(f"Writing data: {target_file}")
            f.write(data)

    @staticmethod
    def reset_git_repo(*, client: DBAcademyRestClient, directory: str, repo_url: str, branch: str, which: Union[str, None]):

        which = "" if which is None else f" ({which})"

        print(f"Resetting git repo{which}:")
        print(f" - Branch:   \"{branch}\"")
        print(f" - Directory: {directory}")
        print(f" - Repo URL:  {repo_url}")
        print()

        status = client.workspace().get_status(directory)

        if status is not None:
            target_repo_id = status["object_id"]
            client.repos().delete(target_repo_id)

        # Re-create the repo to progress in testing
        response = client.repos.create(path=directory, url=repo_url)
        repo_id = response.get("id")

        actual_branch = response.get("branch")
        if actual_branch != branch:
            if actual_branch != "published": print(f"\n*** Unexpected branch: {actual_branch}, expected {branch} ***\n")
            client.repos.update(repo_id=repo_id, branch=branch)

        results = client.repos.get(repo_id)
        current_branch = results.get("branch")

        assert branch == current_branch, f"Expected the new branch to be {branch}, found {current_branch}"

    @staticmethod
    def validate_no_changes_in_repo(*, client: DBAcademyRestClient, build_name: str, repo_url: str, directory: str) -> List[str]:
        # results = BuildUtils.validate_not_uncommitted(client=client,
        #                                               build_name=build_name,
        #                                               repo_url=repo_url,
        #                                               directory=directory,
        #                                               ignored=["/Published/", "/Build-Scripts/"])
        repo_dir = f"/Repos/Temp/{build_name}-diff"
        ignored = ["/Published/", "/Build-Scripts/"]

        print(f"Comparing {directory}")
        print(f"to        {repo_dir}")
        print()

        BuildUtils.reset_git_repo(client=client,
                                  directory=repo_dir,
                                  repo_url=repo_url,
                                  branch="published",
                                  which="diff")

        index_a: Dict[str, Dict[str, str]] = BuildUtils.index_repo_dir(client=client, repo_dir=repo_dir, ignored=ignored)
        index_b: Dict[str, Dict[str, str]] = BuildUtils.index_repo_dir(client=client, repo_dir=directory, ignored=ignored)

        results = BuildUtils.compare_results(index_a, index_b)

        if len(results) != 0:
            print()
            for result in results:
                print(result)
        else:
            print(f"\nPASSED: No changes were found!")

        return results

    @staticmethod
    def __ends_with(test_path: str, values: List[str]):
        for ext in values:
            if test_path.endswith(ext): return True
        return False

    @staticmethod
    def __starts_with(test_path: str, values: List[str]):
        for ext in values:
            if test_path.startswith(ext): return True
        return False

    @staticmethod
    def index_repo_dir(*, client: DBAcademyRestClient, repo_dir: str, ignored: List[str]) -> Dict[str, Dict[str, str]]:
        import os
        from typing import Dict

        start = dbgems.clock_start()
        print(f"Indexing \"{repo_dir}\"", end="...")
        notebooks = client.workspace().ls(repo_dir, recursive=True)
        assert notebooks is not None, f"No notebooks found for the path {repo_dir}"

        results: Dict[str, Dict[str, str]] = {}
        base_path = f"/Workspace/{repo_dir}"

        for path, dirs, files in os.walk(base_path):
            for file in files:
                full_path = f"{path}/{file}"
                relative_path = full_path[len(base_path):]
                if not BuildUtils.__starts_with(relative_path, ignored):
                    results[relative_path] = {
                        "full_path": full_path,
                        "contents": None
                    }
        print(dbgems.clock_stopped(start))
        return BuildUtils.load_sources(client=client, results=results)

    @staticmethod
    def load_sources(*, client: DBAcademyRestClient, results: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        for path in results:
            full_path = results.get(path).get("full_path")

            if BuildUtils.__ends_with(full_path, [".ico"]):
                # These are binary files
                contents = ""
            elif BuildUtils.__ends_with(full_path, [".json", ".txt", ".html", ".md", ".gitignore", "LICENSE"]):
                # These are text files that we can just read in
                with open(full_path) as f:
                    contents = f.read()
            else:
                # These are notebooks
                try:
                    notebook_path = full_path[10:] if full_path.startswith("/Workspace/") else full_path
                    contents = client.workspace.export_notebook(notebook_path)
                except Exception as e:
                    contents = ""
                    print("*" * 80)
                    print("* Failed to export notebook, possibly unanticipated file type ***")
                    print(f"* {full_path}")
                    for line in str(e).split("\n"):
                        print(f"* {line}")
                    print("*" * 80)

            results[path]["contents"] = contents

        return results

    @staticmethod
    def compare_results(index_a: Dict[str, Dict[str, str]], index_b: Dict[str, Dict[str, str]]):
        results = []

        index_b_notebooks = list(index_b.keys())

        for relative_path_a in index_a:
            if relative_path_a not in index_b_notebooks:
                results.append(f"Notebook deleted: `{relative_path_a}`")

        for relative_path_b in index_b_notebooks:
            if relative_path_b not in index_a:
                results.append(f"Notebook added: `{relative_path_b}`")

        for relative_path in index_a:
            if relative_path in index_b:
                source_a = index_a[relative_path]["contents"]
                source_b = index_b[relative_path]["contents"]

                len_a = len(source_a)
                len_b = len(source_b)
                if source_a != source_b:
                    label = f"{len_a:,} vs {len_b:,}:"
                    results.append(f"Differences: {label:>20} {relative_path}")

        return results

    @staticmethod
    def load_change_log(source_repo: str, target_version: Optional[str]) -> ChangeLog:
        import os

        readme_path = f"/Workspace/{source_repo}/README.md"
        assert os.path.exists(readme_path), f"The README.md file was not found at {readme_path}"

        with open(readme_path, "r") as f:
            lines = f.readlines()

        change_log = ChangeLog()
        change_log_index: Optional[int] = None
        version_index: Optional[int] = None

        for i, line in enumerate(lines):
            line = line.strip()
            if line == BuildUtils.CHANGE_LOG_TAG:
                change_log_index = i

            elif change_log_index and i > change_log_index and line == "":
                pass  # Just an empty line

            elif change_log_index and i > change_log_index and not version_index:
                if line.strip().startswith("* "):
                    continue

                assert line.startswith(BuildUtils.CHANGE_LOG_VERSION), f"The next change log entry ({BuildUtils.CHANGE_LOG_VERSION}...) was not found at {readme_path}:{i + 1}\n{line}"

                parts = line.split(" ")  # "### Version 1.0.2 (01-21-2022)"
                assert len(parts) == 4, f"Expected the change log entry to contain 4 parts and of the form \"### Version vN.N.N (M-D-YYYY)\", found \"{line}\"."
                assert parts[0] == "###", f"Part 1 of the change long entry is not \"###\", found \"{parts[0]}\""
                assert parts[1] == "Version", f"Part 2 of the change long entry is not \"Version\", found \"{parts[1]}\""

                change_log.version = parts[2]

                v_parts = change_log.version.split(".")
                assert len(v_parts) == 3, f"The change long entry's version field is not of the form \"vN.N.N\" where \"N\" is an integral value, found {len(v_parts)} parts: \"{change_log.version}\"."
                assert v_parts[0].isnumeric(), f"The change long entry's Major version field is not an integral value, found \"{change_log.version}\"."
                assert v_parts[1].isnumeric(), f"The change long entry's Minor version field is not an integral value, found \"{change_log.version}\"."
                assert v_parts[2].isnumeric(), f"The change long entry's Bug-Fix version field is not an integral value, found \"{change_log.version}\"."

                if target_version is None: version_index = i                  # Use the first one we find.
                elif target_version == change_log.version: version_index = i  # We found the target version.
                else: continue

                change_log.date = parts[3]
                assert change_log.date.startswith("(") and change_log.date.endswith(")"), f"Expected the change log entry's date field to be of the form \"(M-D-YYYY)\" or \"(TBD)\", found \"{change_log.date}\" for version \"{change_log.version}\"."

                change_log.date = change_log.date[1:-1]
                if change_log.date != "TBD":
                    d_parts = change_log.date.split("-")
                    assert len(d_parts) == 3, f"The change long entry's date field is not of the form \"(M-D-YYYY)\", found {change_log.date}\" for version \"{change_log.version}\"."
                    assert d_parts[0].isnumeric(), f"The change long entry's month field is not an integral value, found \"{change_log.date}\" for version \"{change_log.version}\"."
                    assert d_parts[1].isnumeric(), f"The change long entry's day field is not an integral value, found \"{change_log.date}\" for version \"{change_log.version}\"."
                    assert d_parts[2].isnumeric(), f"The change long entry's year field is not an integral value, found \"{change_log.date}\" for version \"{change_log.version}\"."

            elif version_index and i > version_index and not line.startswith("#"):
                change_log.entries.append(line)

            elif version_index and i > version_index and line.startswith("#"):
                change_log.print()
                return change_log

        assert len(change_log.entries) > 0, f"The Change Log section was not found in {readme_path}"
        return change_log

    @staticmethod
    def create_published_message(*, name: str, version: str, change_log: ChangeLog, publishing_info: dict, source_repo: str):
        import urllib.parse

        core_message = f"Change Log: v{change_log.version} ({change_log.date})"
        for entry in change_log.entries:
            core_message += f"  {entry}"

        core_message += f"""
Release notes, course-specific requirements, issue-tracking, and test results for this course can be found in the course's GitHub repository at https://github.com/databricks-academy/{source_repo.split("/")[-1]}

Please contact me (via Slack), or anyone on the curriculum team should you have any questions."""

        email_body = urllib.parse.quote(core_message, safe="")
        slack_message = f"""@channel Published {name}, v{version}\n\n{core_message.strip()}"""

        content = "<div>"
        for group_name, group in publishing_info.items():
            content += f"""<div style="margin-bottom:1em">"""
            content += f"""<div style="font-size:16px;">{group_name}</div>"""
            for link_name, url in group.items():
                if url == "mailto:curriculum-announcements@databricks.com": url += f"?subject=Published {name}, v{version}&body={email_body}"
                content += f"""<li><a href="{url}" target="_blank" style="font-size:16px">{link_name}</a></li>"""
            content += "</div>"
        content += "</div>"

        rows = len(slack_message.split("\n"))+1
        html = f"""
        <body>
            {content}
            <textarea style="width:100%; padding:1em" rows={rows}>{slack_message}</textarea>
        </body>"""
        dbgems.display_html(html)
