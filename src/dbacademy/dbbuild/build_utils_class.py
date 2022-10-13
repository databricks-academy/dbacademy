from typing import Union, List, Dict
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy import dbgems


class BuildUtils:

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
    def validate_not_uncommitted(*, client: DBAcademyRestClient, build_name: str, repo_url: str, directory: str, ignored: List[str]):
        from typing import Dict

        repo_dir = f"/Repos/Temp/{build_name}-diff"

        print(f"Comparing {directory}")
        print(f"to        {repo_dir}")
        print()

        BuildUtils.reset_git_repo(client=client,
                                  directory=repo_dir,
                                  repo_url=repo_url,
                                  branch="published",
                                  which="fresh")

        index_a: Dict[str, Dict[str, str]] = BuildUtils.index_repo_dir(client=client, repo_dir=repo_dir, ignored=ignored)
        index_b: Dict[str, Dict[str, str]] = BuildUtils.index_repo_dir(client=client, repo_dir=directory, ignored=ignored)

        return BuildUtils.compare_results(index_a, index_b)

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

        print(f"...indexing \"{repo_dir}\"")
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
