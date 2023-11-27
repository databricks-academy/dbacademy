__all__ = ["BuildUtils"]

from typing import Union, List, Dict
from dbacademy.clients.dbrest import DBAcademyRestClient


class BuildUtils:

    def __init__(self):
        pass

    @classmethod
    def to_job_url(cls, *, job_id: str, run_id: str) -> str:
        from dbacademy import dbgems
        return f"{dbgems.get_workspace_url()}#job/{job_id}/run/{run_id}"

    @classmethod
    def print_if(cls, condition, text) -> None:
        if condition:
            print(text)

    @classmethod
    def clean_target_dir(cls, client: DBAcademyRestClient, target_dir: str, verbose: bool) -> None:
        from dbacademy.dbbuild.publish.publisher import Publisher

        if verbose:
            print(f"Cleaning {target_dir}...")

        keepers = [f"{target_dir}/{k}" for k in Publisher.KEEPERS]

        for path in [p.get("path") for p in client.workspace.ls(target_dir) if p.get("path") not in keepers]:
            if verbose:
                print(f"...{path}")
            client.workspace.delete_path(path, recursive=True)

    # noinspection PyUnusedLocal
    @classmethod
    def write_file(cls, *, data: bytes, target_file: str, overwrite: bool, target_name) -> None:
        import os
        if target_file.endswith("_meta.json"):
            print(f"\nWriting Meta File to {target_name}:\n   {target_file}")
        else:
            print(f"\nWriting DBC to {target_name}:\n   {target_file}")

        target_file = target_file.replace("dbfs:/", "/dbfs/")

        if os.path.exists(target_file):
            assert overwrite, f"Cannot overwrite existing file: {target_file}"
            print(f"Removing existing file: {target_file}")
            os.remove(target_file)

        course_dir = "/".join(target_file.split("/")[:-2])
        if not os.path.exists(course_dir):
            os.mkdir(course_dir)

        version_dir = "/".join(target_file.split("/")[:-1])
        if not os.path.exists(version_dir):
            os.mkdir(version_dir)

        with open(target_file, "wb") as f:
            # print(f"Writing data: {target_file}")
            f.write(data)

    @classmethod
    def reset_git_repo(cls, *, client: DBAcademyRestClient, directory: str, repo_url: str, branch: str, which: Union[str, None], prefix="") -> None:

        which = "" if which is None else f" ({which})"

        print(f"{prefix}Resetting git repo{which}:")
        print(f"{prefix}| Branch:   \"{branch}\"")
        print(f"{prefix}| Directory: {directory}")
        print(f"{prefix}| Repo URL:  {repo_url}")

        status = client.workspace.get_status(directory)

        if status is not None:
            target_repo_id = status["object_id"]
            client.repos.delete_by_id(target_repo_id)

        # Re-create the repo to progress in testing
        response = client.repos.create(path=directory, url=repo_url)
        repo_id = response.get("id")

        actual_branch = response.get("branch")
        if actual_branch != branch:
            if actual_branch != "published":
                print(f"\n*** Unexpected branch: {actual_branch}, expected {branch} ***\n")
            client.repos.update(repo_id=repo_id, branch=branch)

        results = client.repos.get(repo_id)
        current_branch = results.get("branch")

        assert branch == current_branch, f"Expected the new branch to be {branch}, found {current_branch}"

    @classmethod
    def validate_no_changes_in_repo(cls, *, client: DBAcademyRestClient, build_name: str, repo_url: str, directory: str) -> List[str]:
        repo_dir = f"/Repos/Temp/{build_name}-diff"

        cls.reset_git_repo(client=client,
                           directory=repo_dir,
                           repo_url=repo_url,
                           branch="published",
                           which="diff")
        print()
        index_a: Dict[str, Dict[str, str]] = cls.index_repo_dir(client=client, repo_dir=repo_dir)
        index_b: Dict[str, Dict[str, str]] = cls.index_repo_dir(client=client, repo_dir=directory)
        print()

        print(f"Comparing {directory}")
        print(f"to        {repo_dir}")

        results = cls.compare_results(index_a, index_b)

        if len(results) != 0:
            print()
            for result in results:
                print(result)
        else:
            print(f"\nPASSED: No changes were found!")

        return results

    @classmethod
    def __ends_with(cls, test_path: str, values: List[str]) -> bool:
        for ext in values:
            if test_path.endswith(ext):
                return True
        return False

    @classmethod
    def __starts_with(cls, test_path: str, values: List[str]) -> bool:
        for ext in values:
            if test_path.startswith(ext):
                return True
        return False

    @classmethod
    def index_repo_dir(cls, *, client: DBAcademyRestClient, repo_dir: str) -> Dict[str, Dict[str, str]]:
        import os
        from dbacademy import dbgems

        ignored = ["/Published/", "/Build-Scripts/", "/Build-Scripts-"]

        start = dbgems.clock_start()
        print(f"Indexing \"{repo_dir}\"", end="...")
        notebooks = client.workspace.ls(repo_dir, recursive=True)
        assert notebooks is not None, f"No notebooks found for the path {repo_dir}"

        results: Dict[str, Dict[str, str]] = {}
        base_path = f"/Workspace/{repo_dir}"

        for path, dirs, files in os.walk(base_path):
            for file in files:
                full_path = f"{path}/{file}"
                relative_path = full_path[len(base_path):]
                if not cls.__starts_with(relative_path, ignored):
                    results[relative_path] = {
                        "full_path": full_path,
                        "contents": None
                    }

        sources = cls.load_sources(client=client, results=results)
        print(dbgems.clock_stopped(start, f", {len(sources)} files"))

        return sources

    @classmethod
    def load_sources(cls, *, client: DBAcademyRestClient, results: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        for path in results:
            full_path = results.get(path).get("full_path")

            if cls.__ends_with(full_path, [".ico"]):
                # These are binary files
                contents = ""
            elif cls.__ends_with(full_path, [".json", ".txt", ".html", ".md", ".gitignore", "LICENSE"]):
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

    @classmethod
    def compare_results(cls, index_a: Dict[str, Dict[str, str]],
                        index_b: Dict[str, Dict[str, str]]) -> List[str]:
        results: List[str] = []

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
