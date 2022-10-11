from dbacademy.rest.common import DatabricksApiException, ApiContainer


class Workspace(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def list(self, workspace_path, sort_key=lambda f: f['path']):
        files = self.databricks.api("GET", "2.0/workspace/list", {"path": workspace_path}).get("objects", [])
        return sorted(files, key=sort_key)

    def list_names(self, workspace_path, sort_key=lambda f: f['path']):
        files = self.list(workspace_path, sort_key=sort_key)
        filenames = [f['path'] + ('/' if f['object_type'] == 'DIRECTORY' else '') for f in files]
        return filenames

    def walk(self, workspace_path, sort_key=lambda f: f['path']):
        """Recursively list files into an iterator.  Sorting within a directory is done by the provided sort_key."""
        for f in self.list(workspace_path, sort_key=sort_key):
            yield f
            if f['object_type'] == 'DIRECTORY':
                yield from self.walk(f['path'])

    def mkdirs(self, workspace_path):
        self.databricks.api("POST", "2.0/workspace/mkdirs", {"path": workspace_path})

    def copy(self, source_path, target_path, *, target_connection=None, if_exists="overwrite", exclude=set(),
             dry_run=False):
        source_connection = self.databricks
        if target_connection is None:
            target_connection = source_connection
        # Strip trailing '/', except for root '/'
        source_path = source_path.rstrip("/")
        target_path = target_path.rstrip("/")
        if not source_path:
            source_path = "/"
        if not target_path:
            target_path = "/"
        if source_path in exclude:
            print("skip", source_path, target_path)
            return
        print("copy", source_path, target_path)
        # Try to copy the entire directory at once
        try:
            bytes = source_connection.workspace.export(workspace_path=source_path, format="DBC").get("content",
                                                                                                     None)
            if bytes and not dry_run:
                target_connection.workspace.import_from_data(workspace_path=target_path, content=bytes,
                                                             format="DBC", if_exists=if_exists)
            return
        except DatabricksApiException as e:
            cause = e
            if "exceeded the limit" in e.message:
                pass
            elif "Subtree size exceeds" in e.message:
                pass
            elif source_path.endswith("/Trash") and "RESOURCE_DOES_NOT_EXIST":
                return  # Skip trash folders
            elif "BAD_REQUEST: Cannot serialize item" in e.message:
                return  # Can't copy MLFow experiments this way.  Skip it.
            elif "BAD_REQUEST: Cannot serialize library" in e.message:
                return  # Can't copy libraries this way.  Skip it.
            else:
                raise e
        # If the size limit was exceeded for a file (not a directory) raise the error
        source_files = source_connection.workspace.list_names(workspace_path=source_path)
        if source_files == [source_path]:
            raise cause
        # If the size limit was exceeded for a directory, copy the directory item by item to break
        # it into smaller chunks
        prefix_len = len(source_path)
        if source_path == "/":
            prefix_len = 0
        target_connection.workspace.mkdirs(target_path)
        for s in sorted(source_files):
            t = target_path + s[prefix_len:]
            self.copy(source_path=s,
                      target_path=t,
                      target_connection=target_connection,
                      if_exists=if_exists,
                      exclude=exclude)

    def compare(self, source_path, target_path, target_connection=None, compare_contents=False):
        """
        Recursively compare the filenames and types, but not contents of the files.
        Returns iterator of files that don't have a match on the other side.

        compare_contents is a no-op currently and doesn't change anything.

        This methods signature and behavior is subject to change.  Maintainers are invited to improve it.
        """
        source_connection = self.databricks
        if target_connection is None:
            target_connection = self
        # Strip training '/', except for root '/'
        source_path = source_path.rstrip("/")
        target_path = target_path.rstrip("/")
        if not source_path:
            source_path = "/"
        if not target_path:
            target_path = "/"
        # Compare the tree contents
        source_iter = source_connection.workspace.walk(source_path)
        target_iter = target_connection.workspace.walk(target_path)
        source_prefix_len = len(source_path)
        target_prefix_len = len(target_path)
        try:
            s = next(source_iter)
            t = next(target_iter)
            while True:
                s_name = s['path'][source_prefix_len:]
                t_name = t['path'][target_prefix_len:]
                if s_name < t_name:
                    yield s, None
                    s = next(source_iter)
                elif s_name > t_name:
                    yield None, t
                    t = next(target_iter)
                else:
                    if s['object_type'] != t['object_type'] or s.get('language', None) != t.get('language', None):
                        yield s, t
                    s = next(source_iter)
                    t = next(target_iter)
        except StopIteration:
            from itertools import zip_longest
            yield from zip_longest(source_iter, [], fillvalue=None)
            yield from zip_longest([], target_iter, fillvalue=None)

    def exists(self, workspace_path):
        try:
            self.list(workspace_path)
            return True
        except DatabricksApiException as e:
            if e.error_code == "RESOURCE_DOES_NOT_EXIST":
                return False
            else:
                raise e

    def is_empty(self, workspace_path):
        files = self.list(workspace_path)
        if len(files) == 1:
            return files[0]['path'].endswith("/Trash")
        else:
            return not files

    def delete(self, workspace_path, recursive=True):
        self.databricks.api("POST", "2.0/workspace/delete", {
            "path": workspace_path,
            "recursive": "true" if recursive else "false"
        })

    @staticmethod
    def read_data_from_url(source_url, format="DBC"):
        import base64
        import requests
        r = requests.get(source_url)
        if format == "DBC":
            content = base64.b64encode(r.content).decode("utf8")
        elif format == "SOURCE":
            content = r.text
        else:
            raise DatabricksApiException("Unknown format.")
        return content

    def import_from_url(self, source_url, workspace_path, format="DBC", *, if_exists="error"):
        content = self.read_data_from_url(source_url)
        self.import_from_data(content, workspace_path, format, if_exists=if_exists)

    def import_from_data(self, content, workspace_path, format="DBC", *, language=None, if_exists="error"):
        data = {
            "content": content,
            "path": workspace_path,
            "format": format,
            "language": language,
        }
        try:
            return self.databricks.api("POST", "2.0/workspace/import", data)
        except DatabricksApiException as e:
            if e.error_code != "RESOURCE_ALREADY_EXISTS":
                raise e
            else:
                if if_exists == "overwrite":
                    self.delete(workspace_path)
                    return self.databricks.api("POST", "2.0/workspace/import", data)
                elif if_exists == "ignore":
                    pass
                elif if_exists == "error":
                    raise e
                else:
                    print("Invalid if_exists: " + if_exists)
                    raise e

    def export(self, workspace_path, format="DBC"):
        data = {
            "path": workspace_path,
            "format": format,
        }
        return self.databricks.api("GET", "2.0/workspace/export", data)
