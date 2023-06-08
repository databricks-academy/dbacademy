from typing import Dict


class DocsPublisher:
    import io
    from dbacademy.dbbuild.publish.publishing_info_class import Translation
    from dbacademy.google.google_client_class import GoogleClient

    def __init__(self, build_name: str, version: str, translation: Translation):
        from dbacademy.google.google_client_class import GoogleClient

        self.__translation = translation
        self.__build_name = build_name
        self.__version = version
        self.__pdfs = dict()
        self.__google_client = GoogleClient()

    @property
    def google_client(self) -> GoogleClient:
        return self.__google_client

    @property
    def translation(self) -> Translation:
        return self.__translation

    @property
    def build_name(self) -> str:
        return self.__build_name

    @property
    def version(self) -> str:
        return self.__version

    def get_distribution_path(self, *, version: str, file: Dict[str, str]) -> str:
        file_name = self.__google_client.to_file_name(file)
        return f"/dbfs/mnt/resources.training.databricks.com/distributions/{self.build_name}/v{version}-PENDING/{file_name}"

    def __download_google_doc(self, *, index: int, total: int, gdoc_id: str = None, gdoc_url: str = None) -> (str, str):
        from dbacademy import dbgems

        gdoc_id = self.google_client.to_gdoc_id(gdoc_id=gdoc_id, gdoc_url=gdoc_url)
        file = self.google_client.file_get(gdoc_id)
        name = file.get("name")
        file_name = self.google_client.to_file_name(file)

        print(f"| Processing {index + 1} of {total}: {name}")

        file_bytes = self.google_client.file_export(gdoc_id)

        self.__save_pdfs(file_bytes, f"/dbfs/FileStore/tmp/{file_name}")
        self.__save_pdfs(file_bytes, self.get_distribution_path(version=self.version, file=file))

        parts = dbgems.get_workspace_url().split("/")
        del parts[-1]
        workspace_url = "/".join(parts)

        return file_name, f"{workspace_url}/files/tmp/{file_name}"

    @staticmethod
    def __save_pdfs(file_bytes: io.BytesIO, path: str):
        import os
        import shutil

        if os.path.exists(path):
            os.remove(path)

        target_dir = "/".join(path.split("/")[:-1])
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        file_bytes.seek(0)
        with open(path, "wb") as f:
            print(f"| writing {path}")
            shutil.copyfileobj(file_bytes, f)

    def process_pdfs(self) -> None:
        from dbacademy import common
        from dbacademy.google.google_client_class import GoogleClientException

        print("Exporting Google Docs as PDFs:")
        total = len(self.translation.document_links)

        for index, link in enumerate(self.translation.document_links):
            error_message = f"Document {index + 1} of {total} cannot be downloaded; publishing of this doc is being skipped.\n{link}"
            try:
                file_name, file_url = self.__download_google_doc(index=index, total=total, gdoc_url=link)
                self.__pdfs[file_name] = file_url

            except GoogleClientException as e:
                common.print_warning("SKIPPING DOWNLOAD", f"{error_message}\n{e.message}")

            if index < len(self.translation.document_links)-1:
                print("|")

    def process_google_slides(self) -> None:

        print("Publishing Google Docs:")

        parent_folder_id = self.translation.published_docs_folder.split("/")[-1]
        files = self.__google_client.folder_list(folder_id=parent_folder_id)
        folders = [f for f in files if f.get("name") == f"v{self.version}"]

        for folder in folders:
            folder_id = folder.get("id")
            folder_name = folder.get("name")
            self.__google_client.folder_delete(folder_id)
            print(f"| Deleted existing published folder {folder_name} (https://drive.google.com/drive/folders/{folder_id})")

        folder = self.__google_client.folder_create(parent_folder_id=parent_folder_id, folder_name=f"v{self.version}")

        folder_id = folder.get("id")
        folder_name = folder.get("name")
        print(f"| Created new published folder {folder_name} (https://drive.google.com/drive/folders/{folder_id})")

        total = len(self.translation.document_links)
        for index, link in enumerate(self.translation.document_links):
            gdoc_id = self.google_client.to_gdoc_id(gdoc_url=link)
            file = self.google_client.file_get(file_id=gdoc_id)
            name = file.get("name")

            print(f"| Copying {index + 1} of {total}: {name} ({link})")
            self.google_client.file_copy(file_id=gdoc_id, name=name, parent_folder_id=folder_id)

    def to_html(self) -> str:
        html = """<html><body style="font-size:16px">"""

        html += "\nDownloads<ul>"
        for file_name, file_url in self.__pdfs.items():
            html += f"""\n<li><a href="{file_url}">Download {file_name}</a></li>"""
        html += "\n</ul>"

        html += "\nTools/Docs<ul>"
        html += f"""\n<li><a href="{self.translation.published_docs_folder}" target="_blank">Published Folder</a></li>"""

        links = self.translation.document_links

        if len(links) == 0:
            html += "\n<li>Documents: None</li>"
        elif len(links) == 1:
            html += f"""\n<li><a href="{links[0]}" target="_blank">Document</a></li>"""
        else:
            html += """\n<li>Documents:</li>"""
            html += """\n<ul style="margin:0">"""
            for i, link in enumerate(links):
                html += f"""\n<li><a href="{link}" target="_blank">Document #{i + 1}</a></li>"""
            html += "\n</ul>"

        html += """\n</body></html>"""
        return html
