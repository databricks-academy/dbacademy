__all__ = ["ChangeLog"]

from typing import Optional


class ChangeLog:

    CHANGE_LOG_TAG = "## Change Log"
    CHANGE_LOG_VERSION = "### Version "

    def __init__(self, *, source_repo: str, readme_file_name: str, target_version: Optional[str]):

        self.__source_repo = source_repo
        self.__target_version = target_version
        self.__readme_file_name = readme_file_name

        self.entries = []
        self.version: Optional[str] = None
        self.date: Optional[str] = None

        self.__load()

    @property
    def readme_file_name(self):
        return self.__readme_file_name

    @property
    def source_repo(self):
        return self.__source_repo

    @property
    def target_version(self):
        return self.__target_version

    def __str__(self):
        string = f"Change Log: v{self.version} ({self.date})"
        for entry in self.entries:
            string += f"\n  {entry}"
        return string

    def print(self):
        print(self)

    def validate(self, expected_version: str, date: Optional[str]):
        from datetime import datetime
        assert self.version == expected_version, f"The change log entry's version is not \"{expected_version}\", found \"{self.version}\"."

        date = date or datetime.today().strftime("%-m-%-d-%Y")
        assert self.date == f"{date}", f"The change log entry's date is not \"{date}\", found \"{self.date}\"."

    def __load(self):
        import os

        readme_path = f"/Workspace{self.source_repo}/{self.readme_file_name}"
        assert os.path.exists(readme_path), f"The README file was not found at {readme_path}"

        with open(readme_path, "r") as f:
            lines = f.readlines()

        change_log_index: Optional[int] = None
        version_index: Optional[int] = None

        for i, line in enumerate(lines):
            line = line.strip()
            if line == self.CHANGE_LOG_TAG:
                change_log_index = i

            elif change_log_index and i > change_log_index and line == "":
                pass  # Just an empty line

            elif change_log_index and i > change_log_index and not version_index:
                if line.strip().startswith("* "):
                    continue

                assert line.startswith(self.CHANGE_LOG_VERSION), f"The next change log entry ({self.CHANGE_LOG_VERSION}...) was not found at {readme_path}:{i + 1}\n{line}"

                parts = line.split(" ")  # "### Version 1.0.2 (01-21-2022)"
                assert len(parts) == 4, f"Expected the change log entry to contain 4 parts and of the form \"### Version vN.N.N (M-D-YYYY)\", found \"{line}\"."
                assert parts[0] == "###", f"Part 1 of the change long entry is not \"###\", found \"{parts[0]}\""
                assert parts[1] == "Version", f"Part 2 of the change long entry is not \"Version\", found \"{parts[1]}\""

                self.version = parts[2]

                v_parts = self.version.split(".")
                assert len(v_parts) == 3, f"The change long entry's version field is not of the form \"vN.N.N\" where \"N\" is an integral value, found {len(v_parts)} parts: \"{self.version}\"."
                assert v_parts[0].isnumeric(), f"The change long entry's Major version field is not an integral value, found \"{self.version}\"."
                assert v_parts[1].isnumeric(), f"The change long entry's Minor version field is not an integral value, found \"{self.version}\"."
                assert v_parts[2].isnumeric(), f"The change long entry's Bug-Fix version field is not an integral value, found \"{self.version}\"."

                if self.target_version is None:
                    version_index = i            # Use the first one we find.
                elif self.target_version == self.version:
                    version_index = i  # We found the target version.
                else:
                    continue

                self.date = parts[3]
                assert self.date.startswith("(") and self.date.endswith(")"), f"Expected the change log entry's date field to be of the form \"(M-D-YYYY)\" or \"(TBD)\", found \"{self.date}\" for version \"{self.version}\"."

                self.date = self.date[1:-1]
                if self.date != "TBD":
                    d_parts = self.date.split("-")
                    assert len(d_parts) == 3, f"The change long entry's date field is not of the form \"(M-D-YYYY)\", found {self.date}\" for version \"{self.version}\"."
                    assert d_parts[0].isnumeric(), f"The change long entry's month field is not an integral value, found \"{self.date}\" for version \"{self.version}\"."
                    assert d_parts[1].isnumeric(), f"The change long entry's day field is not an integral value, found \"{self.date}\" for version \"{self.version}\"."
                    assert d_parts[2].isnumeric(), f"The change long entry's year field is not an integral value, found \"{self.date}\" for version \"{self.version}\"."

            elif version_index and i > version_index and not line.startswith("#"):
                self.entries.append(line)

            elif version_index and i > version_index and line.startswith("#"):
                return self

        assert len(self.entries) > 0, f"The Change Log section was not found in {readme_path}"
