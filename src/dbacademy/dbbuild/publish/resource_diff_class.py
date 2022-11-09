from typing import Union


class ResourceDiff:
    from ..build_config_class import BuildConfig

    def __init__(self, build_config: BuildConfig, *, resources_folder: str = None, old_resource: str = None, new_resource: str = None):
        import os

        self.build_config = build_config
        self.resources_folder = resources_folder or f"/Workspace/{build_config.source_repo}/Resources"

        if new_resource is None or old_resource is None:
            versions = [f.split("-")[-1][1:] for f in os.listdir(self.resources_folder) if f.startswith("english-")]
            versions.sort(key=lambda v: (int(v.split(".")[0]) * 10000) + (int(v.split(".")[1]) * 100) + int(v.split(".")[2]))

            new_resource = new_resource or f"english-v{versions[-1]}"
            old_resource = old_resource or f"english-v{versions[-2]}"

        self.new_resource = new_resource
        self.old_resource = old_resource

        self.new_dir = f"{self.resources_folder}/{self.new_resource}"
        self.old_dir = f"{self.resources_folder}/{self.old_resource}"

        self.files_a = None
        self.files_b = None
        self.all_files = None

    def compare_and_save(self, docs_url: str) -> str:
        diff = ResourceDiff(self.build_config)
        file_name = f"{diff.old_resource}_vs_{diff.new_resource}.html"
        html = diff.compare(docs_url, file_name)

        target_file = f"/Workspace{self.build_config.source_repo}/docs/{file_name}"

        with open(target_file, "w") as file:
            file.write(html)

        print(f"Wrote report to \"{target_file}\"")
        return html

    def compare(self, docs_url: str, file_name: str):
        import os

        print(f"Comparing {self.old_resource} to {self.new_resource}")

        self.files_a = [os.path.join(dp, f) for dp, dn, filenames in os.walk(self.old_dir) for f in filenames]
        self.files_a = [r[len(self.old_dir) + 1:] for r in self.files_a]

        self.files_b = [os.path.join(dp, f) for dp, dn, filenames in os.walk(self.new_dir) for f in filenames]
        self.files_b = [r[len(self.new_dir) + 1:] for r in self.files_b]

        self.all_files = []
        self.all_files.extend(self.files_a)
        self.all_files.extend(self.files_b)
        self.all_files = list(set(self.all_files))
        self.all_files.sort()

        html = f"""<!DOCTYPE html><html>
        <head>
        <style>
            td {{padding: 5px; border:1px solid silver}}
        </style>
        </head>
        <body style="font-size:16px">
            <div><a href="{docs_url}/{file_name}" target="_blank">See Report: {file_name}</a></div>
            <div style="font-size:smaller>Note: this link is not valid until the report is committed.</div>
            <table style="border-collapse: collapse; border-spacing:0">
                <tr><td>Original:&nbsp;</td><td><b>{self.old_resource}</b></td></tr>
                <tr><td>Latest:&nbsp;</td><td><b>{self.new_resource}</b></td></tr>
            </table>            
            <table style="border-collapse: collapse; border-spacing:0">"""

        html += f"""<thead><tr><td>Change Type</td><td>Message</td></tr></thead>"""

        for file in self.all_files:
            sd = SegmentDiff(file, self.old_dir, self.new_dir)
            sd.read_segments()
            
            if len(sd.diff()) > 0:
                html += f"""<tbody><tr><td colspan="2" style="background-color:gainsboro"><h2>/{sd.name}</h2></td></tr>"""
                for change in sd.diff():
                    html += f"""<tr><td style="white-space:nowrap; font-weight:bold">{change.change_type}</td>
                                    <td style="font-weight:bold; width:100%">{change.message}</td>
                                </tr>"""
                    if change.change_type == "Cell Changed":
                        rows = max(len(change.original_text.split("\n")), len(change.latest_text.split("\n")))+2

                        html += f"""<tr><td colspan="2" style="padding:0">
                            <table style="width:100%; border-collapse: collapse; border-spacing:0"><tr>
                                <td style="width:50%; vertical-align:top; padding:0">
                                    <textarea rows="{rows}" style="padding:2px; width:100%; white-space:pre; border:0">{change.original_text}</textarea>
                                </td>
                                <td style="width:50%; vertical-align:top; padding:0">
                                    <textarea rows="{rows}" style="padding:2px; width:100%; white-space:pre; border:0">{change.latest_text}</textarea>
                                </td>
                            </tr></table>
                        </td></tr>"""
                html += f"""</tbody>"""

        html += "</table></body></html>"
        return html


class Change:
    def __init__(self, change_type: str, name: str, message: str, original_text: str = None, latest_text: str = None):
        self.change_type = change_type
        self.name = name
        self.message = message

        self.original_text = original_text
        if self.original_text is not None:
            while "\n\n" in self.original_text:
                self.original_text = self.original_text.replace("\n\n", "\n")

        self.latest_text = latest_text
        if self.latest_text is not None:
            while "\n\n" in self.latest_text:
                self.latest_text = self.latest_text.replace("\n\n", "\n")


class Segment:
    def __init__(self, guid):
        self.guid = guid
        self.contents = ""

    def add_line(self, line):
        self.contents += line


class SegmentDiff:

    def __init__(self, name, original_dir, latest_dir):
        self.name = name
        self.original_dir = original_dir
        self.latest_dir = latest_dir
        self.segments_a = None
        self.segments_b = None

    def diff(self):
        if self.segments_a is None:
            return [Change("Missing Notebook", self.name, f"{self.name} from original")]
        elif self.segments_b is None:
            return [Change("Missing Notebook", self.name, f"{self.name} from latest")]

        changes = []

        guids = list()
        guids.extend(self.segments_a.keys())
        guids.extend(self.segments_b.keys())
        guids = list(set(guids))

        for guid in guids:
            if guid not in self.segments_a:
                changes.append(Change("Cell Added", self.name, guid))
            elif guid not in self.segments_b:
                changes.append(Change("Cell Removed", self.name, guid))
            elif self.segments_a[guid].contents.strip() != self.segments_b[guid].contents.strip():
                # Try to figure out the first line that changed.
                changes.append(Change("Cell Changed", self.name, f"{guid}", self.segments_a[guid].contents.strip(), self.segments_b[guid].contents.strip()))

        return changes

    def read_segments(self):
        self.segments_a = self._read_segments_file(f"{self.original_dir}/{self.name}")
        self.segments_b = self._read_segments_file(f"{self.latest_dir}/{self.name}")

    @staticmethod
    def _read_segments_file(file: str) -> Union[None, dict]:
        import os

        if not os.path.exists(file):
            return None

        with open(file, "r") as f:
            lines = f.readlines()

            segment = None
            segments = {}

            for i, line in enumerate(lines):
                try:
                    if i == 0:
                        pass  # Line zero will be the file name.

                    elif line.startswith("<hr>--i18n-"):
                        guid = line[11:].strip()
                        segment = Segment(guid)
                        segments[guid] = segment

                    elif line.startswith("<hr sandbox>--i18n-"):
                        guid = line[19:].strip()
                        segment = Segment(guid)
                        segments[guid] = segment

                    else:
                        segment.add_line(line)

                except Exception as e:
                    segment_num = len(segments)
                    raise Exception(f"Exception processing segment {segment_num}, line {i + 1} from {file}:\n{line}") from e

            return segments
