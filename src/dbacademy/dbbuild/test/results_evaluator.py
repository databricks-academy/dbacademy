__all__ = ["ResultsEvaluator"]

from typing import List, Dict, Any, Union
from dbacademy.common import Cloud


class ResultsEvaluator:

    def __init__(self, results: List[Dict[str, Any]], keep_success: bool):

        self.__keep_success = keep_success

        results.sort(key=lambda r: r.get("notebook_path"))

        self.__failed_set = [r for r in results if r.get("result_state") == "FAILED"]
        self.__ignored_set = [r for r in results if r.get("result_state") == "IGNORED"]
        self.__success_set = [r for r in results if r.get("result_state") == "SUCCESS"]

        self.__cell_style = "padding: 5px; border: 1px solid black; white-space:nowrap"
        self.__header_style = "padding-right:1em; border: 1px solid black; font-weight:bold; padding: 5px; background-color: F0F0F0"

    @property
    def cell_style(self) -> str:
        return self.__cell_style

    @property
    def header_style(self) -> str:
        return self.__header_style

    @property
    def keep_success(self) -> bool:
        return self.__keep_success

    @property
    def failed_set(self) -> List[Dict[str, Any]]:
        return self.__failed_set

    @property
    def ignored_set(self) -> List[Dict[str, Any]]:
        return self.__ignored_set

    @property
    def success_set(self) -> List[Dict[str, Any]]:
        return self.__success_set

    @property
    def passed(self) -> bool:
        return len(self.failed_set) == 0

    def to_html(self) -> str:
        html = "</body>"
        html += self.add_section("Failed", self.failed_set)
        html += self.add_section("Ignored", self.ignored_set)
        html += self.add_section("Success", self.success_set, print_links=self.keep_success)
        html += "</body>"
        return html

    @classmethod
    def add_row(cls, *, style: str, cloud: Union[str, Cloud], job: str, duration: str) -> str:
        return f"""
      <tr>
          <td style="{style}">{cloud}</td>
          <td style="{style}; width:100%">{job}</td>
          <td style="{style}; text-align:right">{duration}</td>
      </tr>
      """

    @classmethod
    def format_duration(cls, duration: int) -> str:
        from math import floor
        seconds = floor(duration / 1000) % 60
        minutes = floor(duration / (1000 * 60)) % 60
        hours = floor(duration / (1000 * 60 * 60)) % 24

        if hours > 0:
            return f"{hours}h, {minutes}m, {seconds}s"
        elif minutes > 0:
            return f"{minutes}m, {seconds}s"
        else:
            return f"{seconds}s"

    @classmethod
    def to_job_link(cls, *, job_id: str, run_id: str, label: str) -> str:
        from dbacademy.dbbuild.build_utils import BuildUtils

        url = BuildUtils.to_job_url(job_id=job_id, run_id=run_id)
        return f"""<a href="{url}" target="_blank">{label}</a>"""

    def add_section(self, title: str, rows: List[Dict[str, Any]], print_links: bool = True) -> str:
        html = f"""<h1>{title}</h1>"""
        if len(rows) == 0:
            html += "<p>No records found</p>"
            return html

        html += f"""<table style="border-collapse: collapse; width:100%">"""
        html += self.add_row(style=self.header_style,
                             cloud="Cloud",
                             job="Job",
                             duration="Duration")
        for row in rows:
            link = row.get("notebook_path")
            if print_links:
                link = self.to_job_link(job_id=row.get("job_id"),
                                        run_id=row.get("run_id"),
                                        label=row.get("notebook_path"))

            html += self.add_row(style=self.cell_style,
                                 cloud=row.get("cloud"),
                                 job=link,
                                 duration=self.format_duration(row.get("execution_duration")))
            html += """<tbody></tbody><tbody>"""

        html += "</table>"

        return html
