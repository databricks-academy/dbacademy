class ResultsEvaluator:
    def __init__(self, results):

        results.sort(key=lambda r: r.get("notebook_path"))

        self.failed_set =  [r for r in results if r.get("result_state") == "FAILED"] # df.filter("status == 'FAILED'").orderBy("notebook_path").collect()
        self.ignored_set = [r for r in results if r.get("result_state") == "IGNORED"] # df.filter("status == 'IGNORED'").orderBy("notebook_path").collect()
        self.success_set = [r for r in results if r.get("result_state") == "SUCCESS"] # df.filter("status == 'SUCCESS'").orderBy("notebook_path").collect()

        self.cell_style = "padding: 5px; border: 1px solid black; white-space:nowrap"
        self.header_style = "padding-right:1em; border: 1px solid black; font-weight:bold; padding: 5px; background-color: F0F0F0"

    @property
    def passed(self) -> bool:
        return len(self.failed_set) == 0

    def to_html(self, print_success_links=False) -> str:
        html = "</body>"
        html += self.add_section("Failed", self.failed_set)
        html += self.add_section("Ignored", self.ignored_set)
        html += self.add_section("Success", self.success_set, print_links=print_success_links)
        html += "</body>"
        return html

    def add_row(self, style, cloud, job, duration):
        return f"""
      <tr>
          <td style="{style}">{cloud}</td>
          <td style="{style}; width:100%">{job}</td>
          <td style="{style}; text-align:right">{duration}</td>
      </tr>
      """

    def format_duration(self, duration):
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

    def add_section(self, title, rows, print_links=True):
        html = f"""<h1>{title}</h1>"""
        if len(rows) == 0:
            html += "<p>No records found</p>"
            return html

        html += f"""<table style="border-collapse: collapse; width:100%">"""
        html += self.add_row(self.header_style, "Cloud", "Job", "Duration")

        for row in rows:

            # self.test_results.append({
            #     "suite_id": self.test_config.suite_id,
            #     "test_id": test_id,
            #     "name": self.test_config.name,
            #     "result_state": result_state,
            #     "execution_duration": execution_duration,
            #     "cloud": self.test_config.cloud,
            #     "job_name": test.job_name,
            #     "job_id": job_id,
            #     "run_id": run_id,
            #     "notebook_path": notebook_path,
            #     "spark_version": self.test_config.spark_version,
            #     "test_type": self.test_config.test_type
            # })


            link = row["notebook_path"]
            if print_links:
                link = to_job_link(row["cloud"], row["job_id"], row["run_id"], row["notebook_path"])

            html += self.add_row(self.cell_style, row["cloud"], link, self.format_duration(row["execution_duration"]))
            html += """<tbody></tbody><tbody>"""

        html += "</table>"

        return html


