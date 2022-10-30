class DocsPublisher:
    from publishing_info_class import Translation

    def __init__(self, translation: Translation):
        self.__translation = translation

    @property
    def translation(self):
        return self.__translation

    def to_html(self):
        html = f"""
        <html><body style="font-size:16px">
            <li><a href="{self.translation.published_docs_folder}" target="_blank">Published Folder</a></li>
            <li><a href="{self.translation.publishing_script}" target="_blank">Publishing Script</a></li>
        """

        links = self.translation.document_links

        if len(links) == 0:
            html += "<li>Documents: None</li>"
        elif len(links) == 1:
            html += f"""<li><a href="{links[0]}" target="_blank">Document</a></li>"""
        else:
            html += """<li>Documents:</li>"""
            html += """<ul style="margin:0">"""
            for i, link in enumerate(links):
                html += f"""<li><a href="{link}" target="_blank">Document #{i + 1}</a></li>"""
            html += "</ul>"

        html += """</body></html>"""
        return html
