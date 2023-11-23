"""
This module contains utilities for building and publishing courseware classically composed of Databricks Notebooks, Google Slides, etc. and serves a key integration point to the Content Distribution System.
"""
__all__ = ["help_html", "BuildConfig"]

from dbacademy.dbbuild.build_config_class import BuildConfig


def help_html():
    from dbacademy.dbbuild.publish.notebook_def_impl import NotebookDefImpl

    docs = {
        NotebookDefImpl.D_SOURCE_ONLY: f"Indicates that this cell is used in the source notebook only and is not to be included in the published version.",
        NotebookDefImpl.D_TODO: f"Indicates that this cell is an exercise for students - the entire cell is expected to be commented out.",
        NotebookDefImpl.D_ANSWER: f"Indicates that this cell is the solution to a preceding {NotebookDefImpl.D_TODO} cell. The build will fail if there total number of {NotebookDefImpl.D_TODO} cells is less than  the total number of {NotebookDefImpl.D_ANSWER} cells",
        NotebookDefImpl.D_DUMMY: f"{NotebookDefImpl.D_DUMMY}: A directive that replaces itself with a nice little message for you - used in unit tests for the build engine",

        NotebookDefImpl.D_INCLUDE_HEADER_TRUE: f"Indicates that this notebook should include the default header - to be included in the first cell of the notebook.",
        NotebookDefImpl.D_INCLUDE_HEADER_FALSE: f"Indicates that this notebook should NOT include the default header - to be included in the first cell of the notebook.",
        NotebookDefImpl.D_INCLUDE_FOOTER_TRUE: f"Indicates that this notebook should include the default footer - to be included in the first cell of the notebook.",
        NotebookDefImpl.D_INCLUDE_FOOTER_FALSE: f"Indicates that this notebook should NOT include the default footer - to be included in the first cell of the notebook.",
    }

    html = "<html><body>"
    html += f"<h1>Publishing Help</h1>"
    html += f"<h2>Supported directives</h2>"
    for directive in NotebookDefImpl.SUPPORTED_DIRECTIVES:
        if directive in docs:
            doc = docs[directive]
            html += f"<div><b>{directive}</b>: {doc}</div>"
        else:
            html += f"<div><b>{directive}</b>: Undocumented</div>"

    html += "</body>"
    return html
