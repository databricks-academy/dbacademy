"""
This module contains utilities for building and publishing courseware classically composed of Databricks Notebooks, Google Slides, etc. and serves a key integration point to the Content Distribution System.
"""
# __all__ = ["help_html", "BuildConfig"]
__all__ = ["help_html"]

# from dbacademy.dbbuild.build_config_class import BuildConfig


def help_html():
    from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

    docs = {
        NotebookDef.D_SOURCE_ONLY: f"Indicates that this cell is used in the source notebook only and is not to be included in the published version.",
        NotebookDef.D_TODO: f"Indicates that this cell is an exercise for students - the entire cell is expected to be commented out.",
        NotebookDef.D_ANSWER: f"Indicates that this cell is the solution to a preceding {NotebookDef.D_TODO} cell. The build will fail if there total number of {NotebookDef.D_TODO} cells is less than  the total number of {NotebookDef.D_ANSWER} cells",
        NotebookDef.D_DUMMY: f"{NotebookDef.D_DUMMY}: A directive that replaces itself with a nice little message for you - used in unit tests for the build engine",

        NotebookDef.D_INCLUDE_HEADER_TRUE: f"Indicates that this notebook should include the default header - to be included in the first cell of the notebook.",
        NotebookDef.D_INCLUDE_HEADER_FALSE: f"Indicates that this notebook should NOT include the default header - to be included in the first cell of the notebook.",
        NotebookDef.D_INCLUDE_FOOTER_TRUE: f"Indicates that this notebook should include the default footer - to be included in the first cell of the notebook.",
        NotebookDef.D_INCLUDE_FOOTER_FALSE: f"Indicates that this notebook should NOT include the default footer - to be included in the first cell of the notebook.",
    }

    html = "<html><body>"
    html += f"<h1>Publishing Help</h1>"
    html += f"<h2>Supported directives</h2>"
    for directive in NotebookDef.SUPPORTED_DIRECTIVES:
        if directive in docs:
            doc = docs[directive]
            html += f"<div><b>{directive}</b>: {doc}</div>"
        else:
            html += f"<div><b>{directive}</b>: Undocumented</div>"

    html += "</body>"
    return html
