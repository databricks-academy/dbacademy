from dbacademy.dbbuild.build_config_class import BuildConfig
from dbacademy.dbbuild.build_utils_class import BuildUtils

from .publish.notebook_def_class import NotebookDef
from .publish.publisher_class import Publisher
from .publish.resource_diff_class import ResourceDiff
from .publish.translator_class import Translator
from .publish.publisher_validator_class import PublisherValidator

from .test.results_evaluator import ResultsEvaluator
from .test.test_suite import TestSuite
from .test.test_instance_class import TestInstance


def help_html():
    from publish.notebook_def_class import D_TODO, D_ANSWER, D_SOURCE_ONLY, D_DUMMY
    from publish.notebook_def_class import SUPPORTED_DIRECTIVES
    from publish.notebook_def_class import D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE

    docs = {
        D_SOURCE_ONLY: f"Indicates that this cell is used in the source notebook only and is not to be included in the published version.",
        D_TODO: f"Indicates that this cell is an exercise for students - the entire cell is expected to be commented out.",
        D_ANSWER: f"Indicates that this cell is the solution to a preceding {D_TODO} cell. The build will fail if there total number of {D_TODO} cells is less than  the total number of {D_ANSWER} cells",
        D_DUMMY: f"{D_DUMMY}: A directive that replaces itself with a nice little message for you - used in unit tests for the build engine",

        D_INCLUDE_HEADER_TRUE: f"Indicates that this notebook should include the default header - to be included in the first cell of the notebook.",
        D_INCLUDE_HEADER_FALSE: f"Indicates that this notebook should NOT include the default header - to be included in the first cell of the notebook.",
        D_INCLUDE_FOOTER_TRUE: f"Indicates that this notebook should include the default footer - to be included in the first cell of the notebook.",
        D_INCLUDE_FOOTER_FALSE: f"Indicates that this notebook should NOT include the default footer - to be included in the first cell of the notebook.",
    }

    html = "<html><body>"
    html += f"<h1>Publishing Help</h1>"
    html += f"<h2>Supported directives</h2>"
    for directive in SUPPORTED_DIRECTIVES:
        if directive in docs:
            doc = docs[directive]
            html += f"<div><b>{directive}</b>: {doc}</div>"
        else:
            html += f"<div><b>{directive}</b>: Undocumented</div>"

    html += "</body>"
    return html
