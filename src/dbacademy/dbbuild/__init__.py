from .build_config_class import BuildConfig

def validate_dependencies():
    from dbacademy_gems import dbgems
    dbgems.validate_dependencies("dbacademy-gems")
    dbgems.validate_dependencies("dbacademy-rest")
    # dbgems.validate_dependencies("dbacademy-helper")
    dbgems.validate_dependencies("dbacademy-courseware")


def help_html():
    from dbacademy_courseware.dbpublish.notebook_def_class import D_TODO, D_ANSWER, D_SOURCE_ONLY, D_DUMMY
    from dbacademy_courseware.dbpublish.notebook_def_class import SUPPORTED_DIRECTIVES
    from dbacademy_courseware.dbpublish.notebook_def_class import D_INCLUDE_HEADER_TRUE, D_INCLUDE_HEADER_FALSE, D_INCLUDE_FOOTER_TRUE, D_INCLUDE_FOOTER_FALSE

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


def get_workspace_url():
    from dbacademy_gems import dbgems

    workspaces = {
        "3551974319838082": "https://curriculum-dev.cloud.databricks.com/?o=3551974319838082",
        "8422030046858219": "https://8422030046858219.9.gcp.databricks.com/?o=8422030046858219",
        "2472203627577334": "https://westus2.azuredatabricks.net/?o=2472203627577334"
    }

    if dbgems.get_browser_host_name() is not None:
        return f"https://{dbgems.get_browser_host_name()}/?o={dbgems.get_workspace_id()}"

    elif dbgems.get_workspace_id() in workspaces:
        return workspaces.get(dbgems.get_workspace_id())

    else:
        return f"https://{dbgems.get_notebooks_api_token()}/?o={dbgems.get_workspace_id()}"


def to_job_url(*, job_id: str, run_id: str):
    base_url = get_workspace_url()
    return f"{base_url}#job/{job_id}/run/{run_id}"


def validate_type(actual_value, name, expected_type):
    assert type(actual_value) == expected_type, f"Expected the parameter {name} to be of type {expected_type}, found {type(actual_value)}"
    return actual_value


validate_dependencies()
