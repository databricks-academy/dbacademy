"""
This module contains utilities for building and publishing courseware classically composed of Databricks Notebooks, Google Slides, etc. and serves a key integration point to the Content Distribution System.
"""
__all__ = ["help_html", "load_build_config", "create_build_config"]


from typing import Dict, Any, List, TypeVar
from dbacademy.dbbuild.build_config import BuildConfig
from dbacademy.common import validate
ParameterType = TypeVar("ParameterType")


def help_html():
    from dbacademy.dbbuild.publish.notebook_def import NotebookDef

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


def load_from_config(param: str, expected_type: ParameterType, notebook_config: Dict[str, Any]) -> ParameterType:

    if param not in notebook_config:
        return None

    actual_value = notebook_config.get(param)

    if expected_type == List[str]:
        assert type(actual_value) == list, f"Expected the value for \"{param}\" to be of type \"List[str]\", found \"{type(actual_value)}\"."
        for item in actual_value:
            assert type(item) == str, f"Expected the elements of \"{param}\" to be of type \"str\", found \"{type(item)}\"."
    else:
        assert type(actual_value) == expected_type, f"Expected the value for \"{param}\" to be of type \"{expected_type}\", found \"{type(actual_value)}\"."

    return actual_value


def create_build_config(config: Dict[str, Any], version: str, **kwargs) -> BuildConfig:
    """
    :param config: The dictionary of configuration parameters
    :param version: The current version being published. Expected to be one of BuildConfig.VERSIONS_LIST or an actual version number in the form of "vX.Y.Z"
    :return:
    """
    validate(config=config).required.dict(str)
    validate(version=version).required.str()

    if kwargs is not None:
        for k, v in kwargs.items():
            config[k] = v

    notebook_configs: Dict[str, Any] = config.get("notebook_config", dict())
    if "notebook_config" in config:
        del config["notebook_config"]

    if "publish_only" in config:
        publish_only: Dict[str, List[str]] = config.get("publish_only")
        del config["publish_only"]

        white_list = publish_only.get("white_list", None)
        config["white_list"] = validate(white_list=white_list).required.list(str)

        black_list = publish_only.get("black_list", None)
        config["black_list"] = validate(black_list=black_list).required.list(str)

    bc = BuildConfig(version=version, **config)
    bc.initialize_notebooks()

    for name, notebook_config in notebook_configs.items():
        assert name in bc.notebooks, f"The notebook \"{name}\" doesn't exist."
        notebook = bc.notebooks.get(name)

        notebook.include_solution = load_from_config("include_solution", bool, notebook_config)
        notebook.test_round = load_from_config("test_round", int, notebook_config)
        notebook.ignored = load_from_config("ignored", bool, notebook_config)
        notebook.order = load_from_config("order", int, notebook_config)

        notebook.ignoring.clear()
        notebook.ignoring.extend(load_from_config("ignored_errors", List[str], notebook_config))

    return bc


def load_build_config(file: str, *, version: str, **kwargs) -> BuildConfig:
    """
    Loads the configuration for this course from the specified JSON file.
    See also BuildConfig.VERSION_TEST
    See also BuildConfig.VERSION_BUILD
    See also BuildConfig.VERSION_TRANSLATION
    See also BuildConfig.VERSIONS_LIST
    :param file: The path to the JSON config file
    :param version: The current version being published. Expected to be one of BuildConfig.VERSIONS_LIST or an actual version number in the form of "vX.Y.Z"
    :return:
    """
    import json

    validate(file=file).required.str()
    validate(version=version).required.str()

    with open(file) as f:
        return create_build_config(config=json.load(f), version=version, **kwargs)
