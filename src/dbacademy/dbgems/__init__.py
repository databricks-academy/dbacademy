from typing import Union, Any
import pyspark
import dbacademy.common
from dbacademy.common import print_warning
from .mock_dbutils_class import MockDBUtils


def check_deprecation_logging_enabled():
    if spark is None:
        return
    status = spark.conf.get("dbacademy.deprecation.logging", None)
    if status is None:
        dbacademy.common.deprecation_log_level = "ignore"
    elif status.lower() == "enabled":
        dbacademy.common.deprecation_log_level = "warn"
    else:
        dbacademy.common.deprecation_log_level = status.lower()


def sql(query):
    return spark.sql(query)


def get_parameter(name: str, default_value: Any = "") -> Union[None, str]:
    from py4j.protocol import Py4JJavaError
    try:
        if default_value is not None and type(default_value) != str:
            default_value = str(default_value)

        result = dbutils.widgets.get(name)
        return_value = result or default_value

        return None if return_value is None else str(return_value)

    except Py4JJavaError as ex:
        if "InputWidgetNotDefined" not in ex.java_exception.getClass().getName():
            raise ex
        else:
            return default_value


def get_cloud():
    import os

    config_path = "/databricks/common/conf/deploy.conf"
    if not os.path.exists(config_path):
        return "UNK"

    with open(config_path) as f:
        for line in f:
            if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line:
                return "GCP"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line:
                return "AWS"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line:
                return "MSA"

    raise Exception("Unable to identify the cloud provider.")


def get_tags():
    tags = dbutils.entry_point.getDbutils().notebook().getContext().tags()
    # noinspection PyProtectedMember,PyUnresolvedReferences
    java_map = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(tags)
    return java_map


def get_tag(tag_name: str, default_value: str = None) -> str:
    try:
        value = get_tags().get(tag_name)
        return value or default_value
    except Exception as e:
        if "CommandContext.tags() is not whitelisted" in str(e):
            return default_value
        else:
            raise e


def get_username() -> str:
    return get_tag("user")


def get_browser_host_name(default_value=None):
    return get_tag(tag_name="browserHostName", default_value=default_value)


def get_job_id(default_value=None):
    return get_tag(tag_name="jobId", default_value=default_value)


def is_job():
    return get_job_id() is not None


def get_workspace_id() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


def get_notebook_path() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)


def get_notebook_name() -> str:
    return get_notebook_path().split("/")[-1]


def get_notebook_dir(offset=-1) -> str:
    return "/".join(get_notebook_path().split("/")[:offset])


def get_notebooks_api_endpoint() -> str:
    if isinstance(dbutils, MockDBUtils):
        raise Exception("This is a MockDBUtils")
    else:
        # noinspection PyUnresolvedReferences
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


def get_notebooks_api_token() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


def jprint(value: dict, indent: int = 4):
    assert type(value) == dict or type(value) == list, f"Expected value to be of type \"dict\" or \"list\", found \"{type(value)}\". "

    import json
    print(json.dumps(value, indent=indent))


def lookup_current_module_version(module: str) -> str:
    import json, pkg_resources

    distribution = pkg_resources.get_distribution(module)
    name = distribution.project_name
    version = distribution.version

    direct_url = f"{distribution.location}/{name}-{version}.dist-info/direct_url.json"

    with open(direct_url) as f:
        data = json.load(f)
        requested_revision = data.get("vcs_info", {}).get("requested_revision", None)
        requested_revision = requested_revision or data.get("vcs_info", {}).get("commit_id", None)
        return requested_revision or f"v{version}"


def is_curriculum_workspace() -> bool:
    if dbutils is None:
        return False

    # TODO Consider that this will return false when ran as a job. The net effect being that we would not, for example, get library warnings for smoke-tests.
    host_name = get_browser_host_name(default_value="unknown")

    if host_name.endswith(".cloud.databricks.com"):
        if host_name.startswith("curriculum-student"):
            return False  # Exception for "student" workspaces - e.g. suppress warnings
        else:
            return host_name.startswith("curriculum-")
    return False


def validate_dependencies(module: str, curriculum_workspaces_only=True) -> bool:
    # Don't do anything unless this is in one of the Curriculum Workspaces
    from dbacademy import github

    testable = curriculum_workspaces_only is False or is_curriculum_workspace()
    try:
        if testable:
            current_version = lookup_current_module_version(module)
            versions = github.default_client.repo(module).list_all_tags()

            if len(versions) == 0:
                print(f"** WARNING ** No versions found for {module}; Double check the spelling and try again.")
                return False  # There are no versions to process

            elif len(versions) == 1 and versions[0] == "v0.0.0":
                print(f"** WARNING ** Cannot test version dependency for {module}; GitHub rate limit exceeded.")
                return False  # We are being rate limited, just bury the message.

            elif current_version.startswith("v"):
                # Starts with "v" when a true version, otherwise it's a branch or commit hash
                if current_version[1:] == versions[-1]:
                    return True  # They match, all done!

                print_warning(title=f"Outdated Dependency",
                              message=f"You are using version \"{current_version}\" but the latest version is \"v{versions[-1]}\".\n" +
                                      f"Please update your dependencies on the module \"{module}\" at your earliest convenience.")
            else:
                print_warning(title=f"Invalid Dependency",
                              message=f"You are using the branch or commit hash \"{current_version}\" but the latest version is \"v{versions[-1]}\".\n" +
                                      f"Please update your dependencies on the module \"{module}\" at your earliest convenience.")
    except Exception as e:
        if testable:
            raise e
        else:
            pass  # Bury the exception

    return False


def get_workspace_url():

    workspaces = {
        "3551974319838082": "https://curriculum-dev.cloud.databricks.com/?o=3551974319838082",
        "8422030046858219": "https://8422030046858219.9.gcp.databricks.com/?o=8422030046858219",
        "2472203627577334": "https://westus2.azuredatabricks.net/?o=2472203627577334"
    }

    workspace_url = sc.getConf().get("spark.databricks.workspaceUrl", defaultValue=None)

    if workspace_url is not None:
        return f"https://{workspace_url}/?o={get_workspace_id()}"

    elif get_browser_host_name() is not None:
        return f"https://{get_browser_host_name()}/?o={get_workspace_id()}"

    elif get_workspace_id() in workspaces:
        return workspaces.get(get_workspace_id())

    else:
        return f"https://{get_notebooks_api_token()}/?o={get_workspace_id()}"


# noinspection PyUnresolvedReferences
def proof_of_life(expected_get_username,
                  expected_get_tag,
                  expected_get_browser_host_name,
                  expected_get_workspace_id,
                  expected_get_notebook_path,
                  expected_get_notebook_name,
                  expected_get_notebook_dir,
                  expected_get_notebooks_api_endpoint):
    """
    Because it is too difficult to validate this from the command line, this function
    simply invokes all the functions as proof of life
    """

    import dbruntime
    from py4j.java_collections import JavaMap

    assert isinstance(dbutils, dbruntime.dbutils.DBUtils), f"Expected {dbruntime.dbutils.DBUtils}, found {type(dbutils)}"
    assert isinstance(spark, pyspark.sql.SparkSession), f"Expected {pyspark.sql.SparkSession}, found {type(spark)}"
    assert isinstance(sc, pyspark.context.SparkContext), f"Expected {pyspark.context.SparkContext}, found {type(sc)}"

    value = get_parameter("some_widget", default_value="undefined")
    assert value == "undefined", f"Expected \"undefined\", found \"{value}\"."

    value = get_cloud()
    assert value == "AWS", f"Expected \"AWS\", found \"{value}\"."

    value = get_tags()
    assert type(value) == JavaMap, f"Expected type \"dict\", found \"{type(value)}\"."

    value = get_tag("orgId")
    assert value == expected_get_tag, f"Expected \"{expected_get_tag}\", found \"{value}\"."

    value = get_username()
    assert value == expected_get_username, f"Expected \"{expected_get_username}\", found \"{value}\"."

    value = get_browser_host_name()
    assert value == expected_get_browser_host_name, f"Expected \"{expected_get_browser_host_name}\", found \"{value}\"."

    value = get_job_id()
    assert value is None, f"Expected \"None\", found \"{value}\"."

    value = is_job()
    assert value is False, f"Expected \"{False}\", found \"{value}\"."

    value = get_workspace_id()
    assert value == expected_get_workspace_id, f"Expected \"{expected_get_workspace_id}\", found \"{value}\"."

    value = get_notebook_path()
    assert value == expected_get_notebook_path, f"Expected \"{expected_get_notebook_path}\", found \"{value}\"."

    value = get_notebook_name()
    assert value == expected_get_notebook_name, f"Expected \"{expected_get_notebook_name}\", found \"{value}\"."

    value = get_notebook_dir()
    assert value == expected_get_notebook_dir, f"Expected \"{expected_get_notebook_dir}\", found \"{value}\"."

    value = get_notebooks_api_endpoint()
    assert value == expected_get_notebooks_api_endpoint, f"Expected \"{expected_get_notebooks_api_endpoint}\", found \"{value}\"."

    value = get_notebooks_api_token()
    assert value is not None, f"Expected not-None."

    print("All tests passed!")


def display_html(html) -> None:
    import inspect
    caller_frame = inspect.currentframe().f_back
    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        function = caller_globals.get("displayHTML")
        if function:
            return function(html)
        caller_frame = caller_frame.f_back
    raise ValueError("displayHTML not found in any caller frames.")


def display(html) -> None:
    import inspect
    caller_frame = inspect.currentframe().f_back
    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        function = caller_globals.get("display")
        if function:
            return function(html)
        caller_frame = caller_frame.f_back
    raise ValueError("display not found in any caller frames.")


GENERATING_DOCS = "generating_docs"


def is_generating_docs() -> bool:
    value = get_parameter(GENERATING_DOCS, False)
    return str(value).lower() == "true"


def stable_hash(*args, length: int) -> str:
    import hashlib
    data = ":".join(args).encode("utf-8")
    value = int(hashlib.md5(data).hexdigest(), 16)
    numerals = "0123456789abcdefghijklmnopqrstuvwxyz"
    result = []
    for i in range(length):
        result += numerals[value % 36]
        value //= 36
    return "".join(result)


def clean_string(value, replacement: str = "_"):
    import re
    replacement_2x = replacement+replacement
    value = re.sub(r"[^a-zA-Z\d]", replacement, str(value))
    while replacement_2x in value: value = value.replace(replacement_2x, replacement)
    return value


def clock_start() -> int:
    import time
    return int(time.time())


def clock_stopped(start: int, end: str = "") -> str:
    import time
    return f"({int(time.time()) - start} seconds{end})"


def find_global(target):
    import inspect
    caller_frame = inspect.currentframe().f_back

    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        what = caller_globals.get(target)
        if what:
            return what
        caller_frame = caller_frame.f_back

    return None


spark: Union[None, "pyspark.sql.SparkSession"] = find_global("spark")
sc: Union[None, "pyspark.SparkContext"] = find_global("sc")
dbutils: Union[MockDBUtils, None] = find_global("dbutils")

check_deprecation_logging_enabled()
