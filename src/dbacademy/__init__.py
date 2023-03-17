"""
The dbacademy package is the super package to a broad number of systems.

common: package for functionality that is generic and used across all other packages.

Courseware Development
dbbuild: Courseware build tools.
dbgems: Wrappers around misc utility functions used from within a notebook.
dbhelper: Primary entry point for Notebook based curriculum and the DBAcademyHelper (DA) object.

Automation & REST APIs
* classrooms:
* rest:
* docebo:
* dougrest:

Misc [REST] APIs
* dbrest: Python wrapper around Databricks's REST API
* github: Python wrapper around GitHub's REST API
* google: Python wrapper around Google's REST API
* slack: Python wrapper around Slack's REST API

Special Projects:
* workspaces_3_0:

"""
def validate_dependencies():
    try:
        # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
        assert validated_dependencies
    except NameError:
        try:
            # noinspection PyUnusedLocal
            validated_dependencies = True
            from dbacademy import dbgems
            dbgems.validate_dependencies("dbacademy")
        except:
            pass


validate_dependencies()
