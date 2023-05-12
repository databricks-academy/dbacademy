# from typing import List
# from dbacademy.classrooms.lab_specs import LabSpec
# from dbacademy.dbrest.client import DBAcademyRestClient
#
# # Definitions of the labs
# from dbacademy_test.cloudlabs.lab_specs.lab_specs_user_acceptance import lab_specs_definition
#
# authentication = lab_specs_definition.get("authentication")
#
# lab_specs: List[LabSpec] = list()
# for lab_spec_def in lab_specs_definition.get("labs"):
#     definition = dict()
#     definition.update(lab_spec_def)
#     definition.update(authentication)
#
#     lab_spec = LabSpec(**definition)
#     lab_spec.api = DBAcademyRestClient(token=lab_spec.token,
#                                        endpoint=lab_spec.url)
#
#     lab_specs.append(lab_spec)
#
# for lab_spec in lab_specs:
#     users = lab_spec.api.scim.users.list()
#
#
# # commands = Commands(cluster_spec=None,
# #                     courseware_spec=None,
# #                     event=None,
# #                     labs_specs=lab_specs)
# # commands.all_users = True
# #
# # workspaces:DatabricksApi = list()
# # curriculum_workspace = DatabricksApi("curriculum.cloud.databricks.com", user="doug.bateman@databricks.com", token=dbutils.secrets.get("admin", "curriculum-ws"))
# #
# # results = scan_workspaces(commands.clusters_list_running, all_workspaces, ignore_connection_errors=False, url="")
