# # Databricks notebook source
# # git+https://github.com/databricks-academy/dbacademy-gems@dev \
# # git+https://github.com/databricks-academy/dbacademy-rest \
# # git+https://github.com/databricks-academy/dbacademy-helper \
#
# # COMMAND ----------
#
# # MAGIC %pip install git+https://github.com/databricks-academy/dbacademy --quiet --disable-pip-version-check
#
# # COMMAND ----------
#
# import dbacademy
# help(dbacademy)
#
# # COMMAND ----------
#
# from dbacademy import dbgems
# help(dbgems)
# type(dbgems)
#
# # COMMAND ----------
#
# import dbacademy.dbgems
# dbacademy.dbgems.proof_of_life(
#     expected_get_username="jacob.parr@databricks.com",
#     expected_get_tag="3551974319838082",
#     expected_get_browser_host_name="curriculum-dev.cloud.databricks.com",
#     expected_get_workspace_id="3551974319838082",
#     expected_get_notebook_path="/Users/jacob.parr@databricks.com/Proof-Of-Life",
#     expected_get_notebook_name="Proof-Of-Life",
#     expected_get_notebook_dir="/Users/jacob.parr@databricks.com",
#     expected_get_notebooks_api_endpoint="https://oregon.cloud.databricks.com",
# )
#
# # COMMAND ----------
#
# import dbacademy.dbgems
# print(type(dbacademy.dbgems.get_dbutils()))
# print("-"*80)
# print(type(dbacademy.dbgems.get_spark_session()))
# print("-"*80)
# print(type(dbacademy.dbgems.get_session_context()))
# print("-"*80)
#
# # COMMAND ----------
#
# import dbacademy.dbgems
# print(type(dbacademy.dbgems.dbutils))
# print("-"*80)
# print(type(dbacademy.dbgems.spark))
# print("-"*80)
# print(type(dbacademy.dbgems.sc))
# print("-"*80)
