# Databricks notebook source
# git+https://github.com/databricks-academy/dbacademy-gems@dev \
# git+https://github.com/databricks-academy/dbacademy-rest \
# git+https://github.com/databricks-academy/dbacademy-helper \

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@dev \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import dbacademy
help(dbacademy)

# COMMAND ----------

from dbacademy import dbgems

# COMMAND ----------

import dbacademy_gems
help(dbacademy_gems)

# COMMAND ----------

import dbacademy.dbgems
type(dbgems)

# COMMAND ----------

import dbacademy.dbgems
dbacademy.dbgems.proof_of_life(
    expected_get_username="jacob.parr@databricks.com",
    expected_get_tag="3551974319838082",
    expected_get_browser_host_name="curriculum-dev.cloud.databricks.com",
    expected_get_workspace_id="3551974319838082",
    expected_get_notebook_path="/Users/jacob.parr@databricks.com/Proof-Of-Life",
    expected_get_notebook_name="Proof-Of-Life",
    expected_get_notebook_dir="/Users/jacob.parr@databricks.com",
    expected_get_notebooks_api_endpoint="https://oregon.cloud.databricks.com",
    expected_get_current_spark_version="11.2.x-scala2.12",
    expected_get_current_instance_pool_id="1117-212409-soars13-pool-6plxsi6q",
    expected_get_current_node_type_id="i3.xlarge"
)

# COMMAND ----------

import dbacademy_gems.dbgems
dbacademy_gems.dbgems.proof_of_life(
    expected_get_username="jacob.parr@databricks.com",
    expected_get_tag="3551974319838082",
    expected_get_browser_host_name="curriculum-dev.cloud.databricks.com",
    expected_get_workspace_id="3551974319838082",
    expected_get_notebook_path="/Users/jacob.parr@databricks.com/Proof-Of-Life",
    expected_get_notebook_name="Proof-Of-Life",
    expected_get_notebook_dir="/Users/jacob.parr@databricks.com",
    expected_get_notebooks_api_endpoint="https://oregon.cloud.databricks.com",
    expected_get_current_spark_version="11.2.x-scala2.12",
    expected_get_current_instance_pool_id="1117-212409-soars13-pool-6plxsi6q",
    expected_get_current_node_type_id="i3.xlarge"
)

# COMMAND ----------

import dbacademy_gems.dbgems
print(type(dbacademy_gems.dbgems.get_dbutils()))
print("-"*80)
print(type(dbacademy_gems.dbgems.get_spark_session()))
print("-"*80)
print(type(dbacademy_gems.dbgems.get_session_context()))
print("-"*80)

# COMMAND ----------

import dbacademy_gems.dbgems
print(type(dbacademy_gems.dbgems.dbutils))
print("-"*80)
print(type(dbacademy_gems.dbgems.spark))
print("-"*80)
print(type(dbacademy_gems.dbgems.sc))
print("-"*80)
