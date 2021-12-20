# Databricks notebook source

# MAGIC %md ## Install via GitHub

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databricks-academy/dbtest

# COMMAND ----------

# MAGIC %md ## Install via Download

# COMMAND ----------

# MAGIC %sh wget https://github.com/RafiKurlansik/project_test/blob/main/dist/my_package-0.1-py3-none-any.whl?raw=true

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/my_package-0.1-py3-none-any.whl?raw=true",
              "dbfs:/dist/my_package-0.1-py3-none-any.whl")

# COMMAND ----------

# MAGIC %pip install /dist/joshuacook/my_package-0.1-py3-none-any.whl
