# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession


def init_locals():

    # noinspection PyGlobalUndefined
    global spark, sc, dbutils

    try: spark
    except NameError:spark = SparkSession.builder.getOrCreate()

    try: sc
    except NameError: sc = spark.sparkContext

    try: dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

    return sc, spark, dbutils


sc, spark, dbutils = init_locals()

def get_current_spark_version():
    spark_version = get_tags()["sparkVersion"]
    assert spark_version, "The DBR has not yet been defined, please try again."
    return spark_version

def get_current_instance_pool_id(client, fail_fast=True):
    cluster_id = get_tags()["clusterId"]
    cluster = client.clusters().get(cluster_id)
    
    instance_pool_id = cluster["instance_pool_id"] if "instance_pool_id" in cluster else None

    if fail_fast: assert instance_pool_id, "The current cluster is not defined using an cluster pool"

    return instance_pool_id

def get_cloud():
    with open("/databricks/common/conf/deploy.conf") as f:
        for line in f:
            if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line:
                return "GCP"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line:
                return "AWS"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line:
                return "MSA"

    raise Exception("Unable to identify the cloud provider.")


def get_tags() -> dict:
    # noinspection PyProtectedMember
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())


def get_tag(tag_name: str, default_value: str = None) -> str:
    values = get_tags()[tag_name]
    try:
        if len(values) > 0:
            return values
    except KeyError:
        return default_value


def get_username() -> str:
    return get_tags()["user"]


def get_notebook_path() -> str:
    return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)


def get_notebook_name() -> str:
    return get_notebook_path().split("/")[-1]


def get_notebook_dir(offset=-1) -> str:
    return "/".join(get_notebook_path().split("/")[:offset])


def get_notebooks_api_endpoint() -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


def get_notebooks_api_token() -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
