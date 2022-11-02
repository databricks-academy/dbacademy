from dbacademy import dbgems

from dbacademy.dbgems.mock_dbutils_class import MockDBUtils
from dbacademy.dbgems.mock_spark_context_class import MockSparkContext

dbgems.dbutils = MockDBUtils()
dbgems.sc = MockSparkContext()
