try: from dbacademy_gems import dbgems
except ImportError as e: raise Exception("The runtime dependency dbgems was not found. Please install https://github.com/databricks-academy/dbacademy-gems") from e

try: from dbacademy import dbrest
except ImportError as e: raise Exception("The runtime dependency dbrest was not found. Please install https://github.com/databricks-academy/dbacademy-rest") from e

from .dbacademy_helper_class import DBAcademyHelper
from .lesson_config_class import LessonConfig
from .course_config_class import CourseConfig
from .paths_class import Paths


def validate_dependencies():
    from py4j.protocol import Py4JError

    try:
        from dbacademy_gems import dbgems
        dbgems.validate_dependencies("dbacademy-gems")
        dbgems.validate_dependencies("dbacademy-rest")
        dbgems.validate_dependencies("dbacademy-helper")

    except Py4JError:
        return False


validate_dependencies()
