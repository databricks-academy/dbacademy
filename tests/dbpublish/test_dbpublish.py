cmd_1_python="""# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE"""

cmd_2_python="""# MAGIC %md
# MAGIC # Your First Lesson
# MAGIC ## Python Notebook
# MAGIC 
# MAGIC Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."""

cmd_3_python="""# MAGIC %run ./Includes/Classroom-Setup"""

cmd_4_python="""# TODO
# val text = FILL_IN
# print(text)"""

cmd_5_python="""# ANSWER
text = "Hello Nurse!"
print(text)"""

cmd_6_python="""# SOURCE_ONLY
# This is source-only code and should not appear in the final product
display(spark.sql("SHOW TABLES"))"""

cmd_7_python="""# MAGIC %scala
# MAGIC println("This is Scala from a Python Notebook")"""

cmd_8_python="""# MAGIC %r
# MAGIC print("This is R from a Python Notebook")"""

cmd_9_python="""# MAGIC %sql
# MAGIC select 'This is SQL from a Python notebook'"""

cmd_1_sql = """-- Databricks notebook source
-- INCLUDE_HEADER_TRUE
-- INCLUDE_FOOTER_TRUE"""

cmd_2_sql = """-- MAGIC %md
-- MAGIC # Your Second Lesson
-- MAGIC ## SQL Notebook
-- MAGIC 
-- MAGIC Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."""

cmd_3_sql = """-- MAGIC %run ./Includes/Classroom-Setup"""

cmd_4_sql = """-- TODO
-- SET text = FILLIN;
-- SELECT text"""

cmd_5_sql = """-- ANSWER
SET c.text = 'Hello Nurse!';
SELECT ${c.text};"""

cmd_6_sql = """-- SOURCE_ONLY
-- This is source-only code and should not appear in the final product
SHOW TABLES"""

cmd_7_sql = """-- MAGIC %scala
-- MAGIC println("This is Scala from a SQL Notebook")"""

cmd_8_sql = """-- MAGIC %r
-- MAGIC print("This is R from a SQL Notebook")"""

cmd_9_sql = """-- MAGIC %python
-- MAGIC print("This is Python from a SQL Notebook")"""


def test_parse_directives_python():
    from dbacademy.dbpublish import get_leading_comments
    from dbacademy.dbpublish import parse_directives

    comments = get_leading_comments("python", cmd_1_python)
    assert len(comments) == 3, f"Expected 3 comments, found\n{comments}"
    assert comments[0] == "Databricks notebook source"
    assert comments[1] == "INCLUDE_HEADER_TRUE"
    assert comments[2] == "INCLUDE_FOOTER_TRUE"

    directives = parse_directives(1, comments)
    assert len(directives) == 2, f"Expected 2 directives, found\n{directives}"
    assert directives[0] == "INCLUDE_HEADER_TRUE"
    assert directives[1] == "INCLUDE_FOOTER_TRUE"


def test_parse_directives_sql():
    from dbacademy.dbpublish import parse_directives
    from dbacademy.dbpublish import get_leading_comments

    comments = get_leading_comments("sql", cmd_1_sql)
    assert len(comments) == 3, f"Expected 3 comments, found\n{comments}"
    assert comments[0] == "Databricks notebook source"
    assert comments[1] == "INCLUDE_HEADER_TRUE"
    assert comments[2] == "INCLUDE_FOOTER_TRUE"

    directives = parse_directives(1, comments)
    assert len(directives) == 2, f"Expected 2 directives, found\n{directives}"
    assert directives[0] == "INCLUDE_HEADER_TRUE"
    assert directives[1] == "INCLUDE_FOOTER_TRUE"


