def test_parse_directives_python():
    from dbacademy.dbpublish import get_leading_comments
    from dbacademy.dbpublish import parse_directives

    source = """
# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE""".strip()

    comments = get_leading_comments("python", source)
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

    source = """
-- Databricks notebook source
-- INCLUDE_HEADER_TRUE
-- INCLUDE_FOOTER_TRUE""".strip()

    comments = get_leading_comments("sql", source)
    assert len(comments) == 3, f"Expected 3 comments, found\n{comments}"
    assert comments[0] == "Databricks notebook source"
    assert comments[1] == "INCLUDE_HEADER_TRUE"
    assert comments[2] == "INCLUDE_FOOTER_TRUE"

    directives = parse_directives(1, comments)
    assert len(directives) == 2, f"Expected 2 directives, found\n{directives}"
    assert directives[0] == "INCLUDE_HEADER_TRUE"
    assert directives[1] == "INCLUDE_FOOTER_TRUE"


def test_parse_md_cell():
    from dbacademy.dbpublish import parse_directives
    from dbacademy.dbpublish import get_leading_comments

    source = """
// MAGIC %md -- Arbitrary comment after magic command
// MAGIC -- DUMMY
// MAGIC 
// MAGIC This is Markdown from a Python notebook""".strip()

    comments = get_leading_comments("scala", source)
    assert len(comments) == 2, f"Expected 2 comments, found\n{comments}"
    assert comments[0] == "Arbitrary comment after magic command"
    assert comments[1] == "DUMMY"

    directives = parse_directives(1, comments)
    assert len(directives) == 1, f"Expected 1 directives, found\n{directives}"
    assert directives[0] == "DUMMY"
