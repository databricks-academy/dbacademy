__all__ = ["NOTEBOOKS"]


# noinspection PyPep8Naming
class NotebookConstants:

    @property
    def DBTITLE(self) -> str:
        return "DBTITLE"

    @property
    def DATABRICKS_NOTEBOOK_SOURCE(self) -> str:
        return "Databricks notebook source"


NOTEBOOKS: NotebookConstants = NotebookConstants()
