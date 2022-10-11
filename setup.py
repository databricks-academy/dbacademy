import setuptools
from setuptools import find_packages

setuptools.setup(
    name="dbacademy",
    author="Databricks, Inc",
    maintainer="Databricks Academy",
    version="v0.0.0",
    package_dir={"": "src"},
    packages=find_packages(where="src")
)
