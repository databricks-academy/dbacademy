import setuptools
from setuptools import find_packages

reqs = [
    "setuptools",
    "requests",
    "urllib3",
    "overrides",
    "Deprecated",
]

setuptools.setup(
    name="dbacademy",
    author="Databricks, Inc",
    maintainer="Databricks Academy",
    version="v0.0.0",
    install_requires=reqs,
    packages=find_packages(where="src")
)
