import setuptools
from setuptools import find_packages

reqs = [
    # "setuptools",  # Provided dependency
    # "requests",    # Provided dependency
    # "urllib3",     # Provided dependency
]

setuptools.setup(
    version="v2.0.5",
    name="dbacademy",
    author="Databricks, Inc",
    maintainer="Databricks Academy",
    url="https://github.com/databricks-academy/dbacademy",
    install_requires=reqs,
    package_dir={"": "src"},
    packages=find_packages(where="src")
)
