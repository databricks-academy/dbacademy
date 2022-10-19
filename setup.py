import setuptools
from setuptools import find_packages

reqs = [
    # "setuptools",  # Provided dependency
    # "requests",    # Provided dependency
    # "urllib3",     # Provided dependency
    "overrides",
    "Deprecated",
]

setuptools.setup(
    name="dbacademy",
    author="Databricks, Inc",
    maintainer="Databricks Academy",
    version="v0.0.0",
    install_requires=reqs,
    package_dir={"": "src"},
    packages=find_packages(where="src")
)
