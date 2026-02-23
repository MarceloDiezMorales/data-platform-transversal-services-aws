from setuptools import setup, find_packages

setup(
    name="sp_dependencies",
    version="0.1",
    packages=find_packages(include=["src", "src.*"]),
)