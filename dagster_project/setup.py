from setuptools import find_packages, setup

setup(
    name="opensource_mds",
    packages=find_packages(exclude=["opensource_mds_tests"]),
)
