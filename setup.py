from setuptools import find_packages, setup

setup(
    name="opensource_mds",
    packages=find_packages(exclude=["opensource_mds_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-duckdb",
        "duckdb[polars]",
        "polars",
        "pyarrow",
        "dbt-duckdb",
        "sling"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
