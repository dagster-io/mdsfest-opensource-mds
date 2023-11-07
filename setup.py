from setuptools import find_packages, setup

setup(
    name="opensource_mds",
    packages=find_packages(exclude=["opensource_mds_tests"]),
    install_requires=[
        "duckdb==0.9.1",
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "dagster-duckdb",
        "dagster-embedded-elt",
        "duckdb[polars]",
        "polars",
        "pyarrow",
        "dbt-duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
