# Open-Source Modern Data Stack

This is the visualization output from the Open-Source MDS Pipeline.

![Asset Graph](/asset_graph.png)

## The Stack

This uses [Dagster](https://dagster.io) as the orchestration layer. Dagster
coordinates the execution of the various other components, capturing logs,
metadata, and displaying the graph you see above.

The bird observations are downloaded via Python from [FeederWatch](https://feederwatch.org/explore/raw-dataset-requests/)

Mastodon Toots are extracted via [Steampipe](https://steampipe.io) from the
[birds.town](https://birds.town) instance.

[Sling](https://slingdata.io) is used to extract demo Postgres data from PopSQL's public
[Postgres Database.](https://popsql.com/sql-templates/analytics/exploring-sample-dataset)
Sling writes Parquet files for DuckDB to ingest.
Sling can do both incremental and full-refreshes, for simplicity this demo only does a full refresh of data.

Data is loaded into [DuckDB](https://duckdb.org/) as both CSV and Parquet.
dbt, with the [dbt-duckdb](https://github.com/jwills/dbt-duckdb) adapter
is used for data transformations.

[Evidence](https://evidence.dev) is used for the presentation/BI
layer.


## Acknowledgements

None of this would've been possible without the tireless and often thankless
work of so many people in the data community, from the creators of all the
tools above, to people like Jacob Matson, Pete Fein, Josh Wills and many others.
