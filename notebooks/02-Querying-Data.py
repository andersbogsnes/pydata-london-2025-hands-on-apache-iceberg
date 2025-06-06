# ---
# jupyter:
#   jupytext:
#     notebook_metadata_filter: -jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import polars as pl
from pyiceberg.catalog.rest import RestCatalog

pl.Config.set_thousands_separator(",")
pl.Config.set_float_precision(2)

# %% [markdown]
# # Querying the data
#
# Now we have some data and we understand a bit better how Iceberg works under the hood. It's time to actually start using it!
#
# The key selling point for Iceberg is that we have the option of using many different query engines to read from the same data storage.
# To show this off, let's run some simple queries using a few different query engines. In this demo, we'll focus on locally runnable query engines, but often the pattern will be using something like Snowflake or Databricks for the heavy lifting, but have the optionality of using alternatives for different usecases.
#
# First, we get a reference to our table, since many of these engines are using pyiceberg as a jumping-off point, either to directly interface with the Pyiceberg table, or because we can use Pyiceberg to find out the location of the current metadata.json file

# %%
catalog = RestCatalog(
    "lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse"
)
table = catalog.load_table("housing.staging_prices")

# %% [markdown]
# ## Pyiceberg
#
# Let's see how we would use Pyiceberg directly to handle querying first. For each of these examples, we'll do something simple - we will calculate the mean monthly house price per month in 2024 (we've loaded data for 2024 and 2023 in the previous notebook). 

# %%
# %%time

iceberg_results = table.scan(
    selected_fields=["price", "date_of_transfer"],
    row_filter="date_of_transfer >= '2024-01-01' and date_of_transfer <= '2024-12-31'",
)
iceberg_results.to_polars().group_by(pl.col("date_of_transfer").dt.month()).agg(
    pl.col("price").mean()
).sort(by="date_of_transfer")

# %% [markdown]
# ## Polars
# Pyiceberg provides us with limited filtering and projection capabilities - it provides the building blocks for libraries that build on top of Pyiceberg. We used Polars to finish the job in this example, but polars can read Iceberg directly so we can avoid the extra step

# %%
# %%time
polars_df = (
    pl.scan_iceberg(table)
    .group_by(pl.col("date_of_transfer").dt.month())
    .agg(pl.col("price").mean())
    .sort(by="date_of_transfer")
    .collect()
)
polars_df

# %% [markdown]
# ## Duckdb
# Duckdb is also an excellent choice for working with Iceberg, especially if you want to stick to SQL.
#
# It does require some setup, since Duckdb doesn't yet know how to talk to the REST catalog, so it needs to have it's own credentials, but the [duckdb-iceberg](https://github.com/duckdb/duckdb-iceberg) extension recently got additional sponsorship from AWS to improve Iceberg compatibility, so keep an eye on that

# %%
import duckdb

# %%
# Create a duckdb connection
conn = duckdb.connect()
# Load the Iceberg extension for DuckDB
conn.install_extension("iceberg")
conn.load_extension("iceberg")


# To be able to read the Iceberg metadata, we need credentials for the bucket
conn.sql("""
CREATE OR REPLACE SECRET minio (
TYPE S3,
ENDPOINT 'minio:9000',
KEY_ID 'minio',
SECRET 'minio1234',
USE_SSL false,
URL_STYLE 'path'
)
""")

# %%
# %%time
# We can read the iceberg data using DuckDB
conn.sql(f"""
SELECT month(date_of_transfer) as transfer_month, mean(price) as mean_price
FROM iceberg_scan('{table.metadata_location}')
GROUP BY 1
""").show()

# %% [markdown]
# ## Trino
# Trino is another popular option, especially since AWS provides it as a serverless query engine through Athena. Trino is another SQL-based query engine, so the query looks pretty similar, just using Trino SQL dialect

# %%
import sqlalchemy as sa

engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")

sql = """
SELECT month(date_of_transfer) as transfer_month, avg(price) as mean_price 
FROM housing.staging_prices
GROUP BY 1
ORDER BY 1
"""

# %%
# %%time
pl.read_database(sql, engine)

# %% [markdown]
# ## Daft
# Daft is a relatively new player in the Dataframe world, similar to Polars, but also designed for scaling out. It's also written in Rust, but Daft has had early support for Iceberg - let's see if that helps

# %%
import daft

# %%
# %%time
(
    daft.read_iceberg(table)
    .groupby(daft.col("date_of_transfer").dt.month())
    .agg(daft.col("price").mean())
    .sort(by=daft.col("date_of_transfer"))
    .show(12)
)

# %% [markdown]
# ## Query engines
# So now we've done a tour of some of the query engines that are also easy to run locally - we've been through Python with Pyiceberg, Rust with Polars and Daft, C++ with Duckdb and finally Java with Trino. One important player we've left out here is Spark. There is no denying that Iceberg was originally a Java project and the Java Iceberg reference library is the most feature-complete. Pyiceberg is a close second though
#
# ![Iceberg Query Engines](images/iceberg_query_engine.svg)
#
# In a real enterprise setup, you'll probably a managed service like Databricks or Snowflake that you can rely on as your main Iceberg driver - but the beauty of Iceberg is that you don't have to. You can mix and match these different query engines depending on the task at hand, while not having to move the data anywhere.

# %% [markdown]
# # Exercise
#
# Try running a query using your favourite query engine to calculate the average house price for your county. If you don't live in the UK - pick the funniest sounding one. (I quite like WORCESTERSHIRE)

# %%
