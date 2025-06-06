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

# %% [markdown]
# # Partitioning
#
# Iceberg (and other lakehouses) don't provide indexes that you may be used to from a more traditional datawarehouse, but they do provide a concept of partitioning, which serves a similar purpose. 
#
# Partitioning refers to structuring the way the files are saved to disk in order to co-locate ranges of values. This makes it more likely that the query engine only has to read a few files to get all the requested data instead of all of them.
#
# If you haven't noticed the theme yet, it's all about eliminating as much disk I/O as possible. The less files we have to scan, the more performant our query is!
#
# Iceberg implements what they call *Hidden Partitioning*, and let's digress a little bit to the past to understand what that means.
#
# Hive implemented *Explicit partitioning*, where the user needs to be aware of the partitioning and explicitly use when reading and writing.
#
# ```{figure} images/hive_partitioning.png
# :alt: Hive-style partitioning
# :align: center
# :figwidth: image
#
# Hive-style partitioning
# ```
#
# The main issue with Hive-style partitioning is that it is explicit.
# Given this partitioning scheme, if I wanted to query a range 2024-01-01 <=> 2024-02-28 I might want to write this query
#
# ```sql
# SELECT * FROM reviews WHERE review_date between '2024-01-01' AND '2024-02-28'
# ```
#
# This query would not use the index, as Hive is explicitly expecting a year, month and date filter.
#
# ```sql
# SELECT * from reviews where year = 2024 AND (month = 1 OR month = 2) AND DAY BETWEEN 1 and 31
# ```
#
# Iceberg hides this complexity away from the user, hence **Hidden Partitioning**
#
# We could have defined our partitioning when we created the table, but like much of data engineering, we often realize later that we needed it. Predicting query patterns up-front is a big ask. 

# %%
from schema import house_prices_schema
from utils import read_house_prices, catalog, engine, get_iceberg_metadata, fs
from IPython.display import JSON
import polars as pl

# %% [markdown]
# Let's reset everything to start from a clean slate

# %%
catalog.drop_table("housing.staging_prices", purge_requested=True)

# %%
house_prices_t = catalog.create_table_if_not_exists(
    "housing.staging_prices",
    schema=house_prices_schema,
    location="s3://warehouse/staging",
)

# %% [markdown]
# ## Hidden Partitioning
#
# Iceberg defines a number of supported `transforms` - functions that Iceberg will use to map a query onto a partition. Dates are pretty common in warehouses, so Year, Month, Day transfomrs enable intelligent date-based partitioning. For keys and identifiers, Bucket and Truncate are used to ensure a distributed write pattern. 
#
# In this case, we know we're interested in date-based queries, and since we don't have a lot of daily activity, partitioning by month sounds like a good starting point.

# %%
from pyiceberg.transforms import MonthTransform, YearTransform

with house_prices_t.update_spec() as spec:
    spec.add_field("date_of_transfer", MonthTransform(), "month_date_of_transfer")

# %% [markdown]
# Let's have a look at the metadata file after the update

# %%
JSON(get_iceberg_metadata(fs, house_prices_t))

# %% [markdown]
# Now that we've setup some partitioning - let's load in our data to see what that looks like. 

# %%
import pathlib

files_to_load = sorted(list(pathlib.Path("data/house_prices/").glob("*.csv")))
files_to_load

# %% [markdown]
# We could imagine that for each monthly load, we would want to generate a tag to be easily able to roll back to a given load, so let's do that for fun :).
#
# Let's start by reading in the first file and loading it to our Iceberg table

# %%
# Load the data into Iceberg
df = read_house_prices(files_to_load[0]).to_arrow().cast(house_prices_schema.as_arrow())
house_prices_t.append(df)

year = files_to_load[0].name[3:7]
# Tag the new snapshot - retain it for a month
current_snapshot = house_prices_t.current_snapshot().snapshot_id
house_prices_t.manage_snapshots().create_tag(
    current_snapshot, f"{year}_load", max_ref_age_ms=2629746000
).commit()

# %% [markdown]
# Let's have a look at what is happening in the physical storage

# %%
fs.ls(f"{house_prices_t.location()}/data")

# %% [markdown]
# The data is now physically partitioned by year-month, and we can now use it without having to know anything about the partitioning. To show how query engines can take advantage of this, let's compare two SQL statements in Trino. 
#
# Looking at the Trino query plan, we can see that the first query is scanning twice the number of rows compared to the second

# %%
# No partition on 'county'
print(
    pl.read_database(
        "EXPLAIN ANALYZE SELECT max(price) as max_price from housing.staging_prices where county = 'WORCESTERSHIRE'",
        engine,
    ).item(0, 0)
)

# %%
# Partition on 'date_of_transfer'
print(
    pl.read_database(
        "EXPLAIN ANALYZE SELECT max(price) as max_price from housing.staging_prices where date_of_transfer between DATE '2015-01-01' AND DATE '2015-06-30'",
        engine,
    ).item(0, 0)
)

# %% [markdown]
# But how big were the files we're scanning?

# %%
fs.ls("warehouse/staging/data/month_date_of_transfer=2015-01", detail=True)

# %% [markdown]
# Around 2.7 Mb - that's not very big at all - while there is no strict guidelines, consensus is that the Parquet files should be somewhere between 128 MB and 1 GB **uncompressed**, depending on use case, as the overhead of reading many small files adds up quick. 
#
# Luckily, we can quickly change our partitioning, without having to rewrite our existing files

# %%
with house_prices_t.update_spec() as spec:
    spec.remove_field("month_date_of_transfer")
    spec.add_field("date_of_transfer", YearTransform(), "year_date_of_transfer")

# %% [markdown]
# Changing partitioning doesn't alter existing files, it only affects future files. To demonstrate let's load the next file to see the effect

# %%
# Load the data into Iceberg
df = read_house_prices(files_to_load[1]).to_arrow().cast(house_prices_schema.as_arrow())
house_prices_t.append(df)

year = files_to_load[1].name[3:7]
# Tag the new snapshot - retain it for a month
current_snapshot = house_prices_t.current_snapshot().snapshot_id
house_prices_t.manage_snapshots().create_tag(
    current_snapshot, f"{year}_load", max_ref_age_ms=2629746000
).commit()

# %% [markdown]
# Let's look at the file structure now

# %%
fs.ls(f"{house_prices_t.location()}/data", refresh=True)

# %%
fs.ls("warehouse/staging/data/year_date_of_transfer=2016", detail=True)

# %% [markdown]
# Around 23 Mb - better, parquet compresses well after all so this is closer to optimal size. We'll keep this and load the rest

# %%
for filename in files_to_load[2:]:
    # Grab the year from the filename
    year = filename.name[3:7]
    # Read in the CSV
    df = read_house_prices(filename).to_arrow().cast(house_prices_schema.as_arrow())
    print(f"Appending {filename.name} - {len(df):,} rows")
    # Write to Iceberg
    house_prices_t.append(df)
    # Get the new snapshot id
    current_snapshot = house_prices_t.current_snapshot().snapshot_id
    # Tag the new snapshot - retain it for a month
    house_prices_t.manage_snapshots().create_tag(
        current_snapshot, f"{year}_load", max_ref_age_ms=2629746000
    ).commit()
    print(f"Tagged: {year}_load")

# %%
fs.ls(f"{house_prices_t.location()}/data", refresh=True)

# %% [markdown]
# Now Iceberg has two different partitions to keep track of, so it will split the partition planning across the two partitions
#
# ![Partition Spec Evolution](images/partition_spec_evolution.png)

# %% [markdown]
# Let's re-examine our query plans to see if we can spot the difference

# %%
# No partition on 'county'
print(
    pl.read_database(
        "EXPLAIN ANALYZE SELECT max(price) as max_price from housing.staging_prices where county = 'WORCESTERSHIRE'",
        engine,
    ).item(0, 0)
)

# %%
# Partition on 'date_of_transfer'
print(
    pl.read_database(
        "EXPLAIN ANALYZE SELECT max(price) as max_price from housing.staging_prices where date_of_transfer between DATE '2015-01-01' AND DATE '2022-12-31'",
        engine,
    ).item(0, 0)
)

# %%
