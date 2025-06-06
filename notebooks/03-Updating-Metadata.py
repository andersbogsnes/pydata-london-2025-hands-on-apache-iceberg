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
from pyiceberg.catalog.rest import RestCatalog
import polars as pl
import datetime as dt
import sqlalchemy as sa

# The functions we defined in the previous notebook are defined in utils.py
from utils import get_iceberg_metadata, read_house_prices
from s3fs import S3FileSystem
from IPython.display import JSON

# %% [markdown] marimo={"config": {"hide_code": true}}
# # Updating Metadata
#
# We've added data to our tables and inspected how Iceberg keeps track of the data in the metadata files
#
# Usually when working with data in real life, we make decisions that we regret in six months time. 
#
# Now that we've added some data, we've found out that we've made a mistake - we should have added a `_loaded_at` column to our data, so that we can differentiate downstream between the source timestamp and our loaded time

# %%
# Get a reference to our catalog and table again
catalog = RestCatalog(
    "lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse"
)
house_prices_t = catalog.load_table("housing.staging_prices")
fs = S3FileSystem(endpoint_url="http://minio:9000", key="minio", secret="minio1234")

# %%
timestamp = dt.datetime.now(tz=dt.UTC)
house_prices_2022 = read_house_prices("data/house_prices/pp-2022.csv").with_columns(
    pl.lit(timestamp).alias("_loaded_at")
)
house_prices_2022

# %%
try:
    house_prices_t.upsert(house_prices_2022.to_arrow())
except ValueError as e:
    # Print out the error message instead of crashing
    print(e.args[0])

# %% [markdown] marimo={"config": {"hide_code": true}}
# Pyiceberg is preventing us from doing something we shouldn't - Iceberg has a fixed schema, so we can't just add arbitrary columns to it. We need to update the schema to accomodate our new column.
#
# ```{note}
# Pyiceberg gives us the ability to do this within a transaction to live up to Iceberg's ACID guarantees.
# ```
# The new schema is added to the Iceberg metadata in the `schemas` array. Note that each of our snapshots reference the schema at the time the data was written. That way Iceberg can keep track of the schema evolution.

# %%
from pyiceberg.types import TimestamptzType

with house_prices_t.update_schema() as schema:
    schema.add_column(
        "_loaded_at", TimestamptzType(), doc="The date this row was loaded"
    )

# %% [markdown]
# Looking at our metadata again - what's changed?

# %%
JSON(get_iceberg_metadata(fs, house_prices_t))

# %% [markdown]
# Now we have our `_loaded_at` column as part of the table schema, Iceberg is happy for us to add our data with the new column

# %%
house_prices_t.append(
    house_prices_2022.to_arrow().cast(house_prices_t.schema().as_arrow())
)

JSON(get_iceberg_metadata(fs, house_prices_t))

# %% [markdown]
# What about the data we already added? How would we modify that data? Here we start running into some limitations of a foundational library like `pyiceberg` - we can do it (bonus homework - how would you do it in pyiceberg natively?), but wouldn't it be much easier to write an `UPDATE` in SQL and not have to worry about the details?
#
# This is the power of Iceberg - we have the ability to switch query engines to suit our usecase - in this case, I want to use Trino to update the data back in time.
#
# Let's verify how many nulls we have - the data we just added should have `_loaded_at` filled in, but the rest should be null.

# %%
engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")

# %%
null_count_sql = """
SELECT
    COUNT(*) as total_rows,
    COUNT_IF(_loaded_at IS NULL) as null_count
FROM housing.staging_prices
    """
pl.read_database(null_count_sql, engine)

# %% [markdown]
# A simple UPDATE in SQL saves us many lines of Python code

# %%
with engine.connect() as conn:
    sql = f"UPDATE housing.staging_prices SET _loaded_at = from_iso8601_timestamp('{timestamp.isoformat()}') WHERE _loaded_at is null"
    result = conn.execute(sa.text(sql))
    print(f"Updated {result.scalar_one():,} rows")

# %%
pl.read_database(null_count_sql, engine)

# %% [markdown]
# We should now have a new snapshot - let's have a peek.
#
# ```{warning} Keep metadata in sync
# Pyiceberg doesn't yet know about our Trino update - we need to refresh the metadata to get the latest metadata from the catalog
# ```

# %%
house_prices_t.refresh();

# %%
JSON(get_iceberg_metadata(fs, house_prices_t))

# %% [markdown]
# ## Deletes
#
# We have a new operation `overwrite` - Parquet is immutable, so we have to physically write out a new file and delete the old one. That is expensive, so Iceberg uses delete files to avoid having to up-front do the work of actually deleting data.
#
# ```{note} Aside
# Technically, Parquet **row groups** are immutable, but it's much faster to treat the Parquet file as immutable, rather than rewriting row groups
# ```
#
# The Iceberg V2 spec defines positional-deletes and equality-deletes. These are both represented by a `delete` file, which is just a parquet file which specifies rows to mark as deleted, either by a filter like `transaction_id = '{045A1898-4ABF-9A24-E063-4804A8C048EA}'` or by position, like this:
# ```{code} parquet
# :filename: some_random_id.parquet
# file_path,pos
# s3://warehouse/house_prices/raw/data/00000-0-0ab09c23-d71c-4686-968a-f5ebd7b2e32a.parquet,0
# s3://warehouse/house_prices/raw/data/00000-0-0ab09c23-d71c-4686-968a-f5ebd7b2e32a.parquet,1
# s3://warehouse/house_prices/raw/data/00000-0-0ab09c23-d71c-4686-968a-f5ebd7b2e32a.parquet,2
# s3://warehouse/house_prices/raw/data/00000-0-0ab09c23-d71c-4686-968a-f5ebd7b2e32a.parquet,3
# ```
#
# ```{warning} Deprecation Warning
# Positional deletes will be replaced by deletion vectors in Iceberg V3
# ```

# %% [markdown]
# Let's use pyiceberg to find a delete file and open it up

# %%
delete_file = (
    pl.from_arrow(house_prices_t.inspect.delete_files())
    .select(pl.col("file_path"))
    .item(0, 0)
)
with pl.Config(fmt_str_lengths=100):
    display(pl.read_parquet(fs.read_bytes(delete_file)))

# %% [markdown]
# # Renaming and moving columns
#
# Iceberg implements all column references use the `field_id`. This makes it trivial to rename a column, since we just have to update the metadata of the schema. Imagine our style guide is updated and now all metadata fields such as our `_loaded_at` should now be prefixed with `dwh` to make it clear who did the load. Now that we have some hands-on user feedback, we also want to move `transfer_date` to be the first column since we're often visually exploring date ranges.
#
# We can also show off transactions - everything we've done until now has actually been done inside a transaction. We can explicitly open a transaction to perform multiple operations inside a single transaction. This includes deleting and adding files, but for now we'll just make our changes
#
#

# %%
with house_prices_t.transaction() as transaction:
    with transaction.update_schema() as update:
        update.rename_column("_loaded_at", "_dwh_loaded_at")
        update.move_first("date_of_transfer")

# %%
pl.scan_iceberg(house_prices_t).head().collect()

# %%
JSON(get_iceberg_metadata(fs, house_prices_t))
