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
# # Data Engineering
#
# Now that we've loaded our data, it's time to do some real data engineering to answer our original question.
#
# At this stage, Iceberg fades into the background, but we're able to pick and choose query engines to perform the various steps - this is the true power of Iceberg

# %%
import sqlalchemy as sa
from utils import engine, catalog
import polars as pl
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_thousands_separator(True)

# %% [markdown]
# We want a way of identifying a given property, so hashing the address related fields seems the easiest. We'll create a dimension table for those addresses, so we can move those rows out of our final data, without losing the ability to filter later. 
# ```{note} SQL Partitioning
# Note that we're adding partitioning to our tables directly through SQL here using a WITH statement on the CREATE TABLE
# ```

# %%
dim_address_sql = """
CREATE OR REPLACE TABLE housing.dim_address 
    WITH ( partitioning = ARRAY['bucket(address_id, 10)'] )
    AS (
    SELECT DISTINCT to_hex(md5(cast(
        coalesce(paon, '') ||
        coalesce(saon, '') ||
        coalesce(street, '') ||
        coalesce(locality, '') ||
        coalesce(town, '') ||
        coalesce(district, '') ||
        coalesce(county, '') ||
        coalesce(postcode, '')
    as varbinary))) AS address_id,
      paon,
      saon,
      street,
      locality,
      town,
      district,
      county,
      postcode
FROM housing.staging_prices)
"""

# %% [markdown]
# As described in the data dictionary, the monthly files incluce a `record_status` column which indicates whether a given record is a new record or if it is deleting or updating an existing record. In moving from our staging table to our fact table, we clean our data to ensure we respect the record_status

# %%
fct_prices_sql = """
CREATE OR REPLACE TABLE housing.fct_house_prices
    WITH ( partitioning = ARRAY['year(date_of_transfer)'] ) AS (
        WITH ranked_records AS (
            SELECT *,
            ROW_NUMBER () OVER (PARTITION BY transaction_id ORDER BY month(date_of_transfer) DESC) AS rn
            FROM housing.staging_prices
    ),
    latest_records AS (
        SELECT *
        FROM ranked_records
        WHERE rn = 1
    ),
    with_address_id AS (
        SELECT to_hex(md5(cast (
                coalesce(paon, '') ||
                coalesce(saon, '') ||
                coalesce(street, '') ||
                coalesce(locality, '') ||
                coalesce(town, '') ||
                coalesce(district, '') ||
                coalesce(county, '') ||
                coalesce(postcode, '')
            as varbinary))) AS address_id,
                transaction_id,
                price,
                date_of_transfer,
                property_type,
                new_property,
                duration,
                ppd_category_type
        FROM latest_records
        WHERE record_status != 'D' and ppd_category_type = 'A'
    )
    SELECT *
    FROM with_address_id
    )
"""

# %%
with engine.begin() as conn:
    num_rows_dim_address = conn.execute(sa.text(dim_address_sql)).fetchone()[0]
    num_rows_fct_prices = conn.execute(sa.text(fct_prices_sql)).fetchone()[0]

print(f"Created dim_address with {num_rows_dim_address:,} rows")
print(f"Created fct_prices with {num_rows_fct_prices:,} rows")

# %% [markdown]
# Now that the data is loaded, we can create a Pyiceberg reference to it

# %%
fct_house_prices_t = catalog.load_table("housing.fct_house_prices")

# %% [markdown]
# For a change of pace, let's use `polars` to write our profits calculation. Some things are easier to express in SQL and some are nice to be able to do in Polars. The choice is yours!

# %%
polars_result = (
    pl.scan_iceberg(fct_house_prices_t)
    .with_columns(
        pl.col("date_of_transfer").min().over(pl.col("address_id")).alias("first_day"),
        pl.col("date_of_transfer").max().over(pl.col("address_id")).alias("last_day"),
        pl.col("price")
        .sort_by("date_of_transfer")
        .first()
        .over(pl.col("address_id"))
        .alias("first_price"),
        pl.col("price")
        .sort_by("date_of_transfer")
        .last()
        .over(pl.col("address_id"))
        .alias("last_price"),
    )
    .with_columns(
        pl.col("last_day").sub(pl.col("first_day")).dt.total_days().alias("days_held"),
        pl.col("last_price").sub(pl.col("first_price")).alias("profit"),
    )
    .filter(pl.col("days_held") != 0)
    .select(
        pl.col("address_id"),
        pl.col("first_day"),
        pl.col("last_day"),
        pl.col("first_price"),
        pl.col("last_price"),
        pl.col("days_held"),
        pl.col("profit"),
    )
    .unique()
).collect()

polars_result

# %% [markdown]
# Let's store the results in a table for future reference - since `polars` is arrow-based, we can use it to define the schema as well if we don't care as much about the details of the resulting schema

# %%
profits_t = catalog.create_table_if_not_exists("housing.profits", schema=polars_result.to_arrow().schema)

# %%
profits_t.overwrite(polars_result.to_arrow())

# %% [markdown]
# To round out the selection of query engines, we can use `daft` to query our newly created table and calculate the mean profits for a given year

# %%
import daft

(
    daft.read_iceberg(profits_t)
    .groupby(daft.col("first_day").dt.year().alias("year"))
    .agg(daft.col("profit").mean())
    .sort(daft.col("year"))
    .collect(10)
)

# %%
