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
# # Time Travelling
#
# Another advantage of Iceberg's metadata structure is that it gives us Time Travel for free. Since all we're doing is storing snapshots and moving pointers, time travelling is essentially just asking to see the data at a previous pointer.

# %%
import sqlalchemy as sa
import polars as pl
from pyiceberg.catalog.rest import RestCatalog
from IPython.display import display
pl.Config.set_thousands_separator(',')

# %%
engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")
catalog = RestCatalog(
    "lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse"
)
house_prices_t = catalog.load_table("housing.staging_prices")

# %% [markdown]
# ## Python API vs SQL
# Pyiceberg offers us some APIs that let us inspect the table metadata - it's all Pyarrow under the hood in Pyiceberg, so we can use polars to pretty-print the dataframes

# %%
with pl.Config(thousands_separator=None):
    display(pl.from_arrow(house_prices_t.inspect.history()))

# %% [markdown]
# The SQL equivalent will depend on the query engine - Trino uses `$` as the metadata table identifier

# %%
history = pl.read_database(
    'SELECT * FROM housing."staging_prices$history" order by made_current_at', engine
)
with pl.Config(thousands_separator=None):
    display(history)

# %% [markdown]
# Now that we have a list of snapshots, we can demonstrate timetravelling. We loaded 2024, 2023 and 2022 data into our table, so we should see different counts in each snapshot

# %%
pl.read_database("SELECT count(transaction_id) as num_rows FROM housing.staging_prices", engine)

# %% [markdown]
# The time travel syntax also varies by query engine, but Trino uses the `FOR VERSION AS OF` syntax

# %%
pl.read_database(
    "SELECT count(transaction_id) as num_rows from housing.staging_prices for version as of 4406190551159350418",
    engine,
)

# %% [markdown]
# Pyiceberg exposes a similar API, where we can specify the `snapshot_id` we want to read

# %%
house_prices_t.scan(
    snapshot_id=4406190551159350418, selected_fields=["transaction_id"]
).to_arrow().num_rows

# %% [markdown]
# Since most libriaries build on Pyiceberg, you'll see similar APIs there

# %%
pl.scan_iceberg(house_prices_t, snapshot_id=4406190551159350418).select(
    pl.count("transaction_id")
).collect()

# %% [markdown]
# SQL offers us some niceties here in that we can timetravel via timestamps as well, and Trino will do the work of looking up the snapshot closest in time

# %%
pl.read_database(
    "SELECT count(transaction_id) as num_rows from housing.staging_prices for timestamp as of timestamp '2025-06-04 20:30:00'",
    engine,
)

# %% [markdown]
# Remembering these snapshot ids or pinpointing the exact time we're interested in is tricky for our human brains, so Iceberg supports tagging so that we can provide human-readable references to a given snapshot.

# %%
house_prices_t.manage_snapshots().create_tag(
    4406190551159350418, "initial commit"
).commit()

# %%
with pl.Config(thousands_separator=None):
    display(pl.from_arrow(house_prices_t.inspect.refs()))

# %% [markdown]
# Now that we have this tag, we can reference it directly in our SQL statement

# %%
pl.read_database(
    "SELECT count(transaction_id) as num_rows from housing.staging_prices for version as of 'initial commit'",
    engine,
)

# %% [markdown]
# Pyiceberg is a bit more clunky - since we need to pass a snapshot ID, we need to use Pyiceberg to lookup the snapshot_id for our tag

# %%
pl.scan_iceberg(
    house_prices_t,
    snapshot_id=house_prices_t.snapshot_by_name("initial commit").snapshot_id,
).select(pl.count("transaction_id")).collect()

# %% [markdown]
# We can permanently rollback a change, though this is not available through Pyiceberg

# %%
with engine.connect() as conn:
    conn.execute(
        sa.text(
            "ALTER TABLE housing.staging_prices EXECUTE rollback_to_snapshot(4406190551159350418)"
        )
    ).fetchone()

# %% [markdown]
# ```{warning}
# The current schema of the table remains unchanged even if we rollback. Current schema is set to include the `_loaded_at` column we added earlier
# ```

# %%
pl.read_database("SELECT count('transaction_id') as num_rows from housing.staging_prices", engine)

# %% [markdown]
# When making metadata changes in a different query engine it's important to refresh our Pyiceberg metadata, since metadata is cached

# %%
house_prices_t.refresh();

# %%
pl.scan_iceberg(house_prices_t).select(pl.col("transaction_id").len().alias('num_rows')).collect()

# %%
with pl.Config(thousands_separator=None):
    display(pl.from_arrow(house_prices_t.inspect.history()))

# %% [markdown]
# ## Cleaning up
#
# Iceberg provides various routines to clean up files and metadata as orphan files and unused data pile up. Depending on your catalogue, this may be an automated process, but we can manually trigger them via Trino

# %%
with engine.connect() as conn:
    # Remove snapshots and corresponding metadata
    conn.execute(
        sa.text(
            "ALTER TABLE housing.staging_prices EXECUTE expire_snapshots(retention_threshold => '0d')"
        )
    ).fetchone()
    # Remove orphaned files not referenced by metadata
    conn.execute(
        sa.text(
            "ALTER table housing.staging_prices execute remove_orphan_files(retention_threshold => '0d')"
        )
    ).fetchone()
    # Co-locate manifests based on partitioning
    conn.execute(
        sa.text("ALTER TABLE housing.staging_prices EXECUTE optimize_manifests")
    ).fetchone()
    # Compact small files into larger
    conn.execute(
        sa.text("ALTER table housing.staging_prices execute optimize")
    ).fetchone()

# %%
with pl.Config(thousands_separator=None):
    display(pl.read_database(
        'SELECT * FROM housing."staging_prices$history" order by made_current_at', engine
    ))
