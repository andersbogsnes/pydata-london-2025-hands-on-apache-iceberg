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

# %% marimo={"config": {"hide_code": true}}
from pyiceberg.catalog.rest import RestCatalog
import polars as pl
from IPython.display import JSON, Markdown

# %% [markdown] marimo={"config": {"hide_code": true}}
# # Hands-on with Apache Iceberg
#
# Welcome to this hands-on demo of working with Apache Iceberg. In this demo, our motivating task is to answer the question:
#
# **"Is my house worth its weight in gold?"**
#
# We will go through some fundamentals of working with Iceberg, and then work our way towards answering this important question. There will be some exercises along the way, so you can get some practice. That also means, you need to remember to execute every cell!
#
# The data we will be working with today is the **Price Paid Dataset** (aka PPD) which is published on `gov.uk`. The description of the website states
#
# > Our Price Paid Data includes information on all property sales in England and Wales that are sold for value and are lodged with us for registration.
#
# As per the license terms, we include the proper attribution and a link to OGL [here](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
# > Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.
#
#
# ```{note}
# Before we get started, make sure you've run the following if you want to follow along:
# - `docker compose up -d` 
# - `iceberg download housing` 
# - `iceberg bootstrap`
# ```

# %% [markdown]
# # Intro to Apache Iceberg
#
# Apache Iceberg is a specification - it merely defines how the data and metadata should be stored on disk. This is the power of standards specifications, as anyone can read and write Iceberg if they follow the specification.
#
# The specification is [fairly long and detailed](https://iceberg.apache.org/spec/), but we will walk through the core concepts to give you an understanding of what Iceberg is doing under the hood.
#
# ## Iceberg Architecture
#
# This is Iceberg - after this demo, you should walk away with a pretty good understanding of what this drawing is showing
#
#
# ```{figure} images/iceberg_architecture.svg
# :height: 500px
# :label: Iceberg Architecture
# ```
#

# %% [markdown] marimo={"config": {"hide_code": true}}
# ## The Catalog
#  
#
# We can ask it for a given table name, and it will tell us where to find it. It also tracks an "optimistic" lock on writing to a table to preserve transactionality
#
# ```{figure}images/iceberg_architecture_catalogue.svg
# :height: 500px
# :label: Architecture Catalog
# ```
#
# The choice of catalog is the first choice to make when adopting Iceberg. 
#
# Sometimes it's made for you e.g `AWS Glue` for Athena-based workloads, `Polaris/Open Catalog` for Snowflake based teams or `Unity Catalog` for the Databricks fans. 
#
# Sometimes you're looking for specific features such as `Nessie`'s focus on branching workflows, or the compatibility of `Hive Meta Store`. 
#
# Today we will be using `Lakekeeper`, a Rust implementation that is a single binary. Note that while Lakekeeper has full support for AuthN/AuthZ, for the sake of the demo, no protection is being used - don't do this at home kids!

# %%
# The warehouse was created during the bootstrapping process, it's specific to Lakekeeper
catalog = RestCatalog(
    "lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse"
)

# %% [markdown]
# In Iceberg, a catalog is responsible for any number of sets of tables, where a set of tables is grouped into a namespace. The catalogue can specify additional layers, such as the `warehouse` abstraction offered by Lakekeeper, though this is not required by the Iceberg spec.
#
# Since we're dealing with house prices, let's go with `housing` - you could imagine we would want to add other interesting housing-related datasets at a later point

# %%
catalog.create_namespace_if_not_exists("housing")

# %% [markdown] marimo={"config": {"hide_code": true}}
# ## Schema
#
# Next we need a schema. In PyIceberg, we can define the schema using Pyiceberg types

# %%
from pyiceberg.schema import Schema, NestedField, StringType, IntegerType, DateType

housing_prices_schema = Schema(
    NestedField(
        1,
        "transaction_id",
        StringType(),
        required=True,
        doc="A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.",
    ),
    NestedField(
        2,
        "price",
        IntegerType(),
        required=True,
        doc="Sale price stated on the transfer deed.",
    ),
    NestedField(
        3,
        "date_of_transfer",
        DateType(),
        required=True,
        doc="Date when the sale was completed, as stated on the transfer deed.",
    ),
    NestedField(
        4,
        "postcode",
        StringType(),
        required=True,
        doc="This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.",
    ),
    NestedField(
        5,
        "property_type",
        StringType(),
        required=True,
        doc="D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other",
    ),
    NestedField(
        6,
        "new_property",
        StringType(),
        required=True,
        doc="Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = a newly built property, N = an established residential building",
    ),
    NestedField(
        7,
        "duration",
        StringType(),
        required=True,
        doc="Relates to the tenure: F = Freehold, L= Leasehold etc. Note that HM Land Registry does not record leases of 7 years or less in the Price Paid Dataset.",
    ),
    NestedField(
        8,
        "paon",
        StringType(),
        doc="Primary Addressable Object Name. Typically the house number or name",
    ),
    NestedField(
        9,
        "saon",
        StringType(),
        doc="Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.",
    ),
    NestedField(10, "street", StringType()),
    NestedField(11, "locality", StringType()),
    NestedField(12, "town", StringType()),
    NestedField(13, "district", StringType()),
    NestedField(14, "county", StringType()),
    NestedField(
        15,
        "ppd_category_type",
        StringType(),
        doc="Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.",
    ),
    NestedField(
        16,
        "record_status",
        StringType(),
        doc="Indicates additions, changes and deletions to the records. A = Addition C = Change D = Delete",
    ),
    identifier_field_ids=[1],
)

# %% [markdown] marimo={"config": {"hide_code": true}}
# ## The Table
#
# With our schema in place, we're now ready to create the table. Here we are directly specifying the location where the table will be stored, though depending on the catalog, it can automatically assign a location.

# %%
house_prices_t = catalog.create_table_if_not_exists(
    "housing.staging_prices",
    schema=housing_prices_schema,
    location="s3://warehouse/housing/staging",
)


# %% [markdown] marimo={"config": {"hide_code": true}}
# ## The Data
# Pyiceberg relies on Apache Arrow as its Data Interchange Format, so we need to read in our CSV files and convert them to Arrow. 
#
# We need to do some light processing of the data, like setting the headers and casting the dtypes properly. 

# %%
def read_house_prices(csv_filename: str) -> pl.DataFrame:
    """Read a CSV file and return a polars Dataframe"""
    # Columns sourced from data dictionary
    house_prices_columns = [
        "transaction_id",
        "price",
        "date_of_transfer",
        "postcode",
        "property_type",
        "new_property",
        "duration",
        "paon",
        "saon",
        "street",
        "locality",
        "town",
        "district",
        "county",
        "ppd_category_type",
        "record_status",
    ]

    df = (
        pl.scan_csv(
            csv_filename,
            has_header=False,
            new_columns=house_prices_columns,
        )
        .with_columns(pl.col("date_of_transfer").str.to_date("%Y-%m-%d %H:%M"))
        .collect()
    )
    return df


house_prices_2024 = read_house_prices("data/house_prices/pp-2024.csv")
house_prices_2024

# %% [markdown] marimo={"config": {"hide_code": true}}
# ## Writing the Data
# Now that we have the data loaded, we're ready to write it out to our Iceberg table. We have 3 different strategies available to us:
#
# - append
# - overwrite
# - upsert
#
# `append` will write the data to the end of the table. 
#
# `overwrite` will delete the existing data before appending - though there is an optional `overwrite_filter` to delete a subset matching that filter before appending.
#
# `upsert` is a recent addition to Pyiceberg (0.9.0), where given a key column, it will compare the keys to existing rows to decide if a given row should be updated or inserted.
#
# We have an empty table, so let's stick to `append`.
#
# ```{note} Note on schemas
# PyIceberg is strict on the schema - by default, Polars is a bit looser, so we need to `cast` the exported polars arrow table into the same schema as we've defined - otherwise our write will be rejected.
# ```
#

# %%
# Export to arrow and cast it
house_prices_arrow = (
    house_prices_2024.to_arrow().cast(  # Export to an Arrow table
        housing_prices_schema.as_arrow()
    )  # Cast into the Iceberg schema
)
# Append data to the table
house_prices_t.append(house_prices_arrow)

# %% [markdown] marimo={"config": {"hide_code": true}}
# # Metadata - the secret of Iceberg
#
# Now that we've created a schema for our houseprices, let's take a look at the metadata that we've created. In Iceberg, all the metadata is stored in a combination of JSON and Avro, and all the metadata is stored in the S3 buckets directly, which is what makes it accessible from the various query engines.
#
# Let's have a look at the different files we've created out of the box. First, we need something that can talk to S3 - in this case our Minio S3 - enter fsspec and s3fs

# %%
import s3fs

fs = s3fs.S3FileSystem(
    endpoint_url="http://minio:9000", key="minio", secret="minio1234"
)
fs.ls("/warehouse/housing/staging")

# %% [markdown] marimo={"config": {"hide_code": true}}
# Now that we have something that can read our S3 bucket in Minio, we need to know where our Iceberg Catalogue put our most recent table update. PyIceberg stores that information in the `metadata_location` of the table

# %%
house_prices_t.metadata_location

# %% [markdown] marimo={"config": {"hide_code": true}}
# That's a gzipped json file, an implementation choice from our Iceberg Rest Catalog, so we need to do some extra work to read our metadata.
#
# We've asked the Iceberg Catalogue to give us the location of the snapshot file for our `housing.staging_prices` table. 
#
# Let's have a look at the contents of the current metadata.json to get a better understanding of how Iceberg does what it does.
#
# ```{figure}images/iceberg_architecture_metadata.svg
# :height: 500px
# ```

# %%
from fsspec import AbstractFileSystem
from pyiceberg.table import Table
from typing import Any
import gzip
import json


def get_iceberg_metadata(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
    """Unzips the gzipped json and reads it into a dictionary"""
    with fs.open(table.metadata_location) as f, gzip.open(f) as g_f:
        return json.load(g_f)

JSON(get_iceberg_metadata(fs, house_prices_t))


# %% [markdown] marimo={"config": {"hide_code": true}}
# The next layer of the onion is the `manifest list` - we can find that by looking in the `snapshots` array for the current snapshot. The manifest list is stored as Avro for faster scanning so we need to convert it to a dictionary for our simple human brains to understand.
#
# ```{figure}images/iceberg_architecture_manifest_list.svg
# :height: 500px
# :label: Iceberg Manifest List
# ```

# %%
def get_iceberg_manifest_list(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
    """Fetch the manifest list for the current snapshot and convert to a list of dicts"""
    manifest_list = table.current_snapshot().manifest_list
    with fs.open(manifest_list) as f:
        return pl.read_avro(f).to_dicts()


JSON(get_iceberg_manifest_list(fs, house_prices_t))


# %% [markdown] marimo={"config": {"hide_code": true}}
# We can see that the manifest list unsurprisingly contains a list of manifests, alongside some metadata for that particular manifest. We've only written once to our Iceberg table, so we have only one manifest currently. Let's dig one level deeper and open that manifest.
#
# ```{figure}images/iceberg_architecture_manifest.svg
# :height: 500px
# :label: Iceberg Manifest
# ```

# %%
def get_iceberg_manifest(
    fs: AbstractFileSystem, table: Table
) -> list[list[dict[str, Any]]]:
    """Get the manifests from the manifest list."""
    manifest_list = get_iceberg_manifest_list(fs, table)
    manifest_lists = []
    for manifest_meta in manifest_list:
        with fs.open(manifest_meta["manifest_path"]) as m_f:
            manifest = pl.read_avro(m_f).to_dicts()
            manifest_lists.extend(manifest)
    return manifest_lists


JSON(get_iceberg_manifest(fs, house_prices_t))


# %% [markdown] marimo={"config": {"hide_code": true}}
# At the lowest level of metadata, we can see a reference to the actual data files that make up the physical data stored on disk.
#
# ```{figure}images/iceberg_architecture_data.svg
# :height: 500px
# :label: Iceberg Data Files
# ```
#
# ```{note} Iceberg vs Hive
# Note that Iceberg keeps track of the physical files of the table, unlike something like Hive, which uses a folder as a logical container for a table.
# ```
#
# We can see that the Parquet file is pretty much as we expected, and we can read it directly as any other Parquet files - Iceberg doesn't specify anything about the physical data - it just stores metadata about the files to enable all the features of Iceberg

# %%
def get_iceberg_data_file(
    fs: AbstractFileSystem, table: Table, index=0
) -> pl.DataFrame:
    """Read the data file from the `index` position in the data_file"""
    manifest = get_iceberg_manifest(fs, table)
    with fs.open(manifest[index]["data_file"]["file_path"]) as p_f:
        return pl.read_parquet(p_f)


get_iceberg_data_file(fs, house_prices_t)

# %% [markdown]
# In total, on disk, the current data comes to around 15 MB, which is pretty small, so we end up only having one data file in our manifest

# %%
get_iceberg_manifest(fs, house_prices_t)[0]["data_file"]["file_size_in_bytes"] / 1024 / 1024

# %% [markdown] marimo={"config": {"hide_code": true}}
# To see what changes in our metadata when we add more data, let's add one more month of data.

# %%
house_prices_2023 = read_house_prices("data/house_prices/pp-2023.csv")
house_prices_t.append(
    house_prices_2023.to_arrow().cast(housing_prices_schema.as_arrow())
)

# %% [markdown] marimo={"config": {"hide_code": true}}
# Now that we have some more data, and based on what we've learnt - what do you see that has changed in the metadata?

# %%
JSON(get_iceberg_metadata(fs, house_prices_t))

# %%
JSON(get_iceberg_manifest_list(fs, house_prices_t))

# %%
JSON(get_iceberg_manifest(fs, house_prices_t))

# %%
fs.ls("/warehouse/housing/staging/data")

# %% [markdown] marimo={"config": {"hide_code": true}}
# ## Concluding on Metadata
#
# All the metadata we've looked at here is stored in object storage. It's this metadata which powers all of Iceberg - if you can understand how this metadata is put together, you understand the inner workings of Iceberg.
