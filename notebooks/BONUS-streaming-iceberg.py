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
# # BONUS: Streaming Iceberg data
#
# An interesting pattern that is emerging currently is using Iceberg as the sink for streaming events. Iceberg is great for persisting large tables, and it's easy to consume from your existing analytical stack as we've learned today.
#
# This is called Kappa Architecture - combining streaming and batch models
#
# ```{figure}images/kappa_iceberg.svg
# ```
# A number of Streaming providers, including Confluent and Redpanda, are now offering this capabiltiy built-in, but we are using the Kafka connector in this example

# %%
from quixstreams import Application
from quixstreams.models.serializers.avro import AvroSerializer, AvroDeserializer
from quixstreams.models import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)
import httpx
import polars as pl
from utils import read_house_prices, catalog

# %% [markdown]
# # Defining our schema
#
# We need to define our housing prices Avro schema, so we can use the Avro schema registry. Our Connector will use the Schema registry to create the target Iceberg table

# %%
housing_prices_avro = {
        "type": "record",
        "name": "HousePrices",
        "namespace": "housing",
        "doc": "Schema for housing.staging_prices",
        "fields": [
            {
                "name": "transaction_id",
                "type": "string",
            },
            {
                "name": "price",
                "type": "int",
            },
            {
                "name": "date_of_transfer",
                "type": {"type": "int", "logicalType": "date"},
            },
            {
                "name": "postcode",
                "type": "string",
            },
            {
                "name": "property_type",
                "type": "string",
            },
            {
                "name": "new_property",
                "type": "string",
            },
            {
                "name": "duration",
                "type": "string",
            },
            {
                "name": "paon",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "saon",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "street", "type": ["null", "string"], "default": None},
            {"name": "locality", "type": ["null", "string"], "default": None},
            {"name": "town", "type": ["null", "string"], "default": None},
            {"name": "district", "type": ["null", "string"], "default": None},
            {"name": "county", "type": ["null", "string"], "default": None},
            {
                "name": "ppd_category_type",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "record_status",
                "type": ["null", "string"],
                "default": None,
            },
        ],
    }

# %% [markdown]
# # Setup Serialization
#
# With our Avro schema defined, we need to tell the Serializer about the schema registry so it can be uploaded.

# %%
schema_registry_client_config = SchemaRegistryClientConfig(
    url='http://schema-registry:8081'
)

# %% [markdown]
# Now we can define an Avro serializer and deserializer - the serializer needs to know the schema up front, but the deserializer can fetch it at read

# %%
serializer = AvroSerializer(housing_prices_avro ,schema_registry_client_config=schema_registry_client_config)
deserializer = AvroDeserializer(schema_registry_client_config=schema_registry_client_config)

# %% [markdown]
# Using the `quixstream` library we simplify our Kafka producing logic a bit

# %%
app = Application(broker_address="broker:29092", consumer_group="iceberg-demo")

# %%
housing_prices_topic = app.topic("housing_prices", value_serializer=serializer, value_deserializer=deserializer)

# %% [markdown]
# # Publishing Data
#
# Now we have a connection to our Kafka broker, as well as a `housing_prices` topic defined, it's time to read in some data.
#
# In our case, we are still batching data, but it would be easy to imagine we had a stream of housing purchases we would want to put on Kafka.

# %%
df = read_house_prices('data/house_prices/pp-2015.csv')

# %% [markdown]
# With our data ready, we produce our messages to Kafka

# %%
with app.get_producer() as producer:
    for line in df.to_dicts():
        message = housing_prices_topic.serialize(key=line['transaction_id'], value=line)
        producer.produce(topic=housing_prices_topic.name, key=message.key, value=message.value)

# %% [markdown]
# Now we have a bunch of messages on Kafka lying around - we would like to sink them to an Iceberg table for analytical purposes. 
#
# We've chosen to use the Iceberg Kafka connector to do this. Kafka Connect is basically a standardised application from Kafka and sinking it somewhere, or sourcing it from somewhere and putting it on Kafka.
#
# # Configuring the Connector
# Kafka Connect can be configured via API - to create a new connector task, we define the configuration in JSON and pass it to the API

# %%
connector_config = {
    "name": "housing-prices-connector",
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "iceberg.catalog.type": "rest", 
    "iceberg.catalog.uri": "http://lakekeeper:8181/catalog", # Connecting to our Catalog
    "iceberg.catalog.warehouse": "lakehouse",
    "iceberg.control.topic": "iceberg-demo-connector-control", # A topic the connector uses to keep track of what files are committed
    "iceberg.tables": "housing.streaming_prices", # The table we want to write to
    "iceberg.tables.auto-create-enabled": "true", # Should the Connector create the table?
    "iceberg.tables.evolve-schema-enabled": "true", # Should the Connector alter the table if the schema changes?
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "topics": housing_prices_topic.name, 
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "iceberg.control.commit.interval-ms": 10000 # How often should we commit data? 10 seconds for demo purposes only - default is 5 mins
}

# %%
r = httpx.put("http://connect:8083/connectors/housing-prices-connector/config", json=connector_config)
r.status_code

# %% [markdown]
# We can check the current status of the connector to make sure it's running

# %%
r = httpx.get("http://connect:8083/connectors/housing-prices-connector/status")
r.json()

# %% [markdown]
# Now we have an always-on syncer that will periodically write any new data on the Kafka topic to our Iceberg table!

# %% [markdown]
# And just to prove that we can work with the Iceberg table, just like we've been doing the whole time

# %%
table = catalog.load_table("housing.streaming_prices")

# %%
pl.scan_iceberg(table).head(10).collect()

# %%
