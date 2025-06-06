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
# # Is your house worth it's weight in gold?
#
# Now we've calculated how much a given house rose in value over time - the question is, would you have been better off buying gold?
#
# In the `data` folder, we have the daily price of one ounce of gold in USD, called `XAUUSD` in trading terms (e`X`change rate of `AU` aka Gold to `USD`)
#
# We want to find out what would have happened if instead of buying a house, we would have bought an equivalent sum of gold. Finally we want to compare what the average profit would be if everyone had invested in gold instead of buying a house.
#
# For simplicity, use the daily Closing price.
#
# ```{note}
# Since gold is priced in USD, and our houseprices are in GBP, there is also a USD/GBP daily prices in the `data/fx` folder to convert between currencies.
# ```
#
# Breaking it down:
#
# 1. Load daily gold prices into Iceberg - for bonus points, apply a yearly partitioning scheme
# 2. Load exchange rates into Iceberg - for bonus points, apply a yearly partitioning scheme
# 3. Convert daily gold prices from USD to GBP
# 4. For each row in our `profits` table, calculate the equivalent amount of gold they would have been able to purchase on that date
# 5. Calculate the total value of that amount of gold on the sell date
# 6. Calculate the profit of our gold trade
# 7. Compare to Gold profit with Housing profit

# %%
import polars as pl
from utils import catalog, engine
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import DecimalType, DateType, StringType
from pyiceberg.partitioning import PartitionSpec, YearTransform, PartitionField
from IPython.display import display
pl.Config.set_thousands_separator(",")

# %% [markdown]
# ## 1. Load daily gold prices into Iceberg
# Start by creating the table in Iceberg. To organize things a bit better, I'm creating a new namespace `commodities` - we could imagine putting a table for oil prices or silver in here

# %%
catalog.create_namespace_if_not_exists("commodities")

gold_schema = Schema(
    NestedField(1, "date", DateType(), required=True, doc="Day of recorded price"),
    NestedField(
        2,
        "price",
        DecimalType(precision=38, scale=2),
        required=True,
        doc="Price in USD of one ounce of gold",
    ),
    identifier_field_ids=[1],
)

# %%
gold_prices_t = catalog.create_table_if_not_exists(
    "commodities.gold",
    schema=gold_schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1, transform=YearTransform(), name="date_year"
        )
    ),
)

# %% [markdown]
# Next, read in the CSV, picking out the two columns we care about, remembering to convert to the correct schema, 

# %%
gold_prices = (
    pl.scan_csv("data/gold/daily_gold_prices.csv", separator=";", try_parse_dates=True)
    .select(pl.col("Date").alias("date"), pl.col("Close").alias("price"))
    .collect()
)
gold_prices

# %%
gold_prices_t.append(gold_prices.to_arrow().cast(gold_schema.as_arrow()))

# %% [markdown]
# # 2. Load daily exchange rates into Iceberg
#
# Similar process - create a new namespace and load the fx rates. Since this is technically a table of currency pairs, we can make the schema a bit more future-proof

# %%
catalog.create_namespace_if_not_exists("fx")

fx_schema = Schema(
    NestedField(1, "date", DateType(), required=True, doc="Day of recorded price"),
    NestedField(
        2,
        "from",
        StringType(),
        required=True,
        doc="From currency code of the currency pair",
    ),
    NestedField(
        3,
        "to",
        StringType(),
        required=True,
        doc="To currency code of the currency pair",
    ),
    NestedField(
        4,
        "exchange_rate",
        DecimalType(precision=38, scale=4),
        required=True,
        doc="Exchange rate of the currency pair",
    ),
    identifier_field_ids=[1, 2, 3],
)

# %% [markdown]
# Next, reading in the CSV file and transforming slightly to conform to our schema

# %%
fx_table = catalog.create_table_if_not_exists("fx.rates", schema=fx_schema)

# %%
fx_rates = (
    pl.scan_csv("data/fx/USD_GBP.csv", try_parse_dates=True)
    .with_columns(pl.lit("USD").alias("from"), pl.lit("GBP").alias("to"))
    .select(
        pl.col("date"),
        pl.col("from"),
        pl.col("to"),
        pl.col("close").cast(pl.Decimal(38, 4)).alias("exchange_rate"),
    )
    .collect()
)
fx_rates

# %%
fx_table.append(fx_rates.to_arrow().cast(fx_schema.as_arrow()))

# %% [markdown]
# ## Business Logic
# Next comes the business logic - the actual work of figuring out how much gold we could have bought and how much that would have sold for

# %%
sql = """
-- Convert USD gold prices into GBP denominated prices
with gold_prices as (
    select commodities.gold.date as gold_date, commodities.gold.price * fx.rates.exchange_rate as gold_price
    from commodities.gold
    join fx.rates on fx.rates.date = commodities.gold.date
), gold_purchase as (
-- Calculate how much gold (in ounces) the house owner could have bought at the time of purchase
    select address_id, 
    first_price / gold_price as purchased_gold
    from housing.profits
    join gold_prices on gold_date = housing.profits.first_day
), gold_sell as (
-- Calculate how much that amount of gold is worth at the time of sale, as well as the profit
    select housing.profits.address_id,
    cast((purchased_gold * gold_price) - first_price as DECIMAL(38, 2)) as gold_profit,
    cast(profit as DECIMAL(38, 2)) as house_profit
    from housing.profits
    join gold_purchase on gold_purchase.address_id = housing.profits.address_id
    join gold_prices on gold_prices.gold_date = last_day
)
select * from gold_sell
"""

# %%
gold_vs_house_profits = pl.read_database(sql, engine)
gold_vs_house_profits

# %% [markdown]
# Now we have a per-address calculation, let's summarize the results

# %%
gold_vs_house_profits.select(pl.all().exclude("address_id")).describe()

# %% [markdown]
# How many percent would have done better buying gold than a house?

# %%
with pl.Config(set_tbl_rows=100):
    summary_df = (
        gold_vs_house_profits.select(
            pl.col("gold_profit")
            .sub(pl.col("house_profit"))
            .qcut(100, labels=[f"Q{i + 1}" for i in range(100)], include_breaks=True)
        )
        .unnest("gold_profit")
        .unique()
        .sort("breakpoint")
    )
    display(summary_df)

# %% [markdown]
# # Exercise: What does it look like in your region?
#
# This is the picture for all of the UK - what does it look like for your region?
