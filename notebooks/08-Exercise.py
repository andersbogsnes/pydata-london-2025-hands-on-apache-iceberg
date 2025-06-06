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
# # Exercise: Is your house worth it's weight in <Blank>?
#
# We can use the same approach to compare the profits with any other asset. 
#
# In the `data` folder, you will find the daily price of various other assets. In the `stocks` folder we have daily stock price data for a few different companies:
# - AAPL == Apple
# - GRG == Greggs
# - JWD == Wetherspoons
# - NDA == Nordea
# - NVO == Novo Nordisk
# - SPY == S&P 500
#
# Get creative - as long as you can find daily price data of something, you can run a comparison
#
# The steps are similar to our gold question - load the source data into Iceberg and join it with the profits data
# Your task is to load the data into Iceberg and then we want to find out what would have happened if instead of buying a house, we would have bought an equivalent sum of gold. Finally we want to compare what the average profit would be if everyone had invested in gold instead of buying a house.
#
# For simplicity, use the daily Closing price.
#
# Breaking it down:
#
# 1. Load daily asset prices into Iceberg - for bonus points, apply a yearly partitioning scheme
# 2. For each row in our `profits` table, calculate the equivalent amount of the asset they would have been able to purchase on that date
# 3. Calculate the total value of that amount of the asset on the sell date
# 4. Calculate the profit of our asset trade
# 5. Compare asset profit with housing profit
#
# Use your favourite query engine!

# %%
