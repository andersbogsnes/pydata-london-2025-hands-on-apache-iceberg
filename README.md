# Hands on with Apache Iceberg

> What is Apache Iceberg and why should you care?

Welcome to this hands-on demo of Apache Iceberg in Python. We will go through what
Apache Iceberg is, how it works and why you should care, and of course
we will play with some real-life data.

## Prerequisites

You will need Docker and Docker Compose installed, as we will be running a number of containers
for the various backing services we need. Once the data and images are downloaded, there is no
further internet access required - the material itself is 100% offline.

### Troubleshooting Note

To run all the containers, you will need a decent amount of RAM. I would recommend at least 16GB.

Most Non-linux Docker engines limit the amount of RAM available to containers, so you will need to
adjust your Docker settings accordingly.

### Linux

See https://docs.docker.com/engine/install/ and https://docs.docker.com/compose/install/

Linux users do not need to adjust Docker settings for memory consumption since docker is running natively.

### Windows

See https://docs.docker.com/desktop/

For changing memory settings, see https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources

### MacOS

I recommend Orbstack:
See https://orbstack.dev/

For changing memory settings, see https://docs.orbstack.dev/settings#memory-limit to set the memory limit.

With Docker and Docker Compose installed, run the following which will start the required
services

```bash
docker compose up -d
```

## Installing the CLI

You'll need to download some data as well as bootstrap our catalog. For your convenience,
this project includes a CLI to do so.

#### With UV (Recommended)

Install UV with your preferred method as outlined
[here](https://docs.astral.sh/uv/getting-started/installation/) - you can now use `uv` to run the
CLI, and it will automatically create a `.venv` and install the required dependencies

```bash
uv run iceberg --help
```

#### With pip

Create and activate a virtualenv using your preferred method

Once the venv is activated, install the CLI using the following:

```bash
python -m pip install .
```

You should now be able to run

```bash
iceberg --help
```

## Bootstrapping services

We need to bootstrap our catalog and object storage

```bash 
iceberg bootstrap
``` 

## Getting the data

We are using Gov.uk's `Price Paid Data` which registers every property sale in
England and Wales. They publish monthly data starting in 1995.

https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

> Contains HM Land Registry data © Crown copyright and database right 2021.
> This data is licensed under the
> [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

### Data dictionary

Gov.uk also provides a data dictionary for this data that is a handy reference when looking at
the data
https://www.gov.uk/guidance/about-the-price-paid-data

### Download the data

```bash
iceberg download housing
```

While gov.uk has data back from 1995, by default, the CLI will download data starting in 2015,
mainly to not have to wait around 10 minutes when processing the data. If you want to explore
further back, set the `--start-year` option to the year you're interested in.

Each year is around 150-200MB, so this will take a while.

## Included Data

In the data folder, there are some pre-downloaded datasets.

### `./data/fx/USD_GBP.csv`

This data is sourced from the Wall Street Journal's historical prices page:
https://www.wsj.com/market-data/quotes/fx/USDGBP/historical-prices

### Gold Prices

Gold prices were sourced from Kaggle
https://www.kaggle.com/datasets/novandraanugrah/xauusd-gold-price-historical-data-2004-2024/data

### Stock prices

Stock ticker historical prices are sourced from Yahoo Finance using the `yfinance` package
https://finance.yahoo.com/

# Let's get started!

You'll find Jupyter Lab running at `http://localhost:8080`

## Caveat

If you find yourself running out of memory, make sure to a) increase the memory limit for Docker and b) ensure
you close each notebook when you're done with it to free up memory.

Other services:

### Minio Console

> Username: minio <br>
> Password: minio1234 <br>
> URL: http://localhost:9001

### Lakekeeper UI

> URL: http://localhost:8181

# BONUS: Iceberg Streaming

I've included a bonus notebook on Iceberg streaming support. Since this requires a few extra GBs of
Docker images (and RAM), you can opt in to it by running the following

```bash
docker compose -f compose.kafka.yaml up -d
```

This will spin up some additional services, including the UI console:

### Redpanda Console

> URL: http://localhost:8001