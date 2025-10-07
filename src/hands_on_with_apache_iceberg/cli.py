import pathlib
from typing import Annotated

import httpx
import s3fs
import typer
from curl_cffi.requests.exceptions import HTTPError
from pyiceberg.exceptions import ForbiddenError
from rich.console import Console

from hands_on_with_apache_iceberg import bootstrap, download

app = typer.Typer(no_args_is_help=True)
download_app = typer.Typer(no_args_is_help=True)
app.add_typer(download_app, name="download", help="Download required data files")
console = Console()


@download_app.command("ticker")
def download_ticker(
    tickers: Annotated[list[str], typer.Argument(help="Ticker symbol to download")],
    output_dir: Annotated[
        pathlib.Path, typer.Option("--output-dir", "-o", help="Output directory")
    ] = "./data/stocks",
) -> None:
    """Download historical stock data from Yahoo Finance"""
    import yfinance as yf

    with console.status("[bold blue]Downloading ticker data...") as status:
        for ticker in tickers:
            status.update(f"[bold blue]Downloading ticker data for {ticker}...")
            ticker_data = yf.Ticker(ticker)
            output_path = output_dir.joinpath(ticker).with_suffix(".csv")
            try:
                ticker_data.history(period="max").to_csv(output_path)
                console.print(f"[bold green]✅ {ticker} data downloaded successfully")
            except HTTPError as e:
                if e.response.status_code == 404:
                    console.print(f"[red] Unknown ticker symbol: {ticker}")
                else:
                    console.print(
                        f"[red] Error downloading {ticker}: {e.response.text}"
                    )


@download_app.command("housing")
def download_data(
    start_year: Annotated[
        int, typer.Option("--start-year", "-s", help="First year of data to download")
    ] = 2015,
    end_year: Annotated[
        int, typer.Option("--end-year", "-e", help="Last year of data to download")
    ] = 2025,
    output_dir: pathlib.Path = typer.Option(
        "./data/house_prices", help="Directory to save downloaded files"
    ),
) -> None:
    """
    Download housing sales data from Gov.uk.
    """

    base_url = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    # Example file: pp-2024.csv
    urls = [f"{base_url}/pp-{year}.csv" for year in range(start_year, end_year + 1)]
    output_path = download.download_files(urls, output_dir)

    console.print(f"[green]All files downloaded successfully to {output_path}")


@app.command("bootstrap")
def bootstrap_demo() -> None:
    """Bootstrap the demo Iceberg warehouse"""
    fs = s3fs.S3FileSystem(
        endpoint_url="http://localhost:9000",
        key="minio",
        secret="minio1234",
        use_ssl=False,
    )

    with console.status("[bold blue]Creating bucket...") as status:
        bootstrap.create_bucket(fs, "warehouse")
        console.print("[bold green]✅ Bucket created successfully")

        status.update("[bold blue]Bootstrapping project...")
        with httpx.Client(base_url="http://localhost:8181") as client:
            try:
                bootstrap.bootstrap_project(client)
                console.print("[bold green]✅ Project bootstrapped successfully")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    console.print("[cyan]Catalog already bootstrapped - skipping...")
                else:
                    console.print(f"[bold red] Something went wrong, {e.response.text}")


            status.update("[bold blue]Creating warehouse...")
            try:
                bootstrap.create_warehouse(client, "warehouse")
                console.print("[bold green]✅ Warehouse created successfully")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    console.print("[cyan]Warehouse already exists - skipping...")
                else:
                    console.print(f"[bold red] Something went wrong, {e.response.text}")


@app.command("clear")
def clear_data() -> None:
    """Delete all Iceberg data to start from scratch"""
    from pyiceberg.catalog.rest import RestCatalog

    catalog = RestCatalog(
        "lakekeeper", uri="http://localhost:8181/catalog", warehouse="lakehouse"
    )
    namespaces = [n[0] for n in catalog.list_namespaces()]

    with console.status(
        "[bold blue]Deleting data...",
    ) as status:
        for namespace in namespaces:
            for table in catalog.list_tables(namespace):
                table_name = ".".join(table)
                status.update(f"Dropping table {table_name}")
                try:
                    catalog.drop_table(table, purge_requested=True)
                    console.print(f"[bold green]✅  {table_name} dropped successfully!")
                except ForbiddenError:
                    console.print(f"[bold red]❌ {table_name} not found!")
    console.print("✅ Data cleared successfully!")
