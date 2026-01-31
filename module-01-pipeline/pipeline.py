import asyncio
import os
import pathlib
import re
import typer

import pandas as pd
import requests as rq

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthenticator
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from pandas.io import sql
from tqdm.auto import tqdm
from typing import Annotated, List


CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_USERNAME")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "dev")

TABLE_NAME = os.environ.get("TABLE_NAME", "yellow")
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
CACHE_DIR = "cache"

separated_matcher = re.compile(r"^\d+(,\d+)*$")
range_matcher = re.compile(r"^\d+-\d+$")


async def is_file_cached(file_name: str, cache_dir: str) -> bool:
    """Checks whether the file_name exists in cache dir or not"""
    # check if cache directory exists
    if not os.path.exists(cache_dir):
        return False

    # look for file name in cache directory
    for f in os.listdir(cache_dir):
        if file_name == f:
            return True

    # a file with the given name not found
    return False


async def download_file(file_name: str, cache_dir: str) -> str | None:
    path = pathlib.Path(cache_dir) / file_name
    if await is_file_cached(file_name, cache_dir):
        print(f"found file: {file_name} in cache. skipping download")
        return str(path)

    print(f"Startin donwload of: {file_name}")
    resp = await asyncio.to_thread(rq.get, URL_PREFIX + file_name)

    if resp.status_code >= 300 or resp.status_code < 200:
        print(
            f"Status code {resp.status_code} received, expected a 2xx code. aborting download of '{file_name}'."
        )
        return None

    os.makedirs(cache_dir, exist_ok=True)
    with open(path, "wb") as f:
        f.write(resp.content)
    return str(path)


async def ensure_files_exist(
    years: List[int],
    months: List[int],
    cache_dir: str,
) -> List[str]:
    tasks: List[asyncio.Task[str | None]] = []

    async with asyncio.TaskGroup() as tg:
        for year in years:
            for month in months:
                file_path = f"yellow_tripdata_{year}-{month:02d}.csv.gz"

                tasks.append(tg.create_task(download_file(file_path, cache_dir)))

    return [p for p in map(lambda t: t.result(), tasks) if p]


def parse_numbers(input_arg: str) -> List[int]:
    ranged = range_matcher.fullmatch(input_arg)
    if ranged:
        start, end = map(int, input_arg.split("-", 1))
        if start >= end:
            raise ValueError("end must be greater than start")
        return list(range(start, end + 1))

    separated = separated_matcher.fullmatch(input_arg)
    if separated:
        return list(map(int, input_arg.split(",")))

    raise ValueError(
        f"could not parse '{input_arg}' to comma-separated or ranged numbers"
    )


def main(
    years: Annotated[
        str,
        typer.Option(
            help="The years to import. accepted forms: '2021', '2019,2021', '2019-2021' "
        ),
    ] = "2021",
    months: Annotated[
        str,
        typer.Option(
            help="The months to import in each year. accepted form: same as years"
        ),
    ] = "1",
    chunck_size: Annotated[
        int, typer.Option(help="The chunck to be processed in memory at a time.")
    ] = 1_000,
    num_rows: Annotated[
        int, typer.Option(help="The maximum amount of rows from each file")
    ] = 10_000,
):
    try:
        year_list = parse_numbers(years)
    except:
        raise ValueError("could not parse the 'years' input")

    try:
        month_list = parse_numbers(months)
    except:
        raise ValueError("could not parse the 'years' input")

    for month in month_list:
        if month < 1 or month > 12:
            raise ValueError("months must be from 1 to 12")

    for year in year_list:
        # this is a bit restrictive to constraint the range to the available years at the time of writing
        # in case files are added later this will not work.
        # but this script is for demonstration purposes only.
        if year < 2019 or year > 2021:
            raise ValueError("years can only be from 2019 to 2021")

    file_paths = asyncio.get_event_loop().run_until_complete(
        ensure_files_exist(year_list, month_list, CACHE_DIR)
    )

    print(file_paths)

    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }
    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    auth_provider: PlainTextAuthenticator | None

    if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
        auth_provider = PlainTextAuthenticator(CASSANDRA_USERNAME, CASSANDRA_PASSWORD)
    else:
        auth_provider = None

    # Configure execution profile to use asyncio event loop
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    )

    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
    )
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': 1};"
    )
    session.set_keyspace(CASSANDRA_KEYSPACE)

    one_timer_executed = False
    prepared = ...
    for file_path in file_paths:
        print(f"Reading file: {file_path}")

        df_iter = pd.read_csv(
            file_path,
            iterator=True,
            chunksize=chunck_size,
            dtype=dtype,
            parse_dates=parse_dates,
            nrows=num_rows,
        )

        for _, df in enumerate(tqdm(df_iter, desc=f"{chunck_size} Chunck read")):
            if not one_timer_executed:
                one_timer_executed = True
                pandas_gen_table: str = sql.get_schema(df, name=TABLE_NAME)
                table_definition = (
                    pandas_gen_table.replace("REAL", "FLOAT")
                    .replace("INTEGER", "INT")
                    .replace(")", ", id UUID PRIMARY KEY\n)")
                )

                session.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
                session.execute(table_definition)

                query_parameterized = (
                    f'INSERT INTO {TABLE_NAME} (\n{'\n, '.join(map(lambda c: f'"{c}"', df.columns))}\n, "id"\n) VALUES '
                    + f"(\n{', '.join(['?'] * df.shape[1])}, uuid()\n)"
                )
                prepared = session.prepare(query_parameterized)

            results = session.execute_concurrent(
                ((prepared, r.values) for _, r in df.iterrows()), results_generator=True
            )

            for res in tqdm(results, desc="Inserted rows "):
                if not res.success:
                    print(f"Failure: {res}")

    session.shutdown()


if __name__ == "__main__":
    typer.run(main)
