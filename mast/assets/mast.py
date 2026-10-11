""" @bruin

name: mast
type: python
connection: motherduck-bkw

materialization:
  type: table
  strategy: create+replace

description: |
  # MaStR solar installations (raw)

  Daily snapshot of small solar generating units from the German
  *Marktstammdatenregister* (MaStR) open registry, ported from the standalone
  `bkw_update/main.py` script.

  ## Upstream filter

  MaStR exposes one large paginated JSON endpoint rather than discrete tables, so
  this is a `python` asset rather than an `ingestr` asset — the selection filter
  has to travel with the request:

  - `Anzahl der Solar-Module` < 5
  - `Betriebs-Status` == 35
  - `Energieträger` == 2495 (solar)

  Records are sorted by `EinheitMeldeDatum` descending and paged 5000 at a time.
  Page 1 is fetched serially to learn the total row count, then the remaining
  pages are pulled in parallel (4 workers).

  ## Transform

  - `InbetriebnahmeDatum` and `DatumLetzteAktualisierung` are decoded from
    MaStR's `/Date(<epoch_ms>)/` encoding into real timestamps.
  - `updated_at` records when this snapshot was materialised.

  ## Materialization

  `create+replace` mirrors the original `CREATE OR REPLACE TABLE mast`, so the
  table follows upstream MaStR field additions and is never briefly empty
  between DELETE and INSERT.

custom_checks:
  - name: mast is not empty
    query: |
      SELECT COUNT(*) AS count FROM mast
    value: 1
    comparison: ">"

@bruin """

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode

import pandas as pd
import requests

BASE_URL = (
    "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/"
    "GetVerkleinerteOeffentlicheEinheitStromerzeugung"
)

FILTER_STRING = (
    "Anzahl der Solar-Module~lt~'5'"
    "~and~Betriebs-Status~eq~'35'"
    "~and~Energieträger~eq~'2495'"
)

PAGE_SIZE = 5000
MAX_WORKERS = 4
TIMEOUT_SECONDS = 60

# MaStR encodes dates as .NET style /Date(<epoch_ms>)/ strings.
DATE_PATTERN = re.compile(r"/Date\((\d+)\)/")
DATE_COLUMNS = ["InbetriebnahmeDatum", "DatumLetzteAktualisierung"]


def fetch_page(page, filter_string):
    """Fetch a single page of MaStR records and the total available row count."""
    params = {
        "sort": "EinheitMeldeDatum-desc",
        "page": page,
        "pageSize": PAGE_SIZE,
        "group": "",
        "filter": filter_string,
        "forExport": True,
    }
    query_string = urlencode(params, safe="~")

    response = requests.get(f"{BASE_URL}?{query_string}", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    return payload.get("Data", []), payload.get("Total", 0)


def fetch_all_solar_data():
    """Page through the MaStR endpoint until every matching record is collected."""
    first_page, total = fetch_page(1, FILTER_STRING)
    records = first_page
    print(f"Fetched page 1 of {total} rows")

    if total <= PAGE_SIZE:
        return records

    pages_needed = (total - 1) // PAGE_SIZE + 1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_page, page, FILTER_STRING): page
            for page in range(2, pages_needed + 1)
        }
        for future in as_completed(futures):
            page_records, _ = future.result()
            records.extend(page_records)
            print(f"Fetched {len(records)}/{total} rows")

    return records


def decode_dates(df):
    """Turn MaStR's /Date(<epoch_ms>)/ strings into timestamps, in place."""
    for column in DATE_COLUMNS:
        if column not in df.columns:
            continue
        epoch_ms = df[column].astype("string").str.extract(DATE_PATTERN, expand=False)
        df[column] = pd.to_datetime(epoch_ms.astype("Int64"), unit="ms", errors="coerce")
    return df


def materialize():
    records = fetch_all_solar_data()
    if not records:
        raise ValueError(f"MaStR returned no records for filter {FILTER_STRING!r}")

    df = decode_dates(pd.DataFrame(records))
    df["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    print(f"Materializing {len(df)} rows")
    return df