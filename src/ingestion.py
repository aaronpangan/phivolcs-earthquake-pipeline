import re
from datetime import datetime, timedelta

import httpx
import polars as pl
from bs4 import BeautifulSoup

COLUMN_NAMES = ["DATE_TIME", "LAT", "LONG", "DEPTH", "MAG", "LOCATION"]


def scrape_earthquake_data() -> pl.DataFrame:
    """
    Fetches earthquake records for yesterday from PHIVOLCS
    and returns a Polars DataFrame.
    """
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d %B %Y")
    date_pattern = re.compile(rf"{yesterday_str}", re.IGNORECASE)
    url = "https://earthquake.phivolcs.dost.gov.ph/"

   
    with httpx.Client(verify=False, timeout=20) as client:
        response = client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    target_table = next(
        (t for t in soup.find_all("table") if "Latitude" in t.get_text()), None
    )

    if not target_table:
        return pl.DataFrame(schema=COLUMN_NAMES)

    data_rows = []
    for tr in target_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        date_raw = tds[0].get_text(" ", strip=True).replace("\xa0", " ")

        if date_pattern.search(date_raw):
            current_row = [
                date_raw,
                tds[1].get_text(strip=True),
                tds[2].get_text(strip=True),
                tds[3].get_text(strip=True),
                tds[4].get_text(strip=True),
                tds[5].get_text(strip=True).encode("ascii", "ignore").decode("ascii"),
            ]
            data_rows.append(current_row)

    return pl.DataFrame(data_rows, schema=COLUMN_NAMES)
