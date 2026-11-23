import requests
import pandas as pd

def fetch_moex_candles(ticker: str, start: str, end: str) -> pd.DataFrame:
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"boards/TQBR/securities/{ticker}/candles.json"
    )
    params = {"from": start, "till": end, "interval": 24, "iss.meta": "off"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()["candles"]
    return pd.DataFrame(payload["data"], columns=payload["columns"])