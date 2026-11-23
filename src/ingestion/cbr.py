import io
import requests
import pandas as pd
import cbrapi as cbr


def fetch_cbr_key_rate(date_from: str, date_to: str) -> pd.DataFrame:
    """
    Загрузка ключевой ставки ЦБ РФ через cbrapi.
    date_from, date_to — строки в формате YYYY-MM-DD.
    """
    series = cbr.get_key_rate(date_from, date_to)
    return series.to_frame(name="key_rate")

def fetch_cbr_currency_rates(date_req: str) -> pd.DataFrame:
    """Загрузка официальных курсов валют ЦБ РФ на дату (DD/MM/YYYY)."""
    url = "https://www.cbr.ru/scripts/XML_daily.asp"
    resp = requests.get(url, params={"date_req": date_req}, timeout=30)
    resp.raise_for_status()
    return pd.read_xml(
        io.BytesIO(resp.content),
        xpath=".//Valute",
        encoding="windows-1251",
    )