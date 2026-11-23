from src.config import load_config
from src.ingestion.moex import fetch_moex_candles
from src.ingestion.cbr import fetch_cbr_key_rate, fetch_cbr_currency_rates


def main():
    cfg = load_config("config/project.yaml")

    # 1. Данные о торгах OZON с MOEX
    print("ЗАГРУЗКА ДАННЫХ С MOEX")
    moex_df = fetch_moex_candles(
        cfg["moex"]["ticker"],
        cfg["moex"]["start"],
        cfg["moex"]["end"],
    )
    print(f"MOEX: получено {len(moex_df)} строк")
    print(f"Колонки: {list(moex_df.columns)}")
    print(moex_df.head())
    print()

    # 2. Ключевая ставка ЦБ РФ
    print("ЗАГРУЗКА КЛЮЧЕВОЙ СТАВКИ (ЦБ РФ)")
    key_rate_df = fetch_cbr_key_rate(
        cfg["cbr"]["key_rate_from"], cfg["cbr"]["key_rate_to"]
    )
    print(f"Ключевая ставка: получено {len(key_rate_df)} строк")
    print(f"Колонки: {list(key_rate_df.columns)}")
    print(key_rate_df.tail())
    print()

    # 3. Курсы валют ЦБ РФ
    print("ЗАГРУЗКА КУРСОВ ВАЛЮТ (ЦБ РФ)")
    rates_df = fetch_cbr_currency_rates(cfg["cbr"]["date_req"])
    print(f"Курсы валют: получено {len(rates_df)} строк")
    print(f"Колонки: {list(rates_df.columns)}")
    print(rates_df[["CharCode", "Name", "Value"]].head())
    print()
    print("\n...done")



if __name__ == "__main__":
    main()