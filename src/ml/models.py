import pandas as pd
import numpy as np
from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
import lightgbm as lgb
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

def prepare_features(df):
    """Создание лагов и признаков."""
    df = df.sort_values("trade_date").copy()
    
    # Лаги цены
    for lag in [1, 2, 3, 5]:
        df[f"close_lag_{lag}"] = df["close_price"].shift(lag)
    
    # Лаги объема
    for lag in [1, 2]:
        df[f"volume_lag_{lag}"] = df["volume"].shift(lag)
        
    # Целевая переменная: цена на следующий день
    df["target_close"] = df["close_price"].shift(-1)
    
    # Удаляем строки с NaN (из-за лагов)
    df = df.dropna()
    return df

def train_and_forecast():
    log_id = log_etl_start("ML_FORECAST")
    engine = get_db_engine()
    
    try:
        # 1. Читаем данные из Gold
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM gold.mart_daily_metrics ORDER BY trade_date", conn)
            
        if len(df) < 20:
            print("Недостаточно данных для обучения (нужно минимум 20 дней).")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # 2. Подготовка признаков
        df_feat = prepare_features(df)
        
        feature_cols = ["close_price", "volume", "daily_return", "key_rate", "usd_rate",
                        "close_lag_1", "close_lag_2", "close_lag_3", "close_lag_5",
                        "volume_lag_1", "volume_lag_2"]
        
        X = df_feat[feature_cols]
        y = df_feat["target_close"]
        
        # 3. Валидация (TimeSeriesSplit)
        tscv = TimeSeriesSplit(n_splits=3)
        baseline_errors = []
        ml_errors = []
        
        for train_idx, test_idx in tscv.split(X):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
            
            # Baseline: прогноз = сегодняшняя цена
            baseline_pred = X_test["close_price"]
            baseline_mae = mean_absolute_error(y_test, baseline_pred)
            baseline_errors.append(baseline_mae)
            
            # ML: LightGBM
            model = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
            model.fit(X_train, y_train)
            ml_pred = model.predict(X_test)
            ml_mae = mean_absolute_error(y_test, ml_pred)
            ml_errors.append(ml_mae)
            
        print(f"Baseline MAE: {np.mean(baseline_errors):.2f}")
        print(f"ML (LightGBM) MAE: {np.mean(ml_errors):.2f}")
        
        # 4. Обучаем финальные модели на всех данных и делаем прогноз на завтра
        # Baseline
        last_close = df_feat["close_price"].iloc[-1]
        last_date = pd.to_datetime(df_feat["trade_date"].iloc[-1])
        target_date = last_date + pd.Timedelta(days=1)
        
        # ML
        final_model = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
        final_model.fit(X, y)
        last_features = X.iloc[[-1]]
        ml_forecast = final_model.predict(last_features)[0]
        
        # 5. Сохраняем прогнозы в fact_forecast
        forecast_date = pd.Timestamp.today().date()
        
        records = [
            {"forecast_date": forecast_date, "target_date": target_date.date(), "ticker": "OZON", 
             "model_name": "Baseline_Naive", "predicted_close": float(last_close), "predicted_volume": None},
            {"forecast_date": forecast_date, "target_date": target_date.date(), "ticker": "OZON", 
             "model_name": "LightGBM", "predicted_close": float(ml_forecast), "predicted_volume": None}
        ]
        
        with engine.begin() as conn:
            query = text("""
                INSERT INTO gold.fact_forecast (forecast_date, target_date, ticker, model_name, predicted_close, predicted_volume)
                VALUES (:forecast_date, :target_date, :ticker, :model_name, :predicted_close, :predicted_volume)
                ON CONFLICT (forecast_date, target_date, ticker, model_name) DO UPDATE SET
                    predicted_close = EXCLUDED.predicted_close
            """)
            conn.execute(query, records)
            
        print(f"Прогнозы сохранены на {target_date.date()}.")
        log_etl_end(log_id, "SUCCESS", 2)
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    train_and_forecast()
