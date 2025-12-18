# etl_script.py (Modifié pour support multi-intervalle)

from __future__ import annotations
import pandas as pd
from typing import Iterable, Any
import datetime as dt
import uuid
import os
import io

# --- ASSUMPTIONS D'IMPORTS (Inchangés) ---
def download_prices(tickers: list[str], interval: str, period: str) -> pd.DataFrame:
    import yfinance as yf
    print(f"Téléchargement de {len(tickers)} tickers, interval={interval}, period={period}")
    data = yf.download(tickers, interval=interval, period=period, group_by='ticker')
    if isinstance(data.columns, pd.MultiIndex):
        return data
    else:
        if len(tickers) == 1:
             return {tickers[0]: data}
        return data

def split_multi_ticker(raw_df: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    per_ticker = {}
    if isinstance(raw_df.columns, pd.MultiIndex):
        for t in tickers:
            try:
                df_t = raw_df[t].dropna(how='all')
                if not df_t.empty:
                    per_ticker[t] = df_t
            except KeyError:
                continue
    elif isinstance(raw_df, dict):
        return raw_df
    return per_ticker

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key_prefix: str, filename: str, s3_client: Any) -> str:
    if s3_client is None:
        import boto3
        s3_client = boto3.client('s3')
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False) 
    key = f"{key_prefix}/{filename}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
    uri = f"s3://{bucket}/{key}"
    return uri

def curated_prices_partition(curated_prefix: str, interval: str, data_date: str) -> str:
    return f"{curated_prefix}/prices/interval={interval}/data_date={data_date}"

# Placeholders DQ (Inchangés)
def check_ohlc_consistency(df): return pd.DataFrame()
def check_negative_volume(df): return pd.DataFrame()
def combine_issues(df1, df2): return pd.DataFrame()


# --- CONSTANTES ---
TICKERS_TO_EXTRACT = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA", "TSLA", "META", "JPM",
    "^GSPC", "^IXIC", "^DJI", "EURUSD=X", "GBPUSD=X", "BTC-USD", "ETH-USD",
]

# Définition des intervalles à traiter
INTERVALS_CONFIG = {
    "1d": "2y",  # Quotidien sur 2 an
    "1h": "6mo"   # Horaire sur 2 ans (limite max yfinance pour 1h)
}

# --- FONCTION DE TRANSFORMATION (CURATED) ---
def _normalize_one(ticker: str, df: pd.DataFrame, interval: str, run_id: str, ingested_at) -> pd.DataFrame:
    # Réinitialisation de l'index pour gérer 'Date' ou 'Datetime'
    out = df.reset_index()
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "datetime"})
    elif "Datetime" in out.columns:
        out = out.rename(columns={"Datetime": "datetime"})
    
    out.columns = [c.lower().replace(" ", "_") for c in out.columns]

    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["ticker"] = ticker
    out["interval"] = interval
    out["source"] = "yfinance"
    out["run_id"] = run_id
    out["ingested_at"] = ingested_at

    out["datetime"] = pd.to_datetime(out["datetime"], utc=True, errors="coerce")
    # data_date reste le pivot de partitionnement (YYYY-MM-DD)
    out["data_date"] = out["datetime"].dt.date.astype("string")

    for c in ["open", "high", "low", "close", "adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")

    out = out.dropna(subset=["datetime"])
    out = out.drop_duplicates(subset=["ticker", "interval", "datetime"], keep="last")

    cols = ["ticker","interval","datetime","data_date","open","high","low","close","adj_close","volume",
             "source","ingested_at","run_id"]
    out = out[cols].sort_values(["ticker","datetime"])
    return out

# --- FONCTION PRINCIPALE ETL (DOUBLE ÉCRITURE S3) ---
def run_prices_etl(
    tickers: list[str],
    interval: str,
    period: str,
    bucket: str,
    raw_prefix: str,
    curated_prefix: str,
    run_id: str,
    ingested_at,
    s3_client=None,
):
    # 1. EXTRACTION
    raw_multi_df = download_prices(tickers, interval=interval, period=period)
    raw_written_uri = None

    if raw_multi_df.empty:
        return {"error": "empty_download"}

    # 2. ÉCRITURE RAW
    print(f"Préparation RAW pour interval={interval}...")
    if isinstance(raw_multi_df.columns, pd.MultiIndex):
        raw_df_to_save = raw_multi_df.stack(level=0).reset_index(names=['datetime', 'ticker'])
        raw_df_to_save.columns = raw_df_to_save.columns.map(lambda x: x[1].lower() if isinstance(x, tuple) else x.lower())
    else:
        raw_df_to_save = raw_multi_df.reset_index()

    raw_df_to_save['ingested_at'] = ingested_at
    raw_df_to_save['run_id'] = run_id
    raw_key_prefix = f"{raw_prefix}/prices/interval={interval}"
    raw_filename = f"run_{run_id}.parquet"
    
    try:
        raw_written_uri = write_parquet_to_s3(raw_df_to_save, bucket, raw_key_prefix, raw_filename, s3_client)
    except Exception as e:
        print(f"ERREUR RAW: {e}")

    # 3. TRANSFORMATION (CURATED)
    per_ticker = split_multi_ticker(raw_multi_df, tickers)
    all_rows = []
    failed = []

    for t in tickers:
        if t not in per_ticker or per_ticker[t].empty:
            failed.append({"ticker": t, "error": "no_data"})
            continue
        try:
            df_t = _normalize_one(t, per_ticker[t], interval, run_id, ingested_at)
            all_rows.append(df_t)
        except Exception as e:
            failed.append({"ticker": t, "error": str(e)})

    if not all_rows:
        return {"error": "no_curated_data"}
    
    final_df = pd.concat(all_rows, ignore_index=True)

    # 4. ÉCRITURE CURATED
    written_uris = []
    print(f"Écriture CURATED pour interval={interval}...")
    for data_date, part_df in final_df.groupby("data_date"):
        part_df_to_write = part_df.drop(columns=['interval', 'data_date'])
        key_prefix = curated_prices_partition(curated_prefix, interval, str(data_date))
        filename = f"part-{run_id}.parquet"
        try:
            uri = write_parquet_to_s3(part_df_to_write, bucket, key_prefix, filename, s3_client)
            written_uris.append(uri)
        except Exception as e:
            print(f"ERREUR CURATED {data_date}: {e}")
        
    return {
        "raw_uri": raw_written_uri,
        "curated_uris": written_uris,
        "data_records": len(final_df),
        "failed": failed
    }

# --- POINT D'ENTRÉE PRINCIPAL ---
if __name__ == "__main__":
    BUCKET_NAME = os.environ.get("S3_BUCKET", "yf-pipeline-ensae-dev") 
    RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
    CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")
    
    RUN_ID = str(uuid.uuid4())
    INGESTED_AT = dt.datetime.utcnow().isoformat()

    # Boucle sur les intervalles définis dans INTERVALS_CONFIG
    for interval, period in INTERVALS_CONFIG.items():
        print(f"\n>>> DÉMARRAGE ETL - INTERVALLE: {interval} <<<")
        result = run_prices_etl(
            tickers=TICKERS_TO_EXTRACT,
            interval=interval,
            period=period,
            bucket=BUCKET_NAME,
            raw_prefix=RAW_PREFIX,
            curated_prefix=CURATED_PREFIX,
            run_id=RUN_ID,
            ingested_at=INGESTED_AT
        )

        print(f"Résultat [{interval}]: {result.get('data_records', 0)} records, {len(result.get('failed', []))} fails")
