# etl_script.py

from __future__ import annotations
import pandas as pd
from typing import Iterable, Any
import datetime as dt
import uuid
import os
import io

# --- ASSUMPTIONS D'IMPORTS ---

# Simuler les imports si le code est dans un seul fichier
def download_prices(tickers: list[str], interval: str, period: str) -> pd.DataFrame:
    """ Simule le téléchargement (yfinance) et retourne un DataFrame multi-indexé. """
    import yfinance as yf
    print(f"Téléchargement de {len(tickers)} tickers, interval={interval}, period={period}")
    # yfinance retourne un DF multi-indexé pour plusieurs tickers
    data = yf.download(tickers, interval=interval, period=period, group_by='ticker')
    # Pour certains marchés/tickers, yfinance renvoie un seul DF au lieu d'un DF multi-indexé
    if isinstance(data.columns, pd.MultiIndex):
        return data
    else:
        # Simplification pour compatibilité
        if len(tickers) == 1:
             return {tickers[0]: data}
        return data

def split_multi_ticker(raw_df: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    """ Sépare un DataFrame multi-indexé en un dictionnaire de DataFrames par ticker. """
    per_ticker = {}
    if isinstance(raw_df.columns, pd.MultiIndex):
        for t in tickers:
            try:
                # Extraire le DF pour le ticker, en ignorant les colonnes NaN
                df_t = raw_df[t].dropna(how='all')
                if not df_t.empty:
                    per_ticker[t] = df_t
            except KeyError:
                continue
    elif isinstance(raw_df, dict):
        return raw_df # Si download_prices retourne déjà un dict
        
    return per_ticker

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key_prefix: str, filename: str, s3_client: Any) -> str:
    """ Simule l'écriture Parquet vers S3. (Implémentation réelle requiert boto3) """
    # Initialisation de S3 client réel si non fourni (pour un environnement ECS)
    if s3_client is None:
        import boto3
        s3_client = boto3.client('s3')
        
    out_buffer = io.BytesIO()
    # ATTENTION: Utiliser index=False si l'index (Date/Datetime) a été réinitialisé en colonne 'datetime'
    df.to_parquet(out_buffer, index=False) 
    
    key = f"{key_prefix}/{filename}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
    
    uri = f"s3://{bucket}/{key}"
    return uri

# Placeholder pour la partition (basé sur le format que vous aviez)
def curated_prices_partition(curated_prefix: str, interval: str, data_date: str) -> str:
    return f"{curated_prefix}/prices/interval={interval}/data_date={data_date}"

# Placeholder pour la DQ (laissez vide pour cet exemple)
def check_ohlc_consistency(df): return pd.DataFrame()
def check_negative_volume(df): return pd.DataFrame()
def combine_issues(df1, df2): return pd.DataFrame()


# --- CONSTANTES ---
TICKERS_TO_EXTRACT = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "NVDA", "TSLA", "META", "JPM",
    "^GSPC", "^IXIC", "^DJI", "EURUSD=X", "GBPUSD=X", "BTC-USD", "ETH-USD",
]
DEFAULT_INTERVAL = "1d" # Intervalle par défaut
DEFAULT_PERIOD = "1y"  # Période par défaut


# --- FONCTION DE TRANSFORMATION (CURATED) ---
def _normalize_one(ticker: str, df: pd.DataFrame, interval: str, run_id: str, ingested_at) -> pd.DataFrame:
    # index -> datetime
    out = df.reset_index().rename(columns={"Date": "datetime"})
    if "Datetime" in out.columns:
        out = out.rename(columns={"Datetime": "datetime"})
    out.columns = [c.lower().replace(" ", "_") for c in out.columns]

    # ... (Vos vérifications de colonnes adj_close et open/high/low/close/volume) ...
    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["ticker"] = ticker
    out["interval"] = interval
    out["source"] = "yfinance"
    out["run_id"] = run_id
    out["ingested_at"] = ingested_at

    # datetime -> UTC timestamp
    out["datetime"] = pd.to_datetime(out["datetime"], utc=True, errors="coerce")
    out["data_date"] = out["datetime"].dt.date.astype("string")

    # cast numerics
    for c in ["open", "high", "low", "close", "adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")

    # drop rows without datetime and dedup
    out = out.dropna(subset=["datetime"])
    out = out.drop_duplicates(subset=["ticker", "interval", "datetime"], keep="last")

    # reorder columns
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
    # -------------------------------------
    # 1. EXTRACTION DES DONNÉES BRUTES
    # -------------------------------------
    raw_multi_df = download_prices(tickers, interval=interval, period=period)
    raw_written_uri = None

    # -------------------------------------
    # 2. ÉCRITURE DES DONNÉES BRUTES (RAW) VERS S3
    # -------------------------------------
    if not raw_multi_df.empty:
        print("Préparation et écriture des données brutes (RAW)...")
        
        # Pour le RAW, nous allons 'flatten' la structure yfinance (MultiIndex)
        if isinstance(raw_multi_df.columns, pd.MultiIndex):
              raw_df_to_save = raw_multi_df.stack(level=0).reset_index(names=['datetime', 'ticker'])
              raw_df_to_save.columns = raw_df_to_save.columns.map(lambda x: x[1].lower() if isinstance(x, tuple) else x.lower())
        else:
            raw_df_to_save = raw_multi_df.reset_index()

        # Ajout des metadata de traçabilité au RAW
        raw_df_to_save['ingested_at'] = ingested_at
        raw_df_to_save['run_id'] = run_id
        
        # Définition du chemin RAW (partitionné par intervalle)
        raw_key_prefix = f"{raw_prefix}/prices/interval={interval}"
        raw_filename = f"run_{run_id}.parquet"
        
        try:
            raw_written_uri = write_parquet_to_s3(
                raw_df_to_save, 
                bucket=bucket, 
                key_prefix=raw_key_prefix, 
                filename=raw_filename, 
                s3_client=s3_client
            )
            print(f"RAW data successfully written to: {raw_written_uri}")
        except Exception as e:
            print(f"ERREUR: Échec de l'écriture des données RAW sur S3: {e}")


    # -------------------------------------
    # 3. TRANSFORMATION (CURATED)
    # -------------------------------------
    per_ticker = split_multi_ticker(raw_multi_df, tickers)

    all_rows = []
    failed = []

    for t in tickers:
        if t not in per_ticker or per_ticker[t].empty:
            failed.append({"ticker": t, "error": "no_data_returned"})
            continue
        try:
            # Votre fonction _normalize_one crée la version CURATED (transformée)
            df_t = _normalize_one(t, per_ticker[t], interval, run_id, ingested_at)
            all_rows.append(df_t)
        except Exception as e:
            failed.append({"ticker": t, "error": str(e)})

    # Concaténation des résultats CURATED
    if not all_rows:
        final_df = pd.DataFrame(columns=["ticker","interval","datetime","data_date","open","high","low","close","adj_close","volume","source","ingested_at","run_id"])
    else:
        final_df = pd.concat(all_rows, ignore_index=True)

    # ... DQ checks (maintenu tel quel) ...
    dq1 = check_ohlc_consistency(final_df) if not final_df.empty else pd.DataFrame()
    dq2 = check_negative_volume(final_df) if not final_df.empty else pd.DataFrame()
    dq_issues = combine_issues(dq1, dq2)


    # -------------------------------------
    # 4. ÉCRITURE DES DONNÉES TRANSFORMÉES (CURATED) VERS S3
    # -------------------------------------
    written_uris = []
    if not final_df.empty:
        print("Écriture des données transformées (CURATED)...")
        
        # C'est la boucle de groupement par partition (data_date)
        for data_date, part_df in final_df.groupby("data_date"):
            
            # <<< CORRECTION CRITIQUE ICI : Supprimer les colonnes de partitionnement avant l'écriture >>>
            # Ces colonnes sont déjà présentes dans le chemin S3 (data_date et interval)
            # Les retirer du DataFrame Parquet empêche la duplication dans Glue/Athena.
            part_df_to_write = part_df.drop(columns=['interval', 'data_date'])
            
            key_prefix = curated_prices_partition(curated_prefix, interval, str(data_date))
            filename = f"part-{run_id}.parquet"
            try:
                # Écriture du DataFrame corrigé (sans les colonnes redondantes)
                uri = write_parquet_to_s3(
                    part_df_to_write, # Utilisation du DataFrame corrigé
                    bucket=bucket, 
                    key_prefix=key_prefix, 
                    filename=filename, 
                    s3_client=s3_client
                )
                written_uris.append(uri)
            except Exception as e:
                print(f"ERREUR: Échec de l'écriture CURATED pour {data_date} sur S3: {e}")
        
    return {
        "raw_uri": raw_written_uri,
        "curated_uris": written_uris,
        "dq_issues": dq_issues,
        "failed": failed,
        "data_records": len(final_df),
    }

# --- POINT D'ENTRÉE PRINCIPAL (Exemple pour l'exécution Fargate) ---
if __name__ == "__main__":
    
    # Récupération des variables d'environnement (passées par la Task Definition)
    
    # Assurez-vous que ces variables sont configurées dans votre Task Definition
    BUCKET_NAME = os.environ.get("S3_BUCKET", "yf-pipeline-ensae-dev") 
    RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
    CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")
    
    # Génération des metadata d'exécution
    RUN_ID = str(uuid.uuid4())
    INGESTED_AT = dt.datetime.utcnow().isoformat()
    
    # Exécution de la tâche
    result = run_prices_etl(
        tickers=TICKERS_TO_EXTRACT,
        interval=DEFAULT_INTERVAL,
        period=DEFAULT_PERIOD,
        bucket=BUCKET_NAME,
        raw_prefix=RAW_PREFIX,
        curated_prefix=CURATED_PREFIX,
        run_id=RUN_ID,
        ingested_at=INGESTED_AT,
        s3_client=None # Le client S3 sera initialisé à l'intérieur de write_parquet_to_s3
    )

    print("\n--- RÉSULTAT ETL ---")
    print(f"RAW File URI: {result.get('raw_uri', 'N/A')}")
    print(f"CURATED Files URI: {result.get('curated_uris', 'N/A')}")
    print(f"Enregistrements CURATED: {result.get('data_records', 0)}")
    print(f"Tickers en échec: {result.get('failed', [])}")
