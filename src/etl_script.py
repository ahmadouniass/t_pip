import yfinance as yf
import pandas as pd
import boto3
import os
from datetime import datetime

# --- Configuration (utilisera les variables d'environnement dans Fargate) ---
# En local, vous pouvez les définir ici ou les exporter dans votre shell
TICKER = os.environ.get('TICKER', '^IXIC') # NASDAQ 100 (Index)
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'yf-pipeline-ensae-dev')
S3_PREFIX = 'daily_prices'

def extract_transform_data(ticker):
    """
    Étape d'extraction (E) et de transformation (T) :
    Télécharge les données, ajoute la date de traitement et nettoie.
    """
    print(f"Extraction des données pour le ticker : {ticker}...")
    
    # 1. Extraction des données historiques (les 5 dernières années)
    data = yf.download(ticker, period="5y")
    
    if data.empty:
        print(f"Erreur: Aucune donnée trouvée pour {ticker}")
        return None

    # 2. Transformation : Ajouter des métadonnées et nettoyer
    data = data.reset_index()
    data = data.rename(columns={'Date': 'trading_date'})
    
    # Ajout de la date de traitement (pour le partitionnement S3 et le suivi)
    data['processing_date'] = datetime.now().strftime('%Y-%m-%d')
    data['ticker'] = ticker
    
    print(f"Extraction et transformation réussies. {len(data)} lignes traitées.")
    return data

def load_data_to_s3(df: pd.DataFrame, bucket: str, prefix: str):
    """
    Étape de chargement (L) :
    Stocke le DataFrame sur S3 au format Parquet avec partitionnement.
    """
    s3 = boto3.client('s3')
    
    # Utilisation de la date de traitement pour le partitionnement
    date_partition = df['processing_date'].iloc[0] 
    
    # Chemin S3 final, optimisé pour l'analyse (partitionnement Hive-style)
    s3_path = f"{prefix}/date={date_partition}/{TICKER}.parquet"
    
    print(f"Chargement vers S3 à l'emplacement : s3://{bucket}/{s3_path}")
    
    # Utilisation de l'API Pandas pour écrire directement sur S3
    # Le moteur PyArrow est utilisé pour une écriture Parquet rapide et compatible
    try:
        # Stockage du DataFrame en mémoire tampon (Buffer)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        
        # Envoi du Buffer à S3
        s3.put_object(Bucket=bucket, Key=s3_path, Body=buffer.getvalue())
        
        print(f"Succès ! Données stockées sur S3.")
        
    except Exception as e:
        print(f"Échec du chargement S3 : {e}")
        raise e

if __name__ == "__main__":
    import io
    
    if BUCKET_NAME == 'yf-pipeline-ensae-dev':
        print("!!! ATTENTION : Modifiez BUCKET_NAME dans le script pour votre test local !!!")
        # On ne peut pas tester la partie S3 sans un nom de bucket valide.
        # Vous pouvez désactiver cette ligne pour un test sans S3 :
        # df = extract_transform_data(TICKER)
        # if df is not None: print(df.head())
        # exit() 
        
    df_transformed = extract_transform_data(TICKER)
    
    if df_transformed is not None:
        load_data_to_s3(df_transformed, BUCKET_NAME, S3_PREFIX)