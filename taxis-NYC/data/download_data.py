import pandas as pd
from sklearn.datasets import load_diabetes
from sklearn.model_selection import train_test_split
import sqlite3
import zipfile
import requests
import os
import shutil

import common

DB_PATH = common.CONFIG['paths']['db_path']
ZIP_URL = common.CONFIG['paths']['zip_url']
ZIP_PATH = common.CONFIG['paths']['zip_path']
EXTRACT_FOLDER = common.CONFIG['paths']['extract_folder']
RANDOM_STATE = int(common.CONFIG['ml']['random_state'])

def download_data():
    """Télécharge, extrait et charge les données dans une base SQLite."""
    
    # Vérifie si le dossier data existe
    if not os.path.exists(os.path.dirname(ZIP_PATH)):
        os.makedirs(os.path.dirname(ZIP_PATH))

    # Télécharger le fichier ZIP
    print("Téléchargement des données...")
    response = requests.get(ZIP_URL, stream=True)

    if response.status_code == 200:
        print("✅ Téléchargement OK !")
    else:
        print(f"❌ Erreur {response.status_code}")
    with open(ZIP_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print("Téléchargement terminé.")

    # Extraction du fichier ZIP
    print("Extraction du fichier ZIP...")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_FOLDER)
    
    print(f"Fichiers extraits dans {EXTRACT_FOLDER}")

    # Recherche d'un fichier CSV dans le dossier extrait
    csv_files = [f for f in os.listdir(EXTRACT_FOLDER) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("Aucun fichier CSV trouvé après extraction.")

    csv_path = os.path.join(EXTRACT_FOLDER, csv_files[0])
    print(f"Chargement des données depuis {csv_path}")

    # Charger le fichier CSV dans Pandas
    data = pd.read_csv(csv_path)

    # Séparer les données en train/test
    data_train, data_test = train_test_split(data, test_size=0.2, random_state=RANDOM_STATE)

    # Sauvegarde en base de données
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    print(f"Enregistrement des données dans la base SQLite : {DB_PATH}")
    with sqlite3.connect(DB_PATH) as con:
        data_train.to_sql(name='train', con=con, if_exists="replace", index=False)
        data_test.to_sql(name='test', con=con, if_exists="replace", index=False)

    print("Données sauvegardées avec succès.")

    # Suppression des fichiers après usage
    print("🧹 Nettoyage des fichiers...")
    os.remove(ZIP_PATH)  
    shutil.rmtree(EXTRACT_FOLDER, ignore_errors=True) 


    print("🚀 Nettoyage terminé, les fichiers temporaires ont été supprimés.")

    pycache_path = os.path.join(os.path.dirname(__file__), "__pycache__")
    if os.path.exists(pycache_path):
        shutil.rmtree(pycache_path)
        print("🚀 Dossier __pycache__ supprimé !")

def test_download_data():
    print(f"Reading train data from the database: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()

        # train data
        res = cur.execute("SELECT COUNT(*) FROM train")
        n_rows = res.fetchone()[0]
        res = cur.execute("SELECT * FROM train LIMIT 1")
        n_cols = len(res.description)
        print(f'Train data: {n_rows} x {n_cols}')
        # for column in res.description:
        #     print(column[0])

        # test data
        res = cur.execute("SELECT COUNT(*) FROM test")
        n_rows = res.fetchone()[0]
        res = cur.execute("SELECT * FROM test LIMIT 1")
        n_cols = len(res.description)
        print(f'Test data: {n_rows} x {n_cols}')



if __name__ == "__main__":



    download_data()
    test_download_data()

