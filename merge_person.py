import os
import pandas as pd
from glob import glob

# --- CONFIG ---
BASE_DIR = r"C:\Users\oumis\Desktop\accidents_bigdata\data\extracted"
OUTPUT_CSV = r"C:\Users\oumis\Desktop\accidents_bigdata\data\person_merged.csv"
OUTPUT_PARQUET = r"C:\Users\oumis\Desktop\accidents_bigdata\data\person_merged.parquet"

def get_person_files(base_dir):
    """
    Détecte automatiquement tous les fichiers PERSON.CSV dans les sous-dossiers.
    """
    pattern = os.path.join(base_dir, "*", "FARS*NationalCSV", "PERSON.CSV")
    files = glob(pattern)
    print(f"Fichiers PERSON trouvés : {len(files)}")
    for f in files:
        print(" -", f)
    return files


def merge_person(files):
    dfs = []
    for file in files:
        try:
            print(f"Lecture : {file}")
            df = pd.read_csv(file, encoding="latin1", low_memory=False)

            # Extraire automatiquement l'année depuis le path
            year_folder = os.path.basename(os.path.dirname(file))  # ex: FARS2010NationalCSV
            year = year_folder[4:8]  # extrait "2010"
            df["YEAR"] = year

            dfs.append(df)

        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture de {file} : {e}")

    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal PERSON fusionnés : {len(merged_df)}")
    return merged_df


if __name__ == "__main__":
    files = get_person_files(BASE_DIR)
    merged = merge_person(files)

    print("\n📌 Sauvegarde du fichier fusionné...")

    # Sécurisation Parquet (PyArrow n'aime pas les types mixtes)
    merged = merged.astype(str)

    # Sauvegarde
    merged.to_csv(OUTPUT_CSV, index=False)
    merged.to_parquet(OUTPUT_PARQUET, index=False)

    print("\n🎉 Fusion PERSON terminée !")
    print(f"CSV : {OUTPUT_CSV}")
    print(f"Parquet : {OUTPUT_PARQUET}")
