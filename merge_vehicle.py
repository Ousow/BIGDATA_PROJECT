import os
import pandas as pd
from glob import glob

# --- CONFIG ---
BASE_DIR = r"C:\Users\oumis\Desktop\accidents_bigdata\data\extracted"
OUTPUT_CSV = r"C:\Users\oumis\Desktop\accidents_bigdata\data\vehicle_merged.csv"
OUTPUT_PARQUET = r"C:\Users\oumis\Desktop\accidents_bigdata\data\vehicle_merged.parquet"


def get_vehicle_files(base_dir):
    """
    Récupère tous les VEHICLE.CSV dans les sous-dossiers yearly FARS.
    """
    pattern = os.path.join(base_dir, "*", "FARS*NationalCSV", "VEHICLE.CSV")
    files = glob(pattern)
    print(f"Fichiers VEHICLE trouvés : {len(files)}")
    for f in files:
        print(" -", f)
    return files


def merge_vehicle(files):
    dfs = []
    for file in files:
        try:
            print(f"Lecture : {file}")
            df = pd.read_csv(file, encoding="latin1", low_memory=False)

            # Extraire l'année depuis le nom du dossier
            year_folder = os.path.basename(os.path.dirname(file))  # ex: FARS2012NationalCSV
            year = year_folder[4:8]  # '2012'
            df["YEAR"] = year

            dfs.append(df)

        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture de {file}: {e}")

    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal VEHICLE fusionnés : {len(merged_df)}")
    return merged_df


if __name__ == "__main__":
    files = get_vehicle_files(BASE_DIR)
    merged = merge_vehicle(files)

    print("\n📌 Sauvegarde du fichier fusionné...")

    # Astuce Parquet : convertir en str pour éviter ArrowTypeError
    merged = merged.astype(str)

    merged.to_csv(OUTPUT_CSV, index=False)
    merged.to_parquet(OUTPUT_PARQUET, index=False)

    print("\n🎉 Fusion VEHICLE terminée !")
    print(f"CSV : {OUTPUT_CSV}")
    print(f"PARQUET : {OUTPUT_PARQUET}")
