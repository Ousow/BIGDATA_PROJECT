import pandas as pd
from pathlib import Path

years = range(2010, 2023)

raw_dir = Path("data/extracted")
output_file = Path("data/silver/ACCIDENT_2010_2022_raw.parquet")
OUTPUT_CSV = r"C:\Users\oumis\Desktop\accidents_bigdata\data\accidents_merged.csv"

all_dfs = []

print("Fusion des années :", list(years))

for year in years:
    year_dir = raw_dir / str(year)

    if not year_dir.exists():
        print(f"❌ Dossier {year_dir} introuvable.")
        continue

    print(f"\n--- Année {year} ---")

    # Chercher tous les fichiers ACCIDENT possibles
    patterns = [
        "**/ACCIDENT.CSV",
        "**/accident.csv",
        "**/ACCIDENT.TXT",
        "**/accident.txt",
        "ACCIDENT.CSV",
        "accident.csv",
        "ACCIDENT.TXT",
        "accident.CSV",
        "accident.csv",
        "accident.txt",
    ]

    accident_files = []
    for p in patterns:
        accident_files += list(year_dir.glob(p))

    if not accident_files:
        print(f"⚠️ Aucun fichier ACCIDENT trouvé pour {year}")
        continue

    # On prend le premier trouvé (normalement il n'y en a qu'un)
    accident_file = accident_files[0]
    print(f"Fichier trouvé : {accident_file}")

    # Chargement du fichier
    try:
        if accident_file.suffix.lower() == ".txt":
            df = pd.read_csv(accident_file, sep="|", low_memory=False, encoding='latin1')
        else:
            df = pd.read_csv(accident_file, low_memory=False, encoding='latin1')
    except Exception as e:
        print(f"❌ Erreur lecture {accident_file}: {e}")
        continue

    df["YEAR"] = year
    all_dfs.append(df)

# Fusion finale
if all_dfs:
    merged = pd.concat(all_dfs, ignore_index=True)

    # Convertir toutes les colonnes object en str pour éviter les erreurs Parquet
    for col in merged.select_dtypes(include='object').columns:
        merged[col] = merged[col].astype(str)

    merged.to_parquet(output_file, index=False)
    merged.to_csv(OUTPUT_CSV, index=False)

    print(f"CSV : {OUTPUT_CSV}")
    print(f"\n✔ Fusion terminée ! {len(merged)} lignes")
    print(f"📁 Fichier : {output_file}")
else:
    print("❌ Aucun fichier accident n'a été fusionné.")
