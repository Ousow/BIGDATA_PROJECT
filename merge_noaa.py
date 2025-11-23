import pandas as pd
from pathlib import Path

silver_noaa = Path("data/silver/noaa")
output_dir = Path("data/silver")
output_dir.mkdir(parents=True, exist_ok=True)

years = range(2010, 2023)

all_data = []

print("Fusion NOAA 2010–2022...\n")

for year in years:
    file_path = silver_noaa / f"NOAA_{year}.parquet"

    if not file_path.exists():
        print(f"❌ Fichier manquant : {file_path}")
        continue

    print(f"Lecture : {file_path}")
    df = pd.read_parquet(file_path)
    df["YEAR"] = year
    all_data.append(df)
    print(f"   → {len(df)} lignes")

# Fusion
if len(all_data) == 0:
    print("❌ Aucun fichier NOAA chargé !")
else:
    full_df = pd.concat(all_data, ignore_index=True)
    print("\nTaille totale fusionnée NOAA :", len(full_df))

    output_path = output_dir / "NOAA_ALL_2010_2022.parquet"
    full_df.to_parquet(output_path, index=False)
    print(f"\n✔ NOAA fusionné enregistré dans : {output_path}")
