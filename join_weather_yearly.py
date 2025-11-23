import pandas as pd
from pathlib import Path

print("\n=== JOIN WEATHER YEAR BY YEAR ===")

silver_dir = Path("data/silver")
gold_dir = Path("data/gold")
gold_dir.mkdir(parents=True, exist_ok=True)

# Load once the station mapping
acc = pd.read_parquet(silver_dir / "ACCIDENT_WITH_NEAREST_STATION.parquet")

# Rename datetime → DATE if needed
if "datetime" in acc.columns:
    acc = acc.rename(columns={"datetime": "DATE"})

acc["DATE"] = pd.to_datetime(acc["DATE"])

# Load NOAA yearly files
noaa_dir = silver_dir / "noaa"
years = range(2010, 2023)

all_outputs = []

for year in years:
    print(f"\n--- Traitement année {year} ---")

    # Subset accidents for that year
    acc_year = acc[acc["DATE"].dt.year == year]
    print(f"Accidents {year} : {len(acc_year)} lignes")

    if len(acc_year) == 0:
        continue

    # Load NOAA file for the year
    noaa_file = noaa_dir / f"NOAA_{year}.parquet"

    if not noaa_file.exists():
        print(f"❌ NOAA {year} manquant : {noaa_file}")
        continue

    noaa_year = pd.read_parquet(noaa_file)
    noaa_year["DATE"] = pd.to_datetime(noaa_year["DATE"])

    print(f"NOAA {year} : {len(noaa_year)} lignes")

    # Join
    merged = acc_year.merge(
        noaa_year,
        left_on=["STATION_ID", "DATE"],
        right_on=["ID", "DATE"],
        how="left"
    )

    merged = merged.drop(columns=["ID"])

    # Save yearly gold
    out_file = gold_dir / f"ACCIDENT_WEATHER_{year}.parquet"
    merged.to_parquet(out_file, index=False)

    print(f"✔ Année {year} sauvegardée : {out_file}")

    all_outputs.append(out_file)


print("\n=== FUSION FINALE DES FICHIERS GOLD ANNUELS ===")

# Merge yearly gold files
dfs = [pd.read_parquet(f) for f in all_outputs]
gold = pd.concat(dfs, ignore_index=True)

final_path = gold_dir / "ACCIDENT_WEATHER_GOLD.parquet"
gold.to_parquet(final_path, index=False)

print("\n✔ GOLD FINAL PRÊT !")
print(f"📁 {final_path}")
print(f"Nombre total de lignes : {len(gold)}")
