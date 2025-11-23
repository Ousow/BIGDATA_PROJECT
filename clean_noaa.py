import pandas as pd
from pathlib import Path

extract_dir = Path("data/extracted/noaa")
silver_dir = Path("data/silver/noaa")
silver_dir.mkdir(parents=True, exist_ok=True)

years = range(2010, 2023)

needed_elements = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]

for year in years:
    print(f"\n--- Nettoyage NOAA {year} ---")
    file_path = extract_dir / f"{year}.csv"

    chunks = pd.read_csv(
        file_path,
        names=["ID", "DATE", "ELEMENT", "VALUE", "MFLAG", "QFLAG", "SFLAG", "OBSTIME"],
        chunksize=500000
    )

    all_chunks = []

    for chunk in chunks:
        # Garder uniquement les stations US + 5 variables + bonne qualité
        chunk = chunk[
            chunk["ELEMENT"].isin(needed_elements)
            & chunk["ID"].str.startswith("US")
            & chunk["QFLAG"].isna()
        ]

        all_chunks.append(chunk[["ID", "DATE", "ELEMENT", "VALUE"]])

    df = pd.concat(all_chunks, ignore_index=True)

    # Pivot vertical → horizontal
    df_pivot = df.pivot_table(
        index=["ID", "DATE"],
        columns="ELEMENT",
        values="VALUE",
        aggfunc="first"
    ).reset_index()

    # Convertir la date
    df_pivot["DATE"] = pd.to_datetime(df_pivot["DATE"], format="%Y%m%d")

    # Sauvegarde
    output_path = silver_dir / f"NOAA_{year}.parquet"
    df_pivot.to_parquet(output_path, index=False)

    print(f"✔ NOAA {year} nettoyé → {output_path}")
