import pyarrow.parquet as pq
import pyarrow as pa
import os

# ---- INPUT & OUTPUT ----
INPUT_PATH = "data/gold/GOLD_FEATURES.parquet"
OUTPUT_PATH = "data/gold/GOLD_FEATURES_COMPLEMENT.parquet"

# ---- COLONNES À EXTRAIRE POUR POWER BI ----
COLUMNS = [
    # Facteurs structurels
    "ROUTE_TYPE", "ROUTE_TYPE_RAW",
    "VEHICLE_TYPE", "VEHICLE_TYPE_RAW",
    "COLLISION_TYPE", "COLLISION_RAW",
    "AREA_TYPE", "URBAN_RAW", "severity", "DATE"
]

print("Ouverture du fichier GOLD_FEATURES…")
parquet_file = pq.ParquetFile(INPUT_PATH)

# Writer initialisé plus tard
writer = None

print(f"Nombre de row groups : {parquet_file.num_row_groups}")

# ---- TRAITEMENT PAR MORCEAUX (aucun dépassement mémoire) ----
for i in range(parquet_file.num_row_groups):
    print(f"Lecture row group {i+1}/{parquet_file.num_row_groups}…")

    # Lecture d’un row group en ne gardant que les colonnes utiles
    existing_cols = [c for c in COLUMNS if c in parquet_file.schema.names]
    table = parquet_file.read_row_group(i, columns=existing_cols)

    # Création de la structure de fichier au premier passage
    if writer is None:
        writer = pq.ParquetWriter(OUTPUT_PATH, table.schema)

    # Ajout du bloc au fichier final
    writer.write_table(table)

# Fermeture du writer
if writer:
    writer.close()

print("\n✔ Nouveau fichier léger créé avec succès :")
print(f"➡ {OUTPUT_PATH}")
