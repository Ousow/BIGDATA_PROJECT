import pandas as pd
import numpy as np

INPUT_PARQUET = r"C:\Users\oumis\Desktop\accidents_bigdata\data\silver_accident_person_vehicle.parquet"
OUTPUT_PARQUET = r"C:\Users\oumis\Desktop\accidents_bigdata\data\silver_cleaned.parquet"

print("📌 Chargement des données Silver...")
df = pd.read_parquet(INPUT_PARQUET)

print("Taille initiale :", df.shape)


# -------------------------
# 1️⃣ Nettoyage des valeurs manquantes
# -------------------------
def replace_missing(x):
    if str(x).strip() in ["", " ", "nan", "NaN", "NA", "None", "-1", "99", "999"]:
        return np.nan
    return x

df = df.applymap(replace_missing)


# -------------------------
# 2️⃣ Conversion des types
# -------------------------

# Liste de colonnes numériques à convertir en int
int_columns = ["ST_CASE", "VEH_NO", "PER_NO", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]

for col in int_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


# -------------------------
# 3️⃣ Reconstruction de la date complète
# -------------------------
print("🕒 Reconstruction de la DATETIME...")

df["DATETIME"] = pd.to_datetime(
    df[["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]],
    errors="coerce"
)


# -------------------------
# 4️⃣ Nettoyage des coordonnées
# -------------------------
def clean_coord(series):
    s = pd.to_numeric(series, errors="coerce")
    s = s.where((s != 0) & (s.abs() < 1000))   # filtre valeurs absurdes
    return s

if "LATITUDE" in df.columns:
    df["LATITUDE"] = clean_coord(df["LATITUDE"])

if "LONGITUD" in df.columns:
    df["LONGITUD"] = clean_coord(df["LONGITUD"])


# -------------------------
# 5️⃣ Suppression colonnes inutiles
# -------------------------

columns_to_drop = [
    col for col in df.columns
    if col.endswith("_ACC") or col.endswith("_VEH") or col.endswith("_PER")
]

# -------------------------
# 6️⃣ Normaliser les noms de colonnes
# -------------------------

df.columns = df.columns.str.lower().str.replace(" ", "_")


# -------------------------
# 7️⃣ Sauvegarde
# -------------------------
print("💾 Sauvegarde de la table Silver nettoyée...")
df.to_parquet(OUTPUT_PARQUET, index=False)

print("🎉 Nettoyage Silver terminé !")
print("Nouveau fichier :", OUTPUT_PARQUET)
print("Taille finale :", df.shape)
