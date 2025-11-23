import pandas as pd

# --- PATHS : mets les tiens ---
ACCIDENT_PATH = r"C:\Users\oumis\Desktop\accidents_bigdata\data\accidents_merged.csv"
PERSON_PATH   = r"C:\Users\oumis\Desktop\accidents_bigdata\data\person_merged.csv"
VEHICLE_PATH  = r"C:\Users\oumis\Desktop\accidents_bigdata\data\vehicle_merged.csv"

OUTPUT_SILVER = r"C:\Users\oumis\Desktop\accidents_bigdata\data\silver_accident_person_vehicle.parquet"


print("📌 Chargement des datasets Bronze...")
acc = pd.read_csv(ACCIDENT_PATH, low_memory=False, dtype=str)
per = pd.read_csv(PERSON_PATH, low_memory=False, dtype=str)
veh = pd.read_csv(VEHICLE_PATH, low_memory=False, dtype=str)

print("ACCIDENT :", acc.shape)
print("PERSON   :", per.shape)
print("VEHICLE  :", veh.shape)


# --- JOINTURE 1 : ACCIDENT × VEHICLE ---
print("\n🔗 Jointure ACCIDENT × VEHICLE ...")

acc_veh = acc.merge(
    veh,
    on=["ST_CASE", "YEAR"],
    how="left",
    suffixes=("_ACC", "_VEH")
)

print("ACCIDENT × VEHICLE :", acc_veh.shape)


# --- JOINTURE 2 : (ACCIDENT × VEHICLE) × PERSON ---
print("\n🔗 Jointure avec PERSON ...")

silver = acc_veh.merge(
    per,
    on=["ST_CASE", "VEH_NO", "YEAR"],
    how="left",
    suffixes=("", "_PER")
)

print("Table Silver finale :", silver.shape)


# --- SAUVEGARDE ---
print("\n💾 Sauvegarde en Parquet...")

silver.to_parquet(OUTPUT_SILVER, index=False)

print(f"\n🎉 Table Silver créée : {OUTPUT_SILVER}")
