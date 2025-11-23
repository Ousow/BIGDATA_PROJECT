import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.neighbors import BallTree

# --- LOAD DATA ---
# Fichier principal des accidents
accidents = pd.read_parquet("data/silver_cleaned.parquet")

# Stations NOAA
stations = pd.read_parquet("data/silver/noaa/NOAA_STATIONS.parquet")

print("Accidents loaded :", len(accidents))
print("Stations loaded  :", len(stations))

# --- Prepare coordinates ---
# On utilise les colonnes correctes 'latitude' et 'longitud'
acc_latlon = np.radians(accidents[["latitude", "longitud"]].to_numpy())
stations_latlon = np.radians(stations[["LAT", "LON"]].to_numpy())

# --- Build BallTree for nearest station search ---
tree = BallTree(stations_latlon, metric="haversine")

# --- Query nearest station ---
distances, indices = tree.query(acc_latlon, k=1)

# Convert distance from radians to kilometers
dist_km = distances[:, 0] * 6371  

# --- Add results ---
accidents["STATION_ID"] = stations.iloc[indices[:, 0]]["ID"].values
accidents["DIST_TO_STATION_KM"] = dist_km

# --- Save ---
output_path = Path("data/silver/ACCIDENT_WITH_NEAREST_STATION.parquet")
accidents.to_parquet(output_path, index=False)

print("✔ Mapping accidents → nearest station DONE")
print(f"Saved to : {output_path}")
