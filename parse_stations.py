import pandas as pd
from pathlib import Path

raw_file = Path("data/raw/noaa/ghcnd-stations.txt")
output_dir = Path("data/silver/noaa")
output_dir.mkdir(parents=True, exist_ok=True)

stations = []

with open(raw_file, "r") as f:
    for line in f:
        station_id = line[0:11].strip()
        
        # on garde uniquement les stations US
        if not station_id.startswith("US"):
            continue

        lat = float(line[12:20])
        lon = float(line[21:30])
        elev = line[31:37].strip()
        elev = float(elev) if elev else None
        state = line[38:40].strip()
        name = line[41:71].strip()

        stations.append({
            "ID": station_id,
            "LAT": lat,
            "LON": lon,
            "ELEVATION": elev,
            "STATE": state,
            "NAME": name,
        })

df = pd.DataFrame(stations)

output_path = output_dir / "NOAA_STATIONS.parquet"
df.to_parquet(output_path, index=False)

print(f"✔ {len(df)} stations NOAA US sauvegardées dans : {output_path}")
