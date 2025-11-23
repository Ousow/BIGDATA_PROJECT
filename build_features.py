import pandas as pd
from pathlib import Path

df = pd.read_parquet("data/gold/GOLD_ANALYSIS.parquet")

print("Lignes :", len(df))

# ===========================================
# 1️⃣ HEURE DU CRASH (déjà présent)
# ===========================================
# On utilise hour_acc si existant, sinon hour
if "hour_acc" in df.columns:
    df["HOUR"] = df["hour_acc"]
elif "hour" in df.columns:
    df["HOUR"] = df["hour"]
else:
    df["HOUR"] = None

# Nettoyage
df["HOUR"] = pd.to_numeric(df["HOUR"], errors="coerce").astype("Int64")

# ===========================================
# 2️⃣ LUMINOSITÉ (day / night)
# ===========================================
# Règle :
# 0–5 → nuit
# 6–18 → jour
# 19–23 → nuit

def get_light(hour):
    if hour is None or pd.isna(hour):
        return None
    if 6 <= hour <= 18:
        return "day"
    else:
        return "night"

df["LIGHT_COND"] = df["HOUR"].apply(get_light)

# ===========================================
# 3️⃣ TYPE DE ROUTE
# ===========================================
# FARS utilise la variable ROUTE (ou route_acc)
# Codes typiques :
#  1 = Interstate
#  2 = U.S Highway
#  3 = State Highway
#  4 = County Road
#  5 = Local Street
#  etc.

route_col = None
for c in df.columns:
    if "route" in c.lower():
        route_col = c
        break

if route_col:
    df["ROUTE_TYPE_RAW"] = df[route_col]
else:
    df["ROUTE_TYPE_RAW"] = None

def simplify_route(x):
    try:
        x = int(x)
    except:
        return None
    if x == 1:
        return "interstate"
    elif x == 2:
        return "us_highway"
    elif x == 3:
        return "state_highway"
    elif x == 4:
        return "county_road"
    elif x == 5:
        return "local_road"
    else:
        return "other"

df["ROUTE_TYPE"] = df["ROUTE_TYPE_RAW"].apply(simplify_route)

# ===========================================
# 4️⃣ TYPE DE VÉHICULE
# ===========================================
# FARS → variable "bodytype" ou "body_typ"

veh_col = None
for c in df.columns:
    if "body" in c.lower() and ("type" in c.lower() or "typ" in c.lower()):
        veh_col = c
        break

if veh_col:
    df["VEHICLE_TYPE_RAW"] = df[veh_col]
else:
    df["VEHICLE_TYPE_RAW"] = None

def simplify_vehicle(x):
    try:
        x = int(x)
    except:
        return "unknown"

    if x in [1, 2]:  # Motorcycle categories
        return "motorcycle"
    if 30 <= x <= 39:
        return "pickup_suv"
    if 40 <= x <= 49:
        return "van"
    if 60 <= x <= 79:
        return "truck"
    if 80 <= x <= 89:
        return "bus"
    if 1 <= x <= 29:
        return "passenger_car"
    return "other"

df["VEHICLE_TYPE"] = df["VEHICLE_TYPE_RAW"].apply(simplify_vehicle)

# ===========================================
# 5️⃣ CONFIGURATION (type de collision)
# ===========================================
# FARS → variable "relation" / "man_coll" / "manner"

col_col = None
for c in df.columns:
    if "man" in c.lower() and "coll" in c.lower():
        col_col = c
        break

if col_col:
    df["COLLISION_RAW"] = df[col_col]
else:
    df["COLLISION_RAW"] = None

def simplify_collision(x):
    try:
        x = int(x)
    except:
        return "unknown"

    if x == 1:
        return "frontal"
    if x == 2:
        return "side"
    if x == 3:
        return "rear_end"
    if x == 4:
        return "collision_with_pedestrian"
    if x == 5:
        return "rollover"
    return "other"

df["COLLISION_TYPE"] = df["COLLISION_RAW"].apply(simplify_collision)

# ===========================================
# 6️⃣ EMPLACEMENT (urbain / rural)
# ===========================================
# FARS → variable "urban" ou "urban_rural"

urb_col = None
for c in df.columns:
    if "urban" in c.lower():
        urb_col = c
        break

if urb_col:
    df["URBAN_RAW"] = df[urb_col]
else:
    df["URBAN_RAW"] = None

def simplify_urban(x):
    try:
        x = int(x)
    except:
        return None

    if x == 1:
        return "urban"
    if x == 2:
        return "rural"
    return "unknown"

df["AREA_TYPE"] = df["URBAN_RAW"].apply(simplify_urban)

# ===========================================
# SAVE DATASET WITH FEATURES
# ===========================================
output = Path("data/gold/GOLD_FEATURES.parquet")
df.to_parquet(output, index=False)

print("✔ Features créées avec succès :")
print(output)
