import pandas as pd
from pathlib import Path

gold_file = Path("data/gold/ACCIDENT_WEATHER_GOLD.parquet")
df = pd.read_parquet(gold_file)

print("Lignes du dataset :", len(df))

# Remplacer NA dans fatals par 0 (si jamais il y en a)
df['fatals'] = df['fatals'].fillna(0).astype(int)

# Variable binaire : accident mortel ou non
df['severity_binary'] = (df['fatals'] > 0).astype(int)

# Variable multi-niveaux
def sev_level(f):
    if f == 0:
        return 0
    elif f == 1:
        return 1
    elif f == 2:
        return 2
    else:
        return 3

df['severity'] = df['fatals'].apply(sev_level)

# Sauvegarde
out = Path("data/gold/GOLD_ANALYSIS.parquet")
df.to_parquet(out, index=False)

print("✔ Severity ajoutée et dataset d'analyse enregistré.")
print(out)
