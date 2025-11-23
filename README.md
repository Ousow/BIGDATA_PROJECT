# **Big Data Project : U.S. Traffic Accidents Analysis (2010–2022)**

**Data Lake – Ingestion, Persistance, Insight & Dashboard Power BI**

---

# **1. Objectif du projet**

Ce projet consiste à concevoir **une architecture complète de Data Lake** permettant :

* L’ingestion de plusieurs sources de données hétérogènes
* La persistance et la transformation via un pipeline ETL
* La production d’un **dashboard interactif Power BI**
* L’analyse de facteurs expliquant la **gravité des accidents routiers aux États-Unis (2010–2022)**
* L’intégration de méthodes avancées (feature engineering, jointures spatio-temporelles…)

---

# **2. Sources de données utilisées**

Deux sources principales, de formats différents :

### **1️⃣ FARS – Fatality Analysis Reporting System (USA – DOT)**

* Format **CSV** dans des archives ZIP
* 1 fichier par année (2010–2022)
* Contient données Accident, Véhicule, Personnes, Lieux…

→ Données massives : *plusieurs centaines de millions de lignes cumulées*.

### **2️⃣ NOAA – Global Historical Climatology Network Daily**

* Source **API / téléchargement direct**
* Format **TXT & CSV**
* Données météo journalières : TMAX, TMIN, PRCP, SNOW, SNWD…

→ Utilisées pour enrichir les accidents avec des conditions météo.

---

# **3. Architecture Big Data (Data Lake)**

Le projet suit l’architecture standard **Raw → Silver → Gold → Insight**.

```
┌─────────────────────────────┐
│         Ingestion            │
│  (FARS CSV, NOAA TXT/CSV)    │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│            RAW               │
│ Données brutes non modifiées │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│           SILVER             │
│ Nettoyage, normalisation     │
│ Fusion multi-sources         │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│            GOLD              │
│ Dataset enrichi : météo +    │
│ facteurs structurels + FE     │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│           INSIGHT            │
│ Dashboard Power BI           │
└─────────────────────────────┘
```

---

# **4. Ingestion (Batch & Résiliente)**

✔ Téléchargement des archives FARS pour 2010–2022
✔ Extraction automatisée des ZIP
✔ Téléchargement NOAA + extraction
✔ Ingestion dans `data/raw/` **sans aucune modification**
✔ Pipeline résilient (continue même si une année est manquante)

Structure :

```
data/
 ├── raw/
 │    ├── 2010/
 │    ├── 2011/
 │    ├── ...
 ├── silver/
 ├── gold/
 └── insight/
```

---

# **5. Persistance & ETL (Silver Layer)**

### ✔ Scripts de fusion (Accident / Vehicle / Person)

* `merge_accident.py`
* `merge_vehicle.py`
* `merge_person.py`
* `silver_merge_all.py` (fusion complète)

### ✔ Nettoyage du dataset

* Uniformisation des colonnes
* Types & formats
* Nettoyage des valeurs manquantes
* Harmonisation Latitude/Longitude
* Création de colonnes temporelles

### ✔ Traitement NOAA

* Fusion de toutes les stations NOAA 2010–2022
* Filtre USA uniquement
* Nettoyage et typage

### ✔ Jointure météo × accidents

* Matching **par date**
* Matching **station la plus proche** (distance Haversine)
* Ajout des colonnes :

  * `TMAX`, `TMIN`
  * `PRCP`
  * `SNOW`, `SNWD`
  * `DIST_TO_STATION_KM`

### ✔ Exports Silver

```
ACCIDENT_2010_2022_cleaned.parquet
ACCIDENT_WITH_NEAREST_STATION.parquet
NOAA_ALL_2010_2022_raw.parquet
```

---

# **6. Feature Engineering (Gold Layer)**

Création de variables explicatives essentielles :

### Luminosité

* `LIGHT_COND` (day / night)

### Type de route

* `ROUTE_TYPE` (interstate / urban / rural…)

### Type de véhicule

* `VEHICLE_TYPE` (car / SUV / truck / motorcycle…)

### Type de collision

* `COLLISION_TYPE` (frontale / latérale / piéton…)

### Zone géographique

* `AREA_TYPE` (urban / rural)

### Variable cible

* `severity` (3 niveaux)

**Dataset final GOLD :**

```
GOLD_FEATURES.parquet
```

### Dataset optimisé Power BI :

```
GOLD_FEATURES_LIGHT.parquet
complement_data.csv
```

---

# **7. Insights & Dashboard Power BI**

Dashboard structuré en **4 pages**.

---

## PAGE 1 — Overview

* KPIs globaux
* Total accidents / accidents mortels
* Severity distribution
* Courbe d’évolution annuelle

---

## PAGE 2 — Analyses temporelles

* Fatalités par heure de la journée
* Répartition Day vs Night
* Accidents par mois

---

## PAGE 3 — Facteurs structurels (Insights clés)

Analyses essentielles :

* Sévérité par type de route
* Sévérité par type de véhicule
* Sévérité par type de collision
* Total accidents par type de route

➡️ Mise en évidence des **vrais facteurs explicatifs de la gravité**.

---

## PAGE 4 — Facteurs météo

* % accidents sous pluie/neige
* TMAX/TMIN vs gravité (ribbon chart)
* PRCP vs gravité

➡️ Insight majeur :
**La météo influence très peu la gravité.**

---

# **8. Résultats & Conclusions**

### La météo influence très faiblement :

* <0,05% des accidents ont pluie/neige
* Corrélation quasi nulle avec la gravité

### ✔ Les vrais facteurs explicatifs :

1. Type de route (rurales ≫ risques élevés)
2. Type de véhicule (motos très mortelles)
3. Type de collision (frontales critiques)
4. Conditions nocturnes
5. Zones rurales (temps d’accès secours)

Conclusions cohérentes avec les rapports FARS.

---

# **Technologies utilisées**

* Python (Pandas, PyArrow)
* Power BI Desktop
* NOAA & FARS datasets
* Haversine distance
* Parquet (stockage optimisé)
* CSV (Power BI optimisé)

---

# **Structure du repository GitHub**

```
accidents_bigdata/
 ├── data/
 │    ├── raw/
 │    ├── silver/
 │    ├── gold/
 │    └── insight/
 ├── scripts/
 │    ├── merge_accident.py
 │    ├── clean_accidents.py
 │    ├── join_weather_to_accidents.py
 │    ├── create_light_dataset.py
 │    ├── convert_to_csv.py
 ├── dashboard/
 │    └── US_Accidents_PowerBI.pbix
 ├── README.md
 └── requirements.txt
```

---

# Conclusion

Ce projet met en œuvre une architecture Data Lake complète, un pipeline ETL robuste et une analyse approfondie révélant les facteurs clés de la gravité des accidents aux États-Unis.
Le dashboard Power BI offre une visualisation claire, dynamique et exploitable par une équipe métier ou un comité exécutif.

---


