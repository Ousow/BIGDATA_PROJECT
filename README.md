# Big Data Project : U.S. Traffic Accidents Analysis (2010–2022)**

**Data Lake – Ingestion, Persistance, Insight & Dashboard Power BI**

---

# 📌 **1. Objectif du projet**

Ce projet consiste à concevoir **une architecture complète de Data Lake** permettant :

* L’ingestion de plusieurs sources de données hétérogènes
* La persistance et la transformation via un pipeline ETL
* La production d’un **dashboard interactif Power BI**
* L’analyse de facteurs expliquant la **gravité des accidents routiers aux États-Unis (2010–2022)**
* L’intégration de méthodes d'analyse avancées (feature engineering, jointures spatio-temporelles, etc.)

---

# 📂 **2. Sources de données utilisées**

Deux sources principales, de formats différents :

### **1️⃣ FARS – Fatality Analysis Reporting System (USA – DOT)**

* Données disponibles au format **CSV** compressé (ZIP)
* 1 fichier par année (2010–2022)
* Contient les informations sur les accidents, véhicules, lieux, mortalité…

→ Données historiques volumineuses (Big Data)

### **2️⃣ NOAA – Global Historical Climatology Network Daily**

* Source **API / HTTP** en téléchargement direct
* Format **TXT & CSV**
* Données météo journalières (TMAX, TMIN, PRCP, SNOW…)

→ Données météorologiques pour enrichissement externe

---

# 🏗️ **3. Architecture Big Data (Data Lake)**

Le projet suit une architecture **Raw → Silver → Gold → Insight**, conforme aux standards Data Engineering.

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
│ Dataset enrichi :            │
│ météo + facteurs structurels │
│ + feature engineering        │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│         INSIGHT              │
│ Dashboard Power BI           │
│ Analyses, visualisations     │
└─────────────────────────────┘
```

---

# 🚀 **4. Ingestion (Batch & Résiliente)**

### ✔ Téléchargement manuel + automatisation année par année

### ✔ Décompression des ZIP automatiquement

### ✔ Ingestion dans `data/raw/`

### ✔ Aucun traitement appliqué aux fichiers bruts

### ✔ Architecture résiliente : si un fichier manque, l’ingestion continue

Exemple de structure :

```
data/
 ├── raw/
 │    └── 2010/
 │    └── 2011/
 │    └── ...
 ├── silver/
 ├── gold/
 └── insight/
```

---

# 🔧 **5. Persistance & ETL (Silver Layer)**

### Étapes effectuées :

### ✔ Fusion des fichiers annuels FARS (2010–2022)

→ Script `merge_accident.py`

### ✔ Nettoyage des colonnes, normalisation, typage

* Harmonisation des formats
* Création des colonnes dates, heures, géolocalisation
* Gestion des valeurs manquantes

### ✔ Extraction et filtrage des stations NOAA (USA uniquement)

### ✔ Jointure spatio-temporelle Accidents × Météo

* Correspondance par date
* Station météo la plus proche via distance Haversine
* Variables météo ajoutées : `TMAX`, `TMIN`, `PRCP`, `SNOW`, `SNWD`

### ✔ Export dans `silver/` :

```
ACCIDENT_2010_2022_cleaned.parquet
NOAA_MASTER_US.parquet
ACCIDENT_WEATHER_YEARLY.parquet
```

---

# 🟡 **6. Feature Engineering (Gold Layer)**

Création de variables explicatives essentielles :

### 🌙 **Conditions de luminosité**

* LIGHT_COND (day/night)

### 🛣️ **Type de route**

* ROUTE_TYPE (urban, rural, interstate…)

### 🚗 **Type de véhicule**

* VEHICLE_TYPE (car, SUV, truck, motorcycle…)

### 💥 **Type de collision**

* COLLISION_TYPE (frontale, latérale, piéton, etc.)

### 🗺️ **Zone**

* AREA_TYPE (urban / rural)

### 👉 Ajout de la variable cible :

* **severity** (3 niveaux)

Le dataset final :

```
GOLD_FEATURES.parquet
```

Puis création d’un dataset **optimisé Power BI** :

```
GOLD_FEATURES_LIGHT.csv
ou
GOLD_FEATURES_LIGHT.parquet
```

---

# 📊 **7. Insights & Dashboard Power BI**

Le dashboard est organisé en **4 pages professionnelles**.

---

## 🟦 **PAGE 1 — Overview (KPIs et Vision Globale)**

* Total accidents
* Total accidents mortels
* Severity distribution
* Évolution annuelle
---

## 🟩 **PAGE 2 — Analyses temporelles**

* Accidents fatales par heure de la journée
* Day/Night distribution
* Accidents par mois

---

## 🟧 **PAGE 3 — Facteurs structurels (Insights clés)**

* Sévérité par :

  * Type de route
  * Type de véhicule
  * Type de collision
  * Total accidents par type de route

→ **Les vrais facteurs explicatifs de la sévérité**

---

## 🟨 **PAGE 4 — Facteurs météo**

* % accidents sous pluie/neige
* Fatality rate vs météo
* Graphique de ruban TMAX/TMIN vs Severity
* PRCP vs Severity

➡️ Insight majeur :
**La météo n’explique presque pas la gravité.**
La gravité dépend surtout des facteurs structurels.

---

# 🧠 **8. Résultats & Conclusions**

Les analyses montrent que :

### ❌ La météo a un impact très faible

* <0,05% d’accidents sous pluie/neige
* Effet quasi nul sur la gravité

### ✔ Les facteurs *réellement* explicatifs :

1. **Type de route** (rural & highway = plus mortels)
2. **Type de véhicule** (motos ≫ mortalité)
3. **Type de collision** (frontales mortelles)
4. **Heure nocturne** (gravité plus élevée la nuit)
5. **Zone rurale** (accès aux secours plus lent)

Ces conclusions sont cohérentes avec la littérature scientifique FARS.

---

# 🧩 **Technologies utilisées**

* **Python** (Pandas, PyArrow)
* **Power BI Desktop**
* **Data Lake local (filesystem)**
* **NOAA + FARS** datasets
* **Haversine distance** pour matching spatio-temporel
* **Parquet** (optimisé pour le stockage)
* **CSV** (optimisé pour Power BI)

---

# 📎 **Structure du repository GitHub**

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

# 🏁 **Conclusion**

Ce projet met en œuvre une architecture Data Lake complète, un pipeline ETL robuste et une analyse approfondie révélant les facteurs clés influençant la gravité des accidents aux États-Unis.
Le dashboard Power BI permet une exploration interactive et fournit des insights prêts pour un usage décisionnel ou opérationnel.
