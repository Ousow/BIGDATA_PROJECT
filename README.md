# BIGDATA_PROJECT
# Analyse multidimensionnelle des facteurs influençant la gravité des accidents routiers aux États-Unis

## 🎯 Contexte du projet
Les accidents routiers représentent une cause majeure de mortalité et de blessures aux États-Unis.  
Bien que les conditions climatiques influencent la survenue des accidents, elles ne suffisent pas à expliquer la gravité.  
Cette gravité résulte d’une combinaison complexe de facteurs : humains, routiers, environnementaux, temporels et contextuels.  

Ce projet vise à construire une **architecture Big Data (DataLake)** capable de traiter des millions d’enregistrements provenant de sources hétérogènes pour analyser ces facteurs conjointement.

---

## 🧩 Problématique
**Problème central :**  
Quels sont les facteurs individuels et combinés qui influencent le plus la gravité d’un accident routier aux États-Unis, et comment peut-on les analyser efficacement à l’aide d’une architecture Big Data ?

**Sous-problématiques :**
1. **Facteurs humains :** Impact de l’alcool, de la fatigue ou de l’âge du conducteur.
2. **Facteurs routiers :** Types de routes présentant les risques les plus élevés.
3. **Facteurs climatiques :** Influence de la pluie, neige, brouillard, température, visibilité.
4. **Facteurs temporels :** Périodes (jour/nuit, week-end, saisons) les plus critiques.
5. **Facteurs géographiques et contextuels :** Identification des zones à forte gravité (hotspots).
6. **Interactions multidimensionnelles :** Combinations critiques (ex : nuit + pluie + route rurale).
7. **Prédiction :** Possibilité de prédire la gravité d’un accident.

**Objectifs :**
- Construire un DataLake complet pour stocker et traiter des données massives hétérogènes.
- Identifier les facteurs les plus déterminants.
- Visualiser les accidents et leurs causes via un dashboard interactif.
- Fournir des insights exploitables pour réduire la gravité des accidents.

---

## 🧱 Architecture Big Data

### 1️⃣ Ingestion Layer
**Sources hétérogènes :**
- FARS / Traffic Accident Data (CSV, API)  
- NOAA Weather Data (API)  
- U.S. Road Information (DOT, CSV/API)  
- Population / Densité (US Census, CSV/API)  

**Méthodes :**
- Batch ingestion via Airflow / scripts Python
- Stockage raw dans le **DataLake - Bronze Zone**  

---

### 2️⃣ Storage & Processing Layer
**DataLake :**  
- **Bronze Zone (Raw)** : données brutes, archivage complet  
- **Silver Zone (Cleaned/Curated)** : données nettoyées, jointures multi-sources, formats optimisés (Parquet)  
- **Gold Zone (Analytics / ML)** : tables finales pour dashboard et modèles ML, tables agrégées  

**Traitement :**  
- Spark / Databricks / PySpark  
- ETL, nettoyage, enrichissement, jointures  

---

### 3️⃣ Insight Layer
**Dashboard & Analyses :**  
- Power BI / Tableau / Grafana  
- Heatmaps géolocalisées, séries temporelles, analyse par facteurs humains, climatiques et routiers  

**Machine Learning (optionnel) :**  
- Random Forest / XGBoost pour prédiction de la gravité  
- SHAP values pour expliquer les facteurs influents  

---

### 4️⃣ Architecture technique (diagramme simplifié)
