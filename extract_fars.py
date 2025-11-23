import zipfile
from pathlib import Path

raw_dir = Path("data/raw")
extract_dir = Path("data/extracted")

# Création du dossier d'extraction s'il n'existe pas
extract_dir.mkdir(parents=True, exist_ok=True)

# Parcourir tous les fichiers ZIP dans data/raw
zip_files = list(raw_dir.glob("*.zip"))

print(f"Fichiers ZIP trouvés : {len(zip_files)}")

for zip_path in zip_files:
    year = zip_path.stem.replace("FARS", "").replace("NationalCSV", "")
    target_folder = extract_dir / year
    target_folder.mkdir(exist_ok=True)
    
    print(f"Décompression de {zip_path.name} dans {target_folder} ...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_folder)

print("Décompression terminée !")
