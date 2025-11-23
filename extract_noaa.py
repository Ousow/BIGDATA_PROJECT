import gzip
import shutil
from pathlib import Path

raw_noaa = Path("data/raw/noaa")
extract_noaa = Path("data/extracted/noaa")
extract_noaa.mkdir(parents=True, exist_ok=True)

gz_files = list(raw_noaa.glob("*.gz"))

print(f"Fichiers NOAA trouvés : {len(gz_files)}")

for gz_file in gz_files:
    output_file = extract_noaa / gz_file.stem  # supprime .gz
    print(f"Décompression : {gz_file.name} -> {output_file.name}")

    with gzip.open(gz_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

print("Décompression NOAA terminée !")
