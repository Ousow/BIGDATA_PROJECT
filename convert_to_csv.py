import pyarrow.parquet as pq
import pandas as pd

INPUT = "data/gold/GOLD_FEATURES_COMPLEMENT.parquet"
OUTPUT = "data/gold/GOLD_FEATURES_COMPLEMENT.csv"

print("Lecture du Parquet en morceaux...")

parquet = pq.ParquetFile(INPUT)

chunks = []

for i in range(parquet.num_row_groups):
    print(f"Row group {i+1}/{parquet.num_row_groups}")
    table = parquet.read_row_group(i)
    df = table.to_pandas()
    chunks.append(df)

print("Fusion des morceaux...")
full_df = pd.concat(chunks, ignore_index=True)

print("Export CSV...")
full_df.to_csv(OUTPUT, index=False)

print("\n✔ CSV prêt :", OUTPUT)
