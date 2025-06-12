import pathlib, pandas as pd, re

NCM  = 85371030
FILE = sorted(pathlib.Path("data/raw").glob("IMP_*.csv"))[-1]

df = pd.read_csv(FILE, sep=";", encoding="latin1", low_memory=False)

# --- descobrir o nome da coluna de valor FOB ---------------------------------
val_col = next(c for c in df.columns if re.fullmatch(r"VL_FOB.*", c))
pais_col = next(c for c in df.columns if re.fullmatch(r"CO_PAIS.*", c))

base = df[df["CO_NCM"] == NCM][["CO_ANO", "CO_MES", pais_col, val_col]]

agg = (base.groupby(["CO_ANO", "CO_MES"], as_index=False)
             .sum(numeric_only=True)
             .rename(columns={val_col: "valor_usd", pais_col: "pais"}))

dest = pathlib.Path("data/processed/import_85371030.parquet")
dest.parent.mkdir(parents=True, exist_ok=True)
agg.to_parquet(dest, index=False)
print(f"✅ parquet escrito em {dest} — {len(agg)} linhas")
