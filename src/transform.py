# src/transform.py (versão simples, sem dicionário externo)
import pathlib, pandas as pd, re
import pickle

PAIS_MAP = pickle.load(open("data/meta/pais_cod2nome.pkl", "rb"))

NCM = 85371030
frames = []

for csv in pathlib.Path("data/raw").glob("IMP_*.csv"):
    print("‣", csv.name)
    df = pd.read_csv(csv, sep=";", encoding="latin1", low_memory=False)

    val_col  = next(c for c in df.columns if re.match(r"VL_FOB", c))
    pais_num = next(c for c in df.columns if re.match(r"CO_PAIS", c))
    txt_cols = [c for c in df.columns if "PAIS" in c and not c.startswith("CO_")]
    pais_txt = txt_cols[0] if txt_cols else None

    cols = ["CO_ANO", "CO_MES", pais_num, val_col]
    if pais_txt:
        cols.append(pais_txt)

    rec = (
        df[df["CO_NCM"] == NCM][cols]
          .rename(columns={
              "CO_ANO": "ano",
              "CO_MES": "mes",
              pais_num: "pais",
              val_col:  "valor_usd",
              pais_txt or "": "pais_nome"
          })
    )

    # usa o dicionário; se faltar, mantém o código numérico
    if "pais_nome" not in rec:
        rec["pais_nome"] = rec["pais"].map(PAIS_MAP).fillna(rec["pais"].astype(str))

    frames.append(rec)

base = pd.concat(frames, ignore_index=True)
base.to_parquet("data/processed/import_85371030.parquet", index=False)
print("✅ parquet consolidado:", base["ano"].min(), "–", base["ano"].max(),
      "|", len(base), "linhas")
