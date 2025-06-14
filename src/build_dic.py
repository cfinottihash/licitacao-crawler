# src/build_dic.py  – roda uma única vez
import pycountry, pickle, pandas as pd, pathlib

df = pd.read_parquet("data/processed/import_85371030.parquet")
codigos = df["pais"].unique()

mapping = {}
for num in codigos:
    try:
        name = pycountry.countries.get(numeric=f"{int(num):03d}").name
        mapping[int(num)] = name.upper()
    except AttributeError:
        pass  # fica fora, preencheremos à mão

# preencha manualmente casos que faltarem (ex. territórios não ISO)
mapping.update({
    7441: "COREIA DO SUL",
    6185: "ESTADOS UNIDOS",
})

meta = pathlib.Path("data/meta"); meta.mkdir(parents=True, exist_ok=True)
pickle.dump(mapping, open(meta / "pais_cod2nome.pkl", "wb"))
print("✅ dicionário salvo –", len(mapping), "entradas")