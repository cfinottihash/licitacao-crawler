# src/get_paises.py  –  gera dicionário código→nome de país
import pandas as pd, requests, io, pathlib, pickle

def via_balanca():
    url = "http://balanca.economia.gov.br/balanca/bd/comexstat-bd/PAIS.csv"

    try:
        r = requests.get(url, timeout=40, allow_redirects=False)   # NÃO segue redirect
        r.raise_for_status()
        if r.status_code in (301, 302):
            raise RuntimeError("balanca redirecionou para HTTPS")

        csv = r.content.decode("latin1")
        df  = pd.read_csv(io.StringIO(csv), sep=";")[["CO_PAIS", "NO_PAIS"]]
        print("✔ obtido via balanca:", len(df), "países")
        return dict(zip(df.CO_PAIS, df.NO_PAIS.str.title()))
    except Exception as e:
        print("⚠️  balanca falhou:", e)
        return None

def via_ibge():
    url = "https://servicodados.ibge.gov.br/api/v1/paises"
    dados = requests.get(url, timeout=40).json()
    mapa  = {int(p["id"]["M49"]): p["nome"]["abreviado"].title() for p in dados}
    print("✔ obtido via IBGE:", len(mapa), "países")
    return mapa

mapping = via_balanca() or via_ibge()

meta = pathlib.Path("data/meta"); meta.mkdir(parents=True, exist_ok=True)
pickle.dump(mapping, open(meta / "pais_cod2nome.pkl", "wb"))
print("✅ dicionário salvo em data/meta/pais_cod2nome.pkl")
