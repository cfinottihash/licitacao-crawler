import datetime, pathlib, requests

ANO = datetime.date.today().year
URL = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{ANO}.csv"
RAW = pathlib.Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)
DEST = RAW / f"IMP_{ANO}.csv"

def run():
    print("↓", URL)
    r = requests.get(URL, stream=True, timeout=120)
    r.raise_for_status()
    with open(DEST, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    print("📥  CSV salvo em", DEST)

if __name__ == "__main__":
    run()
