import datetime, pathlib, requests

RAW = pathlib.Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)

for ano in range(2000, datetime.date.today().year + 1):
    url  = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_{ano}.csv"
    dest = RAW / f"IMP_{ano}.csv"
    if dest.exists():        # evita re-download
        continue
    print("↓", ano)
    r = requests.get(url, stream=True, timeout=120); r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
