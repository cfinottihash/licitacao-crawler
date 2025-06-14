import streamlit as st
import pandas as pd
import plotly.express as px

# ── carrega e padroniza colunas ─────────────────────────────────────────────
df = pd.read_parquet("data/processed/import_85371030.parquet")
df = df.rename(columns={
    "CO_ANO": "ano",
    "CO_MES": "mes",
    "VL_FOB_DOLAR": "valor_usd",
    "VL_FOB": "valor_usd",
    "CO_PAIS": "pais",
    "CO_PAIS_ISO": "pais",
})

# ── filtra intervalo de anos ───────────────────────────────────────────────
min_ano, max_ano = int(df["ano"].min()), int(df["ano"].max())
sel_anos = st.slider(
    "Intervalo de anos",
    min_ano, max_ano,
    (min_ano, max_ano)
)
df = df[(df["ano"] >= sel_anos[0]) & (df["ano"] <= sel_anos[1])]

# ── garante coluna de nome de país ─────────────────────────────────────────
if "pais_nome" not in df.columns:
    NOMES = {
        23: "Argentina", 63: "Canadá", 72: "Chile", 87: "Costa Rica",
        105: "Equador", 111: "Espanha", 149: "Hong Kong", 160: "China",
        190: "México", 232: "Países Baixos", 245: "Alemanha", 249: "Estados Unidos",
        271: "Peru", 275: "Polônia", 351: "Suécia", 355: "Colômbia",
        386: "Suíça", 399: "Taiwan", 493: "Vietnã", 607: "Coreia do Sul",
        628: "Tailândia", 741: "Reino Unido", 764: "Índia"
    }
    df["pais_nome"] = df["pais"].map(NOMES).fillna(df["pais"].astype(str))

# ── seletor de países ──────────────────────────────────────────────────────
paises_sel = st.multiselect(
    "Escolha os países:",
    options=df["pais_nome"].unique(),
    default=df["pais_nome"].unique()[:5]
)
filtro = df[df["pais_nome"].isin(paises_sel)]

# ── agrega e plota ────────────────────────────────────────────────────────
agg = (
    filtro
    .groupby(["ano", "pais_nome"], as_index=False)
    .valor_usd.sum()
)
agg["ano"] = agg["ano"].astype(str)  # para eixo categórico

fig = px.line(
    agg,
    x="ano", y="valor_usd",
    color="pais_nome",
    title="Importação de religadores por país (US$ FOB)",
    markers=True,
    category_orders={"ano": sorted(agg["ano"].unique())}
)

# ── gráfico com key único ─────────────────────────────────────────────────
st.plotly_chart(
    fig,
    use_container_width=True,
    key="import_trend_chart"
)

# ── tabela com key ─────────────────────────────────────────────────────────
st.dataframe(filtro, key="import_data_table")
