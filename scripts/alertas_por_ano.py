import pandas as pd

# Carregar o CSV
df = pd.read_csv("M1/Trabalhos UC/raw bases/Alertas_agrupados_por_cidade_e_ano.csvM1/Trabalhos UC/raw bases/Alertas_agrupados_por_cidade_e_ano.csv")

# Remover a coluna 'city'
df = df.drop(columns=["city"])

# Agrupar por ano, somando registros e área
df_grouped = df.groupby("year", as_index=False).sum()

# Salvar em novo CSV (opcional)
df_grouped.to_csv("alertas_por_ano.csv", index=False)

print(df_grouped)
