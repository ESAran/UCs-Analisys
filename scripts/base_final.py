import pandas as pd

# --- Carregar CSVs ---
df_ucs = pd.read_csv("bases finais/ucs_unificado.csv", decimal=",")  # ÁREA (ha),ANO DE CRIAÇÃO,MUNICÍPIO
df_desmat = pd.read_csv("bases finais/Alertas_agrupados_por_cidade_e_ano.csv")          # year,city,quantidade_registros,area_ha_total

# --- Limpeza básica ---
df_ucs["ANO DE CRIAÇÃO"] = pd.to_numeric(df_ucs["ANO DE CRIAÇÃO"], errors="coerce")
df_ucs = df_ucs.dropna(subset=["ANO DE CRIAÇÃO"])
df_ucs["ANO DE CRIAÇÃO"] = df_ucs["ANO DE CRIAÇÃO"].astype(int)

# Padronizar nomes das cidades: maiúsculas e sem acento
import unicodedata

def normalize_city(name):
    return unicodedata.normalize('NFKD', str(name)).encode('ASCII','ignore').decode('ASCII').upper()

df_ucs["MUNICÍPIO"] = df_ucs["MUNICÍPIO"].apply(normalize_city)
df_desmat["city"] = df_desmat["city"].apply(normalize_city)

# --- Calcular quantidade total de UCS até o ano ---
# Primeiro, renomear colunas para unir facilmente
df_ucs = df_ucs.rename(columns={"ANO DE CRIAÇÃO":"ANO", "MUNICÍPIO":"city"})

# Criar função para contar UCS acumuladas até cada ano
def count_ucs_until(row, df_uc):
    # contar quantas UCS da cidade foram criadas até o ano
    return df_uc[(df_uc["city"]==row["city"]) & (df_uc["ANO"] <= row["year"])].shape[0]

df_desmat["quantidade_total_uc"] = df_desmat.apply(lambda row: count_ucs_until(row, df_ucs), axis=1)

# --- Área total de desmatamento já está na tabela desmat --- 
# Podemos renomear para ficar consistente
df_desmat = df_desmat.rename(columns={"area_ha_total":"area_total_desmat"})

df_desmat['city'] = df_desmat['city'].str.strip().replace("'", "")

# --- Selecionar colunas finais ---
df_final = df_desmat[["year", "city", "quantidade_total_uc", "area_total_desmat"]]

# Salvar resultado
df_final.to_csv("bases finais/base_final.csv", index=False)

"""
--- Adicionar mesorregião ---
"""
# Carregar cidades_mesoregiao.csv
df_meso = pd.read_csv("raw bases/cidades_mesoregiao.csv", sep=";")

# Normalizar nomes das cidades para garantir correspondência
df_final["city"] = df_final["city"].apply(normalize_city)
df_final["city"] = df_final["city"].str.replace("-", " ").str.replace("'", " ")
df_final["city"] = df_final["city"].str.strip()

df_meso["CIDADE"] = df_meso["CIDADE"].apply(normalize_city)
df_meso["CIDADE"] = df_meso["CIDADE"].str.replace("-", " ").str.replace("'", " ")
df_meso["CIDADE"] = df_meso["CIDADE"].str.strip()

# Fazer merge para adicionar a mesorregião
df_final = df_final.merge(df_meso, left_on="city", right_on="CIDADE", how="left")

# Selecionar colunas finais (remover coluna CIDADE duplicada)
df_final = df_final[["year", "city", "MESOREGIAO", "quantidade_total_uc", "area_total_desmat"]]

# Salvar resultado
df_final.to_csv("bases finais/base_final.csv", index=False)

print(df_final.head())
