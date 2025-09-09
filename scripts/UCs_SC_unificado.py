import pandas as pd
from unidecode import unidecode
# Carregar cada aba
df1 = pd.read_excel("raw bases/UCs de Santa Catarina_2024.08.08.xlsx", sheet_name=0)
df2 = pd.read_excel("raw bases/UCs de Santa Catarina_2024.08.08.xlsx", sheet_name=1)
df3 = pd.read_excel("raw bases/UCs de Santa Catarina_2024.08.08.xlsx", sheet_name=2)

# Unir tudo em um dataframe só
df_final = pd.concat([df1, df2, df3], ignore_index=True)

# 1. Substituir "?" por NaN
df_final["ANO DE CRIAÇÃO"] = df_final["ANO DE CRIAÇÃO"].replace("?", pd.NA).replace("n/inf", pd.NA)

# 2. Remover linhas onde ano está vazio/NaN
df_final = df_final.dropna(subset=["ANO DE CRIAÇÃO"])

# Filtrar apenas linhas com ANO DE CRIAÇÃO a até 2019
#df_final = df_final[df_final["ANO DE CRIAÇÃO"] <= 2019]

# 1. Substituir "vazio" por NaN
df_final["ÁREA (ha)"] = df_final["ÁREA (ha)"].replace("n/inf", pd.NA)
df_final = df_final.dropna(subset=["ÁREA (ha)"])

# Colocar MUNICÍPIO em maiúsculo e sem acento
df_final["MUNICÍPIO"] = df_final["MUNICÍPIO"].apply(lambda x: unidecode(str(x)).upper())

# Manter apenas as colunas desejadas
df_final = df_final[["ÁREA (ha)", "ANO DE CRIAÇÃO", "MUNICÍPIO"]]

# Converter coluna "ANO DE CRIAÇÃO" para inteiro antes de salvar
df_final["ANO DE CRIAÇÃO"] = df_final["ANO DE CRIAÇÃO"].astype(int)

# Salvar em novo Excel ou CSV
df_final.to_excel("bases finais/ucs_unificado.xlsx", index=False)
df_final.to_csv("bases finais/ucs_unificado.csv", index=False)


print(df_final.head())
