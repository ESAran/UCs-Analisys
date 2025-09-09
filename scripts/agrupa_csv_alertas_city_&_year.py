import pandas as pd

def processar_dados_csv_ano_cidade(arquivo_entrada, arquivo_saida):
    """
    Processa arquivo CSV agrupando por year e city,
    contando registros e somando area_ha (com correção de formato)
    
    Args:
        arquivo_entrada (str): Caminho do arquivo CSV de entrada
        arquivo_saida (str): Caminho do arquivo CSV de saída
    """
    try:
        # Ler o arquivo CSV
        df = pd.read_csv(arquivo_entrada)
        
        # Limpar e converter a coluna area_ha para numérico
        if 'area_ha' in df.columns:
            def processar_area_ha(valor):
                """Processa valores de area_ha - versão simplificada"""
                if pd.isna(valor):
                    return 0.0
                    
                # Converter para string
                texto = str(valor).strip()
                if not texto or texto == 'nan':
                    return 0.0
                
                # Dividir por espaços para pegar múltiplos valores
                numeros = texto.split()
                soma_total = 0.0
                
                for num_str in numeros:
                    if num_str.strip():
                        try:
                            # Trocar vírgula por ponto
                            num_limpo = num_str.strip().replace(',', '.')
                            numero = float(num_limpo)
                            soma_total += numero
                        except:
                            print(f"Erro convertendo: '{num_str}' -> ignorando")
                            continue
                
                return soma_total
            
            # Debug: mostrar alguns valores antes
            print("=== VALORES ORIGINAIS (primeiros 5) ===")
            for i in range(min(5, len(df))):
                print(f"Linha {i}: '{df.iloc[i]['area_ha']}'")
            
            # Processar
            df['area_ha'] = df['area_ha'].apply(processar_area_ha)
            
            # Debug: mostrar valores após processamento
            print("\n=== VALORES APÓS PROCESSAMENTO (primeiros 5) ===")
            for i in range(min(5, len(df))):
                print(f"Linha {i}: {df.iloc[i]['area_ha']} (tipo: {type(df.iloc[i]['area_ha'])})")
            
            # Filtrar valores válidos
            df = df[df['area_ha'] > 0]
            print(f"\nLinhas válidas após filtro: {len(df)}")
        
        # Verificar se as colunas necessárias existem
        colunas_necessarias = ['year', 'city', 'area_ha']
        for col in colunas_necessarias:
            if col not in df.columns:
                print(f"Erro: Coluna '{col}' não encontrada no arquivo")
                print(f"Colunas disponíveis: {list(df.columns)}")
                return
        
        # Agrupar por year e city
        resultado = df.groupby(['year', 'city']).agg(
            quantidade_registros=('city', 'count'),  # Contagem de registros
            area_ha_total=('area_ha', 'sum')         # Soma da área
        ).reset_index()
        
        # Ordenar por ano e cidade
        resultado = resultado.sort_values(['year', 'city'])
        
        # FORÇAR formato decimal correto antes de salvar
        resultado['area_ha_total'] = resultado['area_ha_total'].round(4)
        resultado['area_ha_total'] = resultado['area_ha_total'].astype(str)
        
        # Salvar o resultado com configurações específicas
        resultado.to_csv(arquivo_saida, index=False, decimal='.', sep=',', float_format='%.4f')
        
        print(f"Processamento concluído!")
        print(f"Arquivo de saída salvo em: {arquivo_saida}")
        print(f"Total de grupos ano/cidade processados: {len(resultado)}")
        print("\nPrimeiras linhas do resultado:")
        print(resultado.head())
        
        # Estatísticas resumidas
        print(f"\nEstatísticas:")
        print(f"Anos processados: {resultado['year'].min()} a {resultado['year'].max()}")
        print(f"Total de cidades únicas: {resultado['city'].nunique()}")
        print(f"Área total processada: {pd.to_numeric(resultado['area_ha_total']).sum():.2f} ha")
        print(f"Média de registros por grupo: {resultado['quantidade_registros'].mean():.1f}")
        
        # Mostrar grupo com mais registros
        idx_max_registros = resultado['quantidade_registros'].idxmax()
        print(f"Grupo com mais registros: {resultado.loc[idx_max_registros, 'year']} - {resultado.loc[idx_max_registros, 'city']} ({resultado.loc[idx_max_registros, 'quantidade_registros']} registros)")
        
        # Mostrar grupo com maior área
        resultado_num = resultado.copy()
        resultado_num['area_ha_total'] = pd.to_numeric(resultado_num['area_ha_total'])
        idx_max_area = resultado_num['area_ha_total'].idxmax()
        print(f"Grupo com maior área: {resultado.loc[idx_max_area, 'year']} - {resultado.loc[idx_max_area, 'city']} ({resultado_num.loc[idx_max_area, 'area_ha_total']:.2f} ha)")
        
    except FileNotFoundError:
        print(f"Erro: Arquivo '{arquivo_entrada}' não encontrado")
    except Exception as e:
        print(f"Erro durante o processamento: {e}")

# Exemplo de uso
if __name__ == "__main__":
    # Configurar os caminhos dos arquivos
    arquivo_entrada = "raw bases/database_alertas_sc.csv"  # Substitua pelo seu arquivo
    arquivo_saida = "bases finais/Alertas_agrupados_por_cidade_e_ano.csv"
    
    # Processar os dados
    processar_dados_csv_ano_cidade(arquivo_entrada, arquivo_saida)
    
    # Exemplo alternativo com nomes específicos
    # processar_dados_csv("meus_dados.csv", "resultado_agrupado.csv")