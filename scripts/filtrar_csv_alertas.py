import pandas as pd
import re
import unicodedata

def remove_accents(text):
    """
    Remove acentos e caracteres especiais de uma string
    """
    if not isinstance(text, str):
        return text
   
    # Normaliza o texto para decompor caracteres acentuados
    nfd = unicodedata.normalize('NFD', text)
    # Remove os caracteres de combinação (acentos)
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
   
    return without_accents

def clean_special_characters(text):
    """
    Remove caracteres especiais como [], {}, vírgulas
    """
    if not isinstance(text, str):
        return text
   
    # Remove caracteres especiais: [ ] { } ,
    cleaned = re.sub(r'[\[\]{}]', '', text)
    # Remove vírgulas (opcional - descomente se necessário)
    # cleaned = re.sub(r',', '', cleaned)
   
    return cleaned

def process_dataframe(df):
    """
    Processa o DataFrame aplicando todas as transformações
    """
    # Criar uma cópia do DataFrame para não modificar o original
    df_processed = df.copy()
   
    # Processar cada coluna
    for column in df_processed.columns:
        # Verificar se a coluna contém dados textuais (string/object)
        if df_processed[column].dtype == 'object':
            # Aplicar transformações em ordem:
            # 1. Remover caracteres especiais
            df_processed[column] = df_processed[column].apply(clean_special_characters)
           
            # 2. Remover acentos
            df_processed[column] = df_processed[column].apply(remove_accents)
           
            # 3. Converter para maiúsculas
            df_processed[column] = df_processed[column].astype(str).str.upper()
   
    return df_processed

def main():
    # Caminho do arquivo
    file_path = "raw bases/rad24_situacao_final.csv"
   
    try:
        # Ler o arquivo CSV
        print("Lendo o arquivo CSV...")
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"Arquivo carregado com sucesso! Dimensões: {df.shape}")
       
        # Exibir informações básicas do dataset original
        print("\n=== INFORMAÇÕES DO DATASET ORIGINAL ===")
        print(f"Número de linhas: {len(df)}")
        print(f"Número de colunas: {len(df.columns)}")
        print(f"Colunas: {list(df.columns)}")
       
        # Verificar a coluna state (índice 5)
        state_column = df.columns[5] if len(df.columns) > 5 else None
        if state_column:
            print(f"Coluna de estado (índice 5): '{state_column}'")
            print(f"Valores únicos na coluna de estado ANTES do processamento:")
            print(df[state_column].value_counts())
        else:
            print("ATENÇÃO: Não foi possível encontrar a coluna de estado no índice 5")
       
        # Mostrar uma amostra dos dados originais
        print("\n=== AMOSTRA DOS DADOS ORIGINAIS ===")
        print(df.head(3))
       
        # Processar o DataFrame
        print("\nProcessando os dados...")
        df_cleaned = process_dataframe(df)
       
        # Verificar os valores únicos APÓS o processamento
        if state_column:
            print(f"\nValores únicos na coluna de estado APÓS o processamento:")
            print(df_cleaned[state_column].value_counts())
       
        # Filtrar apenas registros de Santa Catarina
        print("\nFiltrando dados de Santa Catarina...")
        if state_column:
            # Filtrar por "SANTA CATARINA" (após o processamento)
            df_sc = df_cleaned[df_cleaned[state_column] == 'SANTA CATARINA'].copy()
            
            print(f"Registros encontrados para Santa Catarina: {len(df_sc)}")
            
            if len(df_sc) == 0:
                print("\nATENÇÃO: Nenhum registro encontrado para 'SANTA CATARINA'")
                print("Verificando se existem variações do nome...")
                # Buscar por variações que contenham "SANTA" e "CATARINA"
                mask = df_cleaned[state_column].str.contains('SANTA.*CATARINA', case=False, na=False)
                df_sc = df_cleaned[mask].copy()
                print(f"Registros encontrados com variações: {len(df_sc)}")
                
                if len(df_sc) > 0:
                    print("Valores encontrados:")
                    print(df_sc[state_column].value_counts())
        else:
            df_sc = df_cleaned.copy()
            print("Não foi possível filtrar - usando todos os dados processados")
       
        # Mostrar uma amostra dos dados processados e filtrados
        print("\n=== AMOSTRA DOS DADOS PROCESSADOS E FILTRADOS ===")
        print(df_sc.head(3))
       
        # Salvar o arquivo processado
        output_path = "raw bases/database_alertas_sc.csv"
        df_sc.to_csv(output_path, index=False, encoding='utf-8')
        print(f"\nArquivo processado salvo em: {output_path}")
       
        # Estatísticas finais
        #print("\n=== PROCESSAMENTO CONCLUÍDO ===")
        #print(f"✓ Registros originais: {len(df)}")
        #print(f"✓ Registros removidos (valor 135003): {removed_records}")
        #print(f"✓ Registros após remoção: {records_after_removal}")
        #print(f"✓ Registros finais de Santa Catarina: {len(df_sc)}")
        ##\print(f"✓ Caracteres especiais [ ] {{ }} removidos")
        #print(f"✓ Acentos removidos (ç→c, í→i, etc.)")
        #print(f"✓ Texto convertido para maiúsculas")
        #print(f"✓ Removido registros com valor 135003 na coluna 0")
        #print(f"✓ Filtrado apenas Santa Catarina")
        #print(f"✓ Arquivo salvo como: {output_path}")
       
    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado no caminho: {file_path}")
        print("Verifique se o caminho está correto e se o arquivo existe.")
   
    except pd.errors.EmptyDataError:
        print("Erro: O arquivo CSV está vazio.")
   
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")

#LINHA COM COLUNA 0 = 150003 EXCLUÍDA
#LINHA COM COLUNA 0 = 552950 EXCLUÍDA
#LINHA COM COLUNA 0 = 1216932 EXCLUÍDA 
#LINHA COM COLUNA 0 = 1354922 EXCLUÍDA

if __name__ == "__main__":
    main()