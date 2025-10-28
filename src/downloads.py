import pandas as pd
import requests
from io import StringIO
import warnings
import os

# --- Configurações ---
url = 'https://app.anm.gov.br/DadosAbertos/AMB/Producao_Bruta.csv'
NOME_ARQUIVO_LOCAL = 'Producao_Bruta_Local.csv' # Nome do arquivo que será SALVO
SEPARADOR = ';' # Separador de colunas (provável para dados da ANM)
CODIFICACAO = 'latin1' # Codificação de caracteres (provável para dados da ANM)
caminho_completo = r"C:\Users\03892152284\Documents\Python\ANM\Producao_Bruta.csv"

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

try:
    print(f"1. Tentando baixar o arquivo de: {url}")
    # Faz a requisição, desativando a verificação SSL (verify=False)
    response = requests.get(url, verify=False)
    response.raise_for_status() 

    # Salva o arquivo no caminho completo
    with open(caminho_completo, 'wb') as f:
        f.write(response.content)
   
except requests.exceptions.RequestException as e:
    print(f"\nErro na requisição HTTP/SSL. Não foi possível baixar o arquivo. Erro: {e}")
except Exception as e:
    print(f"\nOcorreu um erro ao processar ou salvar o arquivo: {e}")