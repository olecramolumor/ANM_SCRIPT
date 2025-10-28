import pandas as pd
import requests
from io import StringIO
import warnings
import os
from pathlib import Path
import logging
import datetime

def main():

    #criando logs do sistema

    logger = logging.getLogger(__name__)

    # --- Configurações ---
    url = 'https://app.anm.gov.br/DadosAbertos/AMB/Producao_Bruta.csv'
    nome_arquivo= 'Producao_Bruta.csv' # Nome do arquivo que será SALVO
    #SEPARADOR = ';' # Separador de colunas (provável para dados da ANM)
    #CODIFICACAO = 'latin1' # Codificação de caracteres (provável para dados da ANM)
    #caminho_completo = r"C:\Users\02233260201\Documents\anm\files\Producao_Bruta.csv"

    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    #caminho do projeto ||  encontra a pasta base do projeto

    project_dir = Path(__file__).resolve().parent.parent 
    print(project_dir) # exibe a pasta do pojeto

    #criando (se possivel) e acessando a pasta files
    data_dir = project_dir / "files" 
    data_dir.mkdir(parents=True, exist_ok=True) 

    caminho_completo = data_dir / nome_arquivo

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

if __name__ == '__main__':
    main()