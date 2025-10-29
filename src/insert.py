import os
import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
from dotenv import load_dotenv 

def main():

    # 2. CARREGA AS VARIÁVEIS DE AMBIENTE DO ARQUIVO .env
    load_dotenv()

    logger = logging.getLogger(__name__)

    # 3. CONFIGURAÇÕES DO BANCO DE DADOS USANDO VARIÁVEIS DO AMBIENTE
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT" , 5432)

    # 4. ACESSANDO DIRETÓRIO DO ARQUIVO BAIXADO

    DIRETORIO_SCRIPT = Path(__file__).parent
    PASTA_ARQUIVOS = DIRETORIO_SCRIPT.parent / "files"

    # 5. TABELA DE DESTINO NO BANCO DE DADOS:
    TABELA_DESTINO = "dados_anm_dados_brutos"

    logger.info("Inciando processo de ETL (Extração, Transformação e Carregar)...")

    # 6. CONFIGURA A CONEXÃO COM O SQL (PostgreSQL)

    try:
        url_object = URL.create(
            "postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )

        engine = create_engine(url_object) 
        logger.info("Conexão com o Banco de Dados estabelecida com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        exit()
    
    # 7. PROCESSA ARQUIVOS CONSOLIDADOS
    arquivo_consolidado = PASTA_ARQUIVOS / "Producao_Bruta.csv"

    if not arquivo_consolidado.exists():
        logger.info(f"Erro: O arquivo '{arquivo_consolidado.name}' não foi encontrado na pasta '{PASTA_ARQUIVOS}'!")
    else:

        try:
            logger.info(f"Processando o arquivo: {arquivo_consolidado.name}.")

    # 8. PROCESSA OS DADOS DO .csv

            df = pd.read_csv(arquivo_consolidado, encoding='latin1', sep=",")
    # 9. RENOMEANDO TODAS AS COLUNAS DO .csv

            df['id']=df.index+1

            df = df.rename(columns = {
                            "Ano base": "ano_base",
                            "UF":"uf",
                            "Classe Substância":"classe_substancia",
                            "Substância Mineral":"substancia_mineral",
                            "Quantidade Produção - Minério ROM (t)":"quant_producao",
                            "Quantidade Contido":"quant_contido",
                            "Unidade de Medida - Contido":"unidade_medida_contido",
                            "Indicação Contido":"indicacao_contido",
                            "Quantidade Venda (t)":"quanti_venda",
                            "Valor Venda (R$)":"valor_venda",
                            "Quantidade Transformação / Consumo / Utilização (t)":"quant_transf_cons_util",
                            "Valor Transformação / Consumo / Utilização nesta mina (R$)":"valor_transf_cons_util_mina",
                            "Quantidade Transferência para Transformação / Utilização / Consumo (t)":"quant_transferencia_transf_cons_util",
                            "Valor Transferência para Transformação / Utilização / Consumo (R$)":"valor_transferencia_transf_cons_util" 
                            })
            
            # outras_colunas = [col for col in df.columns if col != 'id']  ## PECORRE TODAS AS COLUNAS DO DATAFRAME ATÉ ENCONTRAR 'id'

            # nova_ordem = ['id'] + outras_colunas ## TROCA A ORDEM DAS COLUNAS COLOCANDO A COLUNA 'id' NA FRENTE

            # df = df[nova_ordem]  ## CRIA O NOVO DATAFRAME

            print(df.head())
    
    #10 CRIA CONEXÃO COM O BANCO DE DADOS        
            df.to_sql(
                name=TABELA_DESTINO,
                con=engine,
                if_exists='replace',
                index=False
            )

    #11 ALTERANDO 'id' COMO PRIMARY KEY 

            with engine.connect() as conn:
                conn.execute(text(f'ALTER TABLE {TABELA_DESTINO} ADD PRIMARY KEY (id);'))
                conn.commit()

        except Exception as e:
            logger.error(f"Erro ao Processar o arquivo {e}")
    
 


if __name__ == "__main__":
    main()

