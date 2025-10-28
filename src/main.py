from pathlib import Path
import logging
import downloads

def setup_master_logging(log_file='processo_completo.log'):
        
    #criando e setando a pasta de logs do script
    project_dir = Path(__file__).resolve().parent.parent
    log_dir = project_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / log_file 

    # formato do log
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig (

        level = logging.INFO,
        forma = log_format,
        handlers= [
            logging.FileHandler(log_path, mode = 'a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def main(): 
    #
    setup_master_logging()
    logger = logging.getLogger(__name__)

    logger.info("Iniciando o Processo de ETL Completo!")

    logger.info("--- Etapa 1: Download dos arquivos ---")
    downloads.main()
