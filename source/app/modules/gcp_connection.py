import logging
from app import secrets_gcp
import pandas as pd
from pandas import DataFrame
from google.cloud import bigquery
from collections import namedtuple
from google.oauth2 import service_account
from app.config.collections_exceptions import collections_exceptions
from datetime import datetime
from typing import Union

class FunctionException(Exception):
    pass

class GcpConnection:
    def __init__(self, path_file_credentials=None) -> None:
        self.path_file_credentials = None if path_file_credentials == None or path_file_credentials == "" else path_file_credentials
        self.eng = self.engine_cloud()
    
    def engine_cloud(self) -> namedtuple:
        """
        ### Função que cria motores `engine` de conexão com o `BigQuery`.

        Variáves:\n
            \t• None : `None`\n
        Retorno:\n
            \t✔️ Engine : ['bigquery_client', 'credentials'] : `namedtuple`\n
        Overview:\n
            \t» De acordo com `login.json` é criado uma nametuple com ⇒ `client` e `credentials` que podem ser utilizados para estabelecer conexão com o Big Query;\n
        """
        try:
            engine_gcp = namedtuple('Engine', ['bigquery_client', 'credentials'])
            
            file_credentials = self.path_file_credentials if self.path_file_credentials != None else secrets_gcp

            credentials = service_account.Credentials.from_service_account_info(file_credentials)
            
            bigquery_client = bigquery.Client(credentials=credentials)
            
            return engine_gcp(bigquery_client, credentials)
        except Exception as e:
            raise collections_exceptions(e)
        
        
    def update_big_query_with_dataframe(self, data : DataFrame, destination_table : str, project_id : str, if_exists : str , str_type: bool = True) -> None:
        """
        ### Função que envia um DataFrame para um BigQuery.
        
        Variáves:\n
            \t• data : `DataFrame` ⇒ Tabela que será enviada do Big Query\n
            \t• destination_table : `str` ⇒ Tabela que será atualizada e/ou criada no Big Query : `claro_gcpbq_bflow_db.ALERTA_RETORNO`\n
            \t• project_id : `str` ⇒ Nome do projeto onde existe a tabela a ser atualizada e/ou criada no Big Query : `acn-comgas`\n
            \t• if_exists : `str` ⇒ Modo de atualização : `append` or `replace`\n
            \t• str_type : `bool` ⇒ `True` tranforma o DataFrame em string e `False` mantém esto original\n
            
        Retorno:\n
            \t✔️ None : `None`\n
            
        Overview:\n
            \t» Usa o `engine_cloud` para gerar as`credentials` que serão utilizadas para acessar o BigQuery e atualizar e/ou criar uma tabela.;\n
        """
        try:
            if str_type:
                data = data.astype(str).copy()
                
            data.replace(to_replace="None", value="", inplace=True)
            data.to_gbq(destination_table=destination_table, project_id=project_id, if_exists=if_exists, credentials=self.eng.credentials)
            
        except Exception as e:
            raise collections_exceptions(e)


    def run_query_sql_big_query(self, file_path_folder : str, file_name:str, name_params:str = None, value_params:str = None) -> dict:
        """
        ### Função que executa uma query no Big Query.
        `DEFAULT ⇒ Raiz do diretorio a ser verificado(bflow_etl/sql/config/select/..)`\n
        Variáves:\n
            \t• name : `str` ⇒ Nome do Arquivo SQL a ser executado.\n
            \t• file_path_folder : `str` ⇒ Novo caminho para obtenção do Arquivo SQL a ser executado ⇒ (bflow_etl/`new_folder`/`n_folder`/..).\n
        Retorno:\n
            \t df : `DataFrame`\n
        Overview:\n
            \t» Pegamos o arquivo de acordo com o nome informado;\n
            \t» Acessamos a query do arquivo;\n
            \t» Executamos a query no Big Query e retornamos um DataFrame;\n
        """
        try:
            file_path_sql = f"{file_path_folder}/{file_name}.sql"
                
            query_sql_file_path_config = lambda name: open(file_path_sql, encoding='utf-8').read()
            
            if name_params and value_params:
                query = str(query_sql_file_path_config(f"{file_name}")).replace(name_params, value_params)
            
            df = pd.read_gbq(query=query, project_id="acn-comgas",credentials=self.eng.credentials)
            if df.empty:
                return {"name":file_name, "data":pd.DataFrame()}
            else:
                return {"name":file_name, "data":df}
        except Exception as e: 
            raise collections_exceptions(e)


    def _regex_nome_colunas(self, colunas:list) -> list:
        import re
        data = colunas
        
        for i in range(0, len(data)):
            data[i] = re.sub(r"[\ ]","_", data[i])
            data[i] = re.sub(r"[À-ü\Wº]","", data[i])
        return data


def __get_datetime(format: str = "%Y-%m-%d %H:%M") -> str:
    """Get a datetime using a specific format

    Args:
        format (str, optional): Date and time format. Defaults to "%Y-%m-%d %H:%M".

    Returns:
        str: Date and time using the format specified
    """
    dt_now = datetime.now()
    dt_now_str = dt_now.strftime(format)

    return dt_now_str


def __run_gcp_query(query: str, credentials_path: str, disable_log: Union[bool, str] = True) -> None:
    """Execute a GCP BigQuery query

    Args:
        query (str): A valid query
        credentials_path (str): Path to where the credential file is located
        disable_log (bool or str, optional): If true the query will be only displayed, if false it's executed. Defaults to True.
    """
    if str(disable_log).lower() == "true":
        print(query)
        return
    eng_gcp = GcpConnection(
        path_file_credentials=credentials_path).eng.bigquery_client
    try:
        job = eng_gcp.query(query)
        print(job.result())
    except Exception as e:
        raise collections_exceptions(e)
    finally:
        eng_gcp.close()


# region queries
__query_inicio = """
INSERT INTO {db_destination} (
  id,
  current_status,
  automation_name,
  process_name,
  execution_start,
  created_at
) VALUES (
  "{uuid}",
  "Running",
  "{automation_name}",
  "{process_name}",
  "{execution_start}",
  "{created_at}"
)
"""

__query_fim = """
UPDATE {db_destination} 
SET
  current_status = "Done",
  execution_end  = "{execution_end}"
WHERE 
  id = "{uuid}" AND
  automation_name = "{automation_name}" AND
  process_name = "{process_name}" AND
  created_at = "{created_at}"
"""

__query_erro = """
UPDATE {db_destination} 
SET 
  current_status = "Error", 
  execution_end = "{execution_end}",
  execution_error = "{execution_error}"
WHERE
  id = "{uuid}" AND
  automation_name = "{automation_name}" AND
  process_name = "{process_name}" AND
  created_at = "{created_at}"
"""


# endregion queries

def monitoramento(automation_name: str, process_name: str, table: str, dataset: str,
                  gcp_project: str, credentials_path: str, uuid: str,
                  disable_log: Union[bool, str] = True,
                  date_time_format: str = "%Y-%m-%d %H:%M:%S"):
    """Decorator to monitor a function execution

    Args:
        automation_name (str): Automation name
        process_name (str): Process/function name
        table (str): Table where the execution log is inserted
        dataset (str): Dataset where the table is located
        gcp_project (str): Project where the dataset is located
        credentials_path (str): Path to where the credential file is located
        uuid (str, optional): UUID related to the process. Defaults to None.
        disable_log (bool or str, optional): If true the query will be only displayed, if false it's executed. Defaults to True.
        date_time_format (str, optional): Date and time format. Defaults to "%Y-%m-%d %H:%M".

    Raises:
        Exception: Any exception is logged and raised
    """
    db_destination = f"{gcp_project}.{dataset}.{table}"
    created_at = __get_datetime(format=date_time_format)
    execution_start = created_at

    def decorador_function(func):
        def wrapper(*args, **kwargs):
            try:
                __run_gcp_query(query=__query_inicio.format(
                    uuid=uuid,
                    db_destination=db_destination,
                    automation_name=automation_name,
                    process_name=process_name,
                    execution_start=execution_start,
                    created_at=created_at
                ),
                    credentials_path=credentials_path,
                    disable_log=disable_log)
                print(
                    f"INICIO - Automação: {automation_name} - Processo: {process_name} - Function Name: {func.__name__}")

                try:
                    func_return = func(*args, **kwargs)
                except Exception as e:
                    raise collections_exceptions(e)

                execution_end = __get_datetime(format=date_time_format)
                __run_gcp_query(query=__query_fim.format(
                    uuid=uuid,
                    db_destination=db_destination,
                    execution_end=execution_end,
                    created_at=created_at,
                    automation_name=automation_name,
                    process_name=process_name,
                ),
                    credentials_path=credentials_path,
                    disable_log=disable_log)
                print(f"FIM - Automação: {automation_name} - Processo: {process_name} - Function Name: {func.__name__}")
                return func_return
            
            except FunctionException as err:
                execution_end = __get_datetime(format=date_time_format)

                error = str(err)

                __run_gcp_query(query=__query_erro.format(
                    uuid=uuid,
                    db_destination=db_destination,
                    execution_end=execution_end,
                    execution_error=error,
                    created_at=created_at,
                    automation_name=automation_name,
                    process_name=process_name,
                ),
                    credentials_path=credentials_path,
                    disable_log=disable_log)
                print(f"FIM ERRO - Automação: {automation_name} - Processo: {process_name} - Function: {func.__name__}")
                raise collections_exceptions(err)
            
            except Exception as e:
                error = str(e)
                logging.error(f'Erro na execução do decorador {error}')

        return wrapper

    return decorador_function