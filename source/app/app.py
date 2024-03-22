import logging
from app.config.collections_exceptions import collections_exceptions
from app import (secrets_gcp, secrets, UUID)

def run(request, headers, ambiente) -> dict:
    logging.info(f"{UUID} - INICIO REGRA NEGOCIO")
    logging.info(f"{UUID} - headers:{list(headers)}")
    logging.info(f"{UUID} - request:{request}")
    logging.info(f"{UUID} - request:{ambiente}")
    try:
        response = {'status': { 'code': 200, 'message': 'Sucesso Pam !!' }}
        """
        block code
        """
        
    except Exception as error:
        response = {'status': { 'code': 500, 'message': str(error) }}
        logging.error(f"{UUID} - {response}")
        collections_exceptions(response)
        
    finally:
        logging.info(f"{UUID}   - FIM REGRA NEGOCIO")
        
    return response