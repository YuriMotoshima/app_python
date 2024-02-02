# pip install Flask, autopep8, pandas-gbq, python-dotenv, google-cloud-bigquery, google-cloud-storage
# pip install utils-api-pipefy, pandas-gbq, openpyxl, lxml, Flask, autopep8, python-dotenv, google-cloud-bigquery, google-cloud-storage

import logging
from uuid import uuid4
from sys import platform
from json import loads
from flask import Flask, request, jsonify
from app.config.collections_exceptions import collections_exceptions

from app.modules.gcp_connection import monitoramento
from app.modules.validacao_requests import ValidarRequests
from app.app import run
from app import (NAME_SOURCER, VERSION_DEPLOY, DATE_TIME, UUID, monitoramento_options, LISTA_REGRAS, LISTA_ACOES, _locals)

@monitoramento(**monitoramento_options, process_name=NAME_SOURCER)
def process(request):
    UUID = uuid4().__str__()
    
    try:
        
        valid = ValidarRequests(request=request, lista_regras=loads(LISTA_REGRAS), type_action=loads(LISTA_ACOES))
        code = valid.retorno_main.code
        message = valid.retorno_main.message
        
        response = {'status': {'code': 500, 'message': message } }
        
        if code == 200:
            if valid.ambiente == "TEST":
                message = f'NAME_SOURCER: {NAME_SOURCER} - VERSION_DEPLOY: {VERSION_DEPLOY} - DATE_TIME: {DATE_TIME}'
                response = {'status': {'code': code, 'message': message } }
                
            elif (valid.ambiente in ["DEV", "HML", "PROD"]) and (valid.headers['Host'] == _locals):
                response = run(request=valid.request, headers=valid.headers, ambiente=valid.ambiente)
            
    except Exception as error:
        code = 500
        logging.info(collections_exceptions(error))

    logging.info(f"{UUID} - response:{response}")
    return jsonify(response), code


def main(request):
    return process(request)


app = Flask(__name__)

@app.route('/main', methods=['POST'])
def main_flask():
    return process(request)

if (__name__ == "__main__") and \
    platform in ["win32", "win64"]:
        app.run(host='0.0.0.0', port=8090, debug=True)