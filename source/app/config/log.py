# reference https://github.com/YuriMotoshima/utils-api-pipefy

from os import (path, environ, getcwd, makedirs)
from sys import stdout
from datetime import datetime
import logging
from logging import (StreamHandler, basicConfig)
from dotenv import load_dotenv
from urllib3.connectionpool import log as urllib_log

load_dotenv(dotenv_path=fr"{getcwd()}\.env")

formatter = '[%(levelname)s]: [%(filename)s line - %(lineno)d] | Date_Time: %(asctime)s | Function: [%(funcName)s] | Message: ➪ %(message)s'
stdout_handler = StreamHandler(stdout)
handlers = [stdout_handler]
basicConfig(level=10,format=formatter, handlers=handlers, encoding='utf-8')

def loginit(name_file_log:str = "GPC", dev_env:str="DEV", disable_log:str=True):
    name_file_log = environ.get("LOGNAME") if environ.get("LOGNAME") else name_file_log
    dev_env = environ.get("LOGENV") if environ.get("LOGENV") else dev_env
    disable_log = environ.get("DISABLELOG") if environ.get("DISABLELOG") else disable_log
    valid_variables = list(set([name_file_log, dev_env, disable_log]))
    
    if (disable_log == True) or (disable_log == "True"):
        urllib_log.disabled = True
        return
        
    if valid_variables:
        filename = f'{name_file_log} - {datetime.now().strftime("%d-%m-%Y %H")}.log'
        filename = f'[DEV] {filename}' if dev_env == 'DEV' else f'[PROD] {filename}'
        dirname = f'{getcwd()}\\logs'
        makedirs(dirname, exist_ok=True)
        dirname = f'{getcwd()}\\logs\\{datetime.now().strftime("%d-%m-%Y")}'
        makedirs(dirname, exist_ok=True)
        full_filename = path.join(dirname, filename)
        file_handler = logging.FileHandler(filename=full_filename, encoding='utf-8')
        stdout_handler = logging.StreamHandler(stdout)
        handlers = [file_handler, stdout_handler]
        basicConfig(level=10,format=formatter, handlers=handlers, encoding='utf-8')