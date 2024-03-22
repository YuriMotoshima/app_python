from app.config.log import loginit
from sys import platform
import json
from uuid import uuid4
from os import (environ)
from app.config.collections_exceptions import collections_exceptions

loginit()

try:
    UUID = uuid4().__str__()
    
    NAME_SOURCER = environ.get("NAME_SOURCER", "Em desenvolvimento")
    VERSION_DEPLOY = environ.get("VERSION_DEPLOY", "1.0.0")
    DATE_TIME = environ.get("DATE_TIME", '2022-10-16 16:00')
    
    GCP_PROJECT = environ.get("GCP_PROJECT", None)
    GCP_DATABASE = environ.get("GCP_DATABASE", None)
    GCP_MONITOR_TABLE = environ.get("GCP_MONITOR_TABLE", None)
    
    LISTA_REGRAS = environ.get("LISTA_REGRAS", "['XPTO']")
    LISTA_ACOES = environ.get("LISTA_ACOES", "['XPTO']")
    
    _environ_pipefy = json.loads(environ.get("PIPEFY")) if environ.get("PIPEFY") else None
    _environ_gcp = json.loads(environ.get("GCP")) if environ.get("GCP") else None
    _locals = environ.get("LOCALS") if environ.get("LOCALS") else None
    secrets_gcp = None
    
    if _environ_pipefy:
        secrets = _environ_pipefy
    else:
        secrets = {
            "HOST_PIPE": environ.get("HOST_PIPE"),
            "PIPE": environ.get("PIPE"),
            "NONPHASES": environ.get("NONPHASES"),
            "TOKEN": environ.get("TOKEN"),
            "LOGENV": environ.get("LOGENV"),
            "LOGNAME": environ.get("LOGNAME"),
            "DISABLELOG": environ.get("DISABLELOG")
        }
        
    if _environ_gcp :
        secrets_gcp = _environ_gcp
    else:
        if (platform in ["win32", "win64"]):
            with open("./login.json") as config_file:
                app_config = json.load(config_file)
            secrets_gcp =   app_config
        
except Exception as error:
    raise collections_exceptions(error)
 
monitoramento_options = {
    "automation_name": NAME_SOURCER,
    "gcp_project": GCP_PROJECT,
    "dataset": GCP_DATABASE,
    "table": GCP_MONITOR_TABLE,
    "credentials_path": secrets_gcp,
    "disable_log": False,
    "uuid":UUID
}

_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Mobile Safari/537.36'
    }

response_fields_card='title assignees { id name email } comments { id } comments_count' \
                                ' current_phase { id name } pipe { id name } createdAt done due_date fields { field{id type} name value array_value } labels { id name } phases_history ' \
                                '{ phase { id name } firstTimeIn lastTimeOut } url '
                                
response_fields_organization = 'users{ id name email } pipes{ id name }'