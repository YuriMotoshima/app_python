import logging
from collections import namedtuple
from app.config.collections_exceptions import collections_exceptions

class ValidarRequests():
    def __init__(self, request, lista_regras:list, type_action:list=None) -> None:
        try:
            self.lista_regras = lista_regras
            self.type_action = type_action
            self.request = request.get_json()
            self.headers = request.headers
            self.ambiente = request.headers.get("ambiente").upper() if "Ambiente" in list(dict(request.headers).keys()) else None
            self.return_request = namedtuple("Retorno", ["code","message"])
            self.retorno_main = self.__verificar_main()
            logging.debug(f"status_code: {self.retorno_main.code} - message: {self.retorno_main.message}")
            
        except Exception as error:
            self.return_request(500, error)
            collections_exceptions(error)
        
    def __verificar_main(self) -> namedtuple:
        
        # Verifica se é chamada de Retry do Pipefy
        if (self.headers.get("Webhook-Retry") is not None) and \
           (self.headers.get("Webhook-Retry").lower() == "true"):
               message = "Chamada de Retry, ignorada."
               logging.info(message)
               return self.return_request(202, message)
           
        # Verifica se tem action e se o action é igual ao recebido
        elif (self.type_action is not None) and \
             (self.request["data"]["action"] not in self.type_action):
                message = "action não aceitável."
                logging.info(message)
                return self.return_request(406, message)
        
        # Verifica se NÃO tem ambiente OU se a regra NÃO está na lista de regras.
        elif (self.headers.get("Ambiente") is None) or \
             (self.headers.get("Role") not in self.lista_regras):
            message = "Parametros não recebidos."
            logging.info(message)
            return self.return_request(400, message)
        
        # Verifica se tem ambiente e se a regra está na lista de regras.
        elif (self.headers.get("Ambiente") is not None) or \
             (self.headers.get("Role") in self.lista_regras):
            message = "Success."
            logging.info(message)
            return self.return_request(200, message)
        
        else:
            message = "Internal server error."
            logging.error(message)
            return self.return_request(500, message)

    def __verificar_id_field(self, lista_field_id:list) -> bool:
        
        retorno = False
        if self.request["data"]["field"]["id"] in lista_field_id:
            retorno = True
        return retorno
        
    def __verificar_label_field(self, lista_label_id:list) -> bool:
        retorno = False
        
        if self.request["data"]["field"]["label"] in lista_label_id:
            retorno = True
        return retorno
        
    def __verificar_new_value(self, lista_new_value:list) -> bool:
        retorno = False
        self.new_value = self.request["data"]["new_value"]
        
        if self.request["data"]["new_value"] in lista_new_value:
            retorno = True
        return retorno
        
    def _validacao_principal(self, lista_field_id:list=[], lista_label_id:list=[], lista_new_value:list=[]) -> bool:
        retorno = False
        
        validacao_field_id = self.__verificar_id_field(lista_field_id=lista_field_id) if lista_field_id else True
        validacao_label_id = self.__verificar_label_field(lista_label_id=lista_label_id) if lista_label_id else True
        validacao_new_value = self.__verificar_new_value(lista_new_value=lista_new_value) if lista_new_value else True
        
        if all([validacao_field_id, validacao_label_id, validacao_new_value]):
            retorno = True
            
        return retorno