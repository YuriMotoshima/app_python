import sys
import time
import logging
from collections import namedtuple
from datetime import datetime, timedelta
from app import (UUID)
from concurrent.futures import (ProcessPoolExecutor, as_completed)


def _time_run(func):
    def _time_total(*args, **kwarg):
        time_init = time.time()
        retorno = func(*args, **kwarg)
        time_fim = time.time() - time_init
        logging.info(f"{UUID} - def name:{func.__name__} => Tempo Total em Segundos: {time_fim}")
        return retorno
    return _time_total


def run_cpu_tasks(*tasks: list) -> list:
    """ Executa tarefas de maneira assíncrona """
    try:
        tasks = tasks[0]
        with ProcessPoolExecutor() as exe:
            running_tasks = []
            for task in tasks:
                if type(task) == list:
                    running_tasks.append(exe.submit(task[0], *task[1:]))
                else:
                    running_tasks.append(exe.submit(task))

        running_tasks = [task.result() for task in as_completed(running_tasks) if task._exception == None]

        return running_tasks
    except Exception as error:
        print(error)

def _get_today_date_systems() -> datetime:
    plataform_linux = ['linux', 'linux1', 'linux2']
    date_today = datetime.now()
    try:
        if sys.platform in plataform_linux:
            date_today = (datetime.now() - timedelta(hours=3))
        return date_today
    except Exception as error:
        print(error)
        
def last_date(days=0): return _get_today_date_systems().date() - timedelta(days=days)

def future_date(days=0): return _get_today_date_systems().date() + timedelta(days=days)
    
def format_date(date): return datetime.strftime(date, "%d/%m/%Y")

def namedtuple(name_tuple:str, columns:list) -> namedtuple:
    return namedtuple(f"{name_tuple}", columns)

