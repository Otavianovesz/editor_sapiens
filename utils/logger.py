# -*- coding: utf-8 -*-

import logging
import queue
from datetime import datetime
from typing import Optional

# Para evitar importação circular com DatabaseManager para type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core.database import DatabaseManager

class Logger:
    """
    Centraliza o sistema de logging, enviando mensagens para a UI (via queue),
    para um arquivo de log e, opcionalmente, para o banco de dados.
    """
    def __init__(self, log_queue: queue.Queue, db_manager: Optional['DatabaseManager'] = None):
        self.log_queue = log_queue
        self.db_manager = db_manager

    def log(self, message: str, level: str="INFO", task_id: Optional[str]=None, to_ui: bool=True, exc_info=False):
        """
        Registra uma mensagem de log em múltiplos destinos.
        
        Args:
            message (str): A mensagem a ser logada.
            level (str): O nível do log (DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL).
            task_id (str, optional): O ID da tarefa associada.
            to_ui (bool): Se a mensagem deve ser enviada para a UI.
            exc_info (bool): Se informações de exceção devem ser incluídas.
        """
        log_level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "SUCCESS": logging.INFO, # Mapeia para INFO no arquivo, mas é distinto na UI
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        
        # Log para o arquivo
        logging.log(log_level_map.get(level.upper(), logging.INFO), f"[Task: {task_id or 'Global'}] {message}", exc_info=exc_info)

        # Log para a UI via Fila
        if to_ui:
            self.log_queue.put(f"{datetime.now().strftime('%H:%M:%S')} - [{level.upper()}] {message}\n")

        # Log para o Banco de Dados
        if self.db_manager:
            self.db_manager._enqueue_sql("INSERT INTO log_entries (task_id, level, message) VALUES (?, ?, ?)", (task_id, level, message))
