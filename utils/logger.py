# -*- coding: utf-8 -*-

import logging
import queue
from datetime import datetime
from typing import Optional, Dict, Any
from logging import LogRecord
import threading
import logging.handlers
import sys
import atexit

def initialize_global_logging():
    """
    Inicializa a configuração global de logging.
    Deve ser chamada uma única vez no início da aplicação.
    """
    # Configura o logging global para um nível permissivo
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Garante que os loggers da biblioteca não propaguem para o root
    logging.getLogger('faster_whisper').propagate = False
    logging.getLogger('transformers').propagate = False
    logging.getLogger('tqdm').propagate = False
    
    # Registra um handler para tratar erros não capturados
    def handle_exception(exc_type: Any, exc_value: BaseException, exc_traceback: Any):
        if issubclass(exc_type, KeyboardInterrupt):
            # Ctrl+C - não faz log
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
            
        logging.error("Erro não capturado:", exc_info=(exc_type, exc_value, exc_traceback))
    
    sys.excepthook = handle_exception
    
    # Registra função de cleanup para fechar handlers no shutdown
    def cleanup():
        for handler in logging.getLogger().handlers[:]:
            try:
                handler.close()
            except:
                pass
            
    atexit.register(cleanup)

# Para evitar importação circular com DatabaseManager para type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core.database import DatabaseManager

class TaskIdFilter(logging.Filter):
    """Filtro para adicionar task_id aos registros de log."""
    def __init__(self):
        super().__init__()
        self._local = threading.local()
        self._default_task_id = 'Global'
        
    def set_task_id(self, task_id: Optional[str]) -> None:
        if task_id is None:
            task_id = self._default_task_id
        self._local.task_id = task_id
        
    def filter(self, record: LogRecord) -> bool:
        # Força a adição do task_id mesmo se já existir
        record.task_id = getattr(self._local, 'task_id', self._default_task_id)
        return True

class Logger:
    """
    Centraliza o sistema de logging, enviando mensagens para a UI (via queue),
    para um arquivo de log e, opcionalmente, para o banco de dados.
    Implementa a interface padrão de logging do Python para compatibilidade.
    """
    
    def debug(self, message: str, *args, **kwargs):
        """Interface padrão de logging para nível DEBUG."""
        self.log(str(message), "DEBUG", *args, **kwargs)
        
    def info(self, message: str, *args, **kwargs):
        """Interface padrão de logging para nível INFO."""
        self.log(str(message), "INFO", *args, **kwargs)
        
    def warning(self, message: str, *args, **kwargs):
        """Interface padrão de logging para nível WARNING."""
        self.log(str(message), "WARNING", *args, **kwargs)
        
    def error(self, message: str, *args, **kwargs):
        """Interface padrão de logging para nível ERROR."""
        self.log(str(message), "ERROR", *args, **kwargs)
        
    def critical(self, message: str, *args, **kwargs):
        """Interface padrão de logging para nível CRITICAL."""
        self.log(str(message), "CRITICAL", *args, **kwargs)
        
    def success(self, message: str, *args, **kwargs):
        """Método adicional para logs de sucesso."""
        self.log(str(message), "SUCCESS", *args, **kwargs)
    def __init__(self, log_queue: queue.Queue, db_manager: Optional['DatabaseManager'] = None):
        self.log_queue = log_queue
        self.db_manager = db_manager
        self.task_id_filter = TaskIdFilter()
        self._setup_logging()
        
    def _setup_logging(self):
        """Configura o sistema de logging com o filtro de task_id."""
        # Configura o root logger para garantir que todos os loggers herdem nossas configurações
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        
        # Limpa todos os handlers existentes
        root.handlers.clear()
        
        # Configura o formatter principal que inclui o task_id
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(task_id)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo com rotação
        file_handler = logging.handlers.RotatingFileHandler(
            'sapiens.log',
            maxBytes=5*1024*1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(self.task_id_filter)
        root.addHandler(file_handler)
        
        # Handler para Console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.addFilter(self.task_id_filter)
        root.addHandler(console_handler)
        
        # Configura o logger da aplicação
        self.logger = logging.getLogger('sapiens')
        self.logger.setLevel(logging.DEBUG)
        
        # Aplica o filtro globalmente para todos os loggers existentes
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            if not logger.filters or self.task_id_filter not in logger.filters:
                logger.addFilter(self.task_id_filter)

    def log(self, message: str, level: str="INFO", task_id: Optional[str]=None, to_ui: bool=True, exc_info=False):
        """
        Registra uma mensagem de log em múltiplos destinos de forma thread-safe.
        
        Args:
            message (str): A mensagem a ser logada.
            level (str): O nível do log (DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL).
            task_id (str, optional): O ID da tarefa associada.
            to_ui (bool): Se a mensagem deve ser enviada para a UI.
            exc_info (bool): Se informações de exceção devem ser incluídas.
        """
        try:
            log_level_map = {
                "DEBUG": logging.DEBUG,
                "INFO": logging.INFO,
                "SUCCESS": logging.INFO,
                "WARNING": logging.WARNING,
                "ERROR": logging.ERROR,
                "CRITICAL": logging.CRITICAL,
            }
            
            # Normaliza o nível do log
            level = level.upper()
            level_num = log_level_map.get(level, logging.INFO)
            
            # Define task_id para o filtro de forma thread-safe
            self.task_id_filter.set_task_id(task_id)
            
            # Tenta fazer o log para o arquivo
            try:
                self.logger.log(level_num, str(message), exc_info=exc_info)
            except Exception as e:
                # Se falhar, tenta fazer log direto para o console
                print(f"ERRO AO LOGAR: {e}\nMensagem original: {message}")
            
            # Log para a UI via Fila (thread-safe por natureza)
            if to_ui and self.log_queue is not None:
                try:
                    ui_message = f"{datetime.now().strftime('%H:%M:%S')} - [{level}] {message}\n"
                    self.log_queue.put_nowait(ui_message)
                except queue.Full:
                    # Se a fila estiver cheia, descarta a mensagem da UI mas mantém no arquivo
                    pass
                except Exception:
                    # Ignora outros erros de UI para não impactar o funcionamento principal
                    pass

            # Log para o Banco de Dados (já é thread-safe via queue)
            if self.db_manager:
                try:
                    self.db_manager._enqueue_sql(
                        "INSERT INTO log_entries (task_id, level, message) VALUES (?, ?, ?)",
                        (task_id or 'Global', level, str(message))
                    )
                except Exception:
                    # Ignora erros de BD para não impactar o funcionamento principal
                    pass
                    
        except Exception as e:
            # Último recurso: tenta imprimir direto no console
            print(f"ERRO CRÍTICO NO SISTEMA DE LOG: {e}\nTentando logar: [{level}] {message}")
        finally:
            # Sempre limpa o task_id após o log para evitar vazamentos
            self.task_id_filter.set_task_id(None)
