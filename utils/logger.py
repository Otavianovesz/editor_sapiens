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
    Initializes the global logging configuration.
    Should be called once at the beginning of the application.
    """
    # Configures global logging to a permissive level
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Ensures that library loggers do not propagate to the root
    logging.getLogger('faster_whisper').propagate = False
    logging.getLogger('transformers').propagate = False
    logging.getLogger('tqdm').propagate = False
    
    # Registers a handler to handle uncaught errors
    def handle_exception(exc_type: Any, exc_value: BaseException, exc_traceback: Any):
        if issubclass(exc_type, KeyboardInterrupt):
            # Ctrl+C - does not log
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
            
        logging.error("Uncaught error:", exc_info=(exc_type, exc_value, exc_traceback))
    
    sys.excepthook = handle_exception
    
    # Registers a cleanup function to close handlers on shutdown
    def cleanup():
        for handler in logging.getLogger().handlers[:]:
            try:
                handler.close()
            except:
                pass
            
    atexit.register(cleanup)

# To avoid circular import with DatabaseManager for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core.database import DatabaseManager

class TaskIdFilter(logging.Filter):
    """Filter to add task_id to log records."""
    def __init__(self):
        """Initializes the TaskIdFilter."""
        super().__init__()
        self._local = threading.local()
        self._default_task_id = 'Global'
        
    def set_task_id(self, task_id: Optional[str]) -> None:
        """Sets the task_id for the current thread.

        Args:
            task_id (Optional[str]): The ID of the task.
        """
        if task_id is None:
            task_id = self._default_task_id
        self._local.task_id = task_id
        
    def filter(self, record: LogRecord) -> bool:
        """Filters the log record.

        Args:
            record (LogRecord): The log record.

        Returns:
            bool: True if the record should be logged, False otherwise.
        """
        # Forces the addition of task_id even if it already exists
        record.task_id = getattr(self._local, 'task_id', self._default_task_id)
        return True

class Logger:
    """
    Centralizes the logging system, sending messages to the UI (via queue),
    to a log file and, optionally, to the database.
    Implements the standard Python logging interface for compatibility.
    """
    
    def debug(self, message: str, *args, **kwargs):
        """Standard logging interface for the DEBUG level."""
        self.log(str(message), "DEBUG", *args, **kwargs)
        
    def info(self, message: str, *args, **kwargs):
        """Standard logging interface for the INFO level."""
        self.log(str(message), "INFO", *args, **kwargs)
        
    def warning(self, message: str, *args, **kwargs):
        """Standard logging interface for the WARNING level."""
        self.log(str(message), "WARNING", *args, **kwargs)
        
    def error(self, message: str, *args, **kwargs):
        """Standard logging interface for the ERROR level."""
        self.log(str(message), "ERROR", *args, **kwargs)
        
    def critical(self, message: str, *args, **kwargs):
        """Standard logging interface for the CRITICAL level."""
        self.log(str(message), "CRITICAL", *args, **kwargs)
        
    def success(self, message: str, *args, **kwargs):
        """Additional method for success logs."""
        self.log(str(message), "SUCCESS", *args, **kwargs)
    def __init__(self, log_queue: queue.Queue, db_manager: Optional['DatabaseManager'] = None):
        """Initializes the Logger.

        Args:
            log_queue (queue.Queue): The queue for log messages to the UI.
            db_manager (Optional['DatabaseManager'], optional): The database manager. Defaults to None.
        """
        self.log_queue = log_queue
        self.db_manager = db_manager
        self.task_id_filter = TaskIdFilter()
        self._setup_logging()
        
    def _setup_logging(self):
        """Configures the logging system with the task_id filter."""
        # Configures the root logger to ensure that all loggers inherit our settings
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        
        # Clears all existing handlers
        root.handlers.clear()
        
        # Configures the main formatter that includes the task_id
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(task_id)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler for file with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            'sapiens.log',
            maxBytes=5*1024*1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(self.task_id_filter)
        root.addHandler(file_handler)
        
        # Handler for Console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.addFilter(self.task_id_filter)
        root.addHandler(console_handler)
        
        # Configures the application logger
        self.logger = logging.getLogger('sapiens')
        self.logger.setLevel(logging.DEBUG)
        
        # Applies the filter globally to all existing loggers
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            if not logger.filters or self.task_id_filter not in logger.filters:
                logger.addFilter(self.task_id_filter)

    def log(self, message: str, level: str="INFO", task_id: Optional[str]=None, to_ui: bool=True, exc_info=False):
        """
        Records a log message in multiple destinations in a thread-safe manner.
        
        Args:
            message (str): The message to be logged.
            level (str): The log level (DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL).
            task_id (str, optional): The ID of the associated task.
            to_ui (bool): If the message should be sent to the UI.
            exc_info (bool): If exception information should be included.
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
            
            # Normalizes the log level
            level = level.upper()
            level_num = log_level_map.get(level, logging.INFO)
            
            # Defines task_id for the filter in a thread-safe way
            self.task_id_filter.set_task_id(task_id)
            
            # Tries to log to the file
            try:
                self.logger.log(level_num, str(message), exc_info=exc_info)
            except Exception as e:
                # If it fails, tries to log directly to the console
                print(f"ERROR WHEN LOGGING: {e}\nOriginal message: {message}")
            
            # Logs to the UI via Queue (thread-safe by nature)
            if to_ui and self.log_queue is not None:
                try:
                    ui_message = f"{datetime.now().strftime('%H:%M:%S')} - [{level}] {message}\n"
                    self.log_queue.put_nowait(ui_message)
                except queue.Full:
                    # If the queue is full, it discards the UI message but keeps it in the file
                    pass
                except Exception:
                    # Ignores other UI errors so as not to impact the main functioning
                    pass

            # Logs to the Database (it is already thread-safe via queue)
            if self.db_manager:
                try:
                    self.db_manager._enqueue_sql(
                        "INSERT INTO log_entries (task_id, level, message) VALUES (?, ?, ?)",
                        (task_id or 'Global', level, str(message))
                    )
                except Exception:
                    # Ignores DB errors so as not to impact the main functioning
                    pass
                    
        except Exception as e:
            # Last resort: tries to print directly to the console
            print(f"CRITICAL ERROR IN THE LOGGING SYSTEM: {e}\nTrying to log: [{level}] {message}")
        finally:
            # Always clears the task_id after logging to avoid leaks
            self.task_id_filter.set_task_id(None)
