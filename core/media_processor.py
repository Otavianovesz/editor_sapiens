# -*- coding: utf-8 -*-

import sys
import logging
import faulthandler
import threading
import queue
import gc
import os
from typing import NoReturn, Optional
from tkinter import messagebox
from types import FrameType
from typing import Union, Type, Optional

class LoggerAdapter(logging.LoggerAdapter):
    """Adapter para garantir que task_id esteja sempre presente"""
    def process(self, msg, kwargs):
        kwargs.setdefault('extra', {}).setdefault('task_id', 'Global')
        return msg, kwargs

def setup_logging() -> None:
    """Configura o logger global com rotação de arquivos e níveis adequados."""
    try:
        log_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] - %(threadName)s: [%(task_id)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Configuração do arquivo de log com rotação
        from logging.handlers import RotatingFileHandler
        log_file_handler = RotatingFileHandler(
            'sapiens.log',
            mode='a',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        log_file_handler.setFormatter(log_formatter)
        
        # Configuração do logger raiz
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Limpa handlers existentes
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            
        root_logger.addHandler(log_file_handler)
        
        # Configura o adapter global
        global logger
        logger = LoggerAdapter(root_logger, {'task_id': 'Global'})
        
        logger.info("================ INICIANDO APLICAÇÃO ================")
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao configurar logging: {e}", file=sys.stderr)
        raise

def cleanup_threads() -> None:
    """Realiza limpeza sistemática de recursos do sistema."""
    def safe_thread_stop(thread: threading.Thread) -> None:
        try:
            if hasattr(thread, '_stop'):
                thread._stop()
        except Exception as e:
            logger.warning(f"Falha ao interromper thread {thread.name}: {e}")

    try:
        # 1. Coleta de lixo forçada
        gc.collect()
        
        # 2. Limpeza de progressbar
        try:
            from tqdm import tqdm
            tqdm._instances.clear()
        except Exception as e:
            logger.debug(f"Ignorando erro na limpeza tqdm: {e}")

        # 3. Interrupção controlada de threads
        current = threading.current_thread()
        for thread in threading.enumerate():
            if thread is not current and not thread.daemon:
                safe_thread_stop(thread)
                
    except Exception as e:
        logger.error(f"Erro durante cleanup: {e}", exc_info=True)

def setup_thread_excepthook() -> None:
    """Configura handler global para exceções não tratadas em threads."""
    
    def custom_thread_excepthook(args: threading.ExceptHookArgs) -> None:
        error_msg = (
            f"Erro não tratado em thread {args.thread.name}: "
            f"{args.exc_type.__name__}: {args.exc_value}"
        )
        
        logger.critical(error_msg, 
                        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
                        extra={'task_id': 'Global'})
        
        if isinstance(args.exc_value, (SystemExit, KeyboardInterrupt)):
            logger.info("Encerrando aplicação por sinal de interrupção",
                        extra={'task_id': 'Global'})
            cleanup_threads()
            os._exit(1)

    threading.excepthook = custom_thread_excepthook

def signal_handler(signum: int, frame: Optional[FrameType]) -> NoReturn:
    """Handler unificado para sinais do sistema."""
    logger.info(f"Sinal recebido: {signum}", extra={'task_id': 'Global'})
    cleanup_threads()
    sys.exit(0)

def main() -> int:
    """Ponto de entrada principal com gestão robusta de erros."""
    try:
        # Configurações iniciais
        faulthandler.enable()
        setup_logging()
        setup_thread_excepthook()
        
        # Configuração de sinais
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            from ui.app import App
        except ImportError as e:
            error_message = (
                f"Erro de importação: {e}\n\n"
                "Dependências essenciais não encontradas.\n"
                "Execute: pip install -r requirements.txt"
            )
            logger.critical(f"Falha na importação: {e}", exc_info=True)
            
            try:
                import tkinter as tk
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror("Erro Crítico", error_message)
            except Exception:
                print(error_message, file=sys.stderr)
            return 1

        try:
            app = App()
            app.mainloop()
        except Exception as e:
            error_msg = f"Erro fatal na aplicação: {type(e).__name__}: {e}"
            logger.critical(error_msg, exc_info=True)
            
            try:
                messagebox.showerror("Erro Fatal", error_msg)
            except Exception:
                print(error_msg, file=sys.stderr)
            return 1
            
    except Exception as e:
        print(f"Erro fatal: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            cleanup_threads()
            logger.info("================ ENCERRANDO APLICAÇÃO ================")
        except Exception as e:
            print(f"Erro na finalização: {e}", file=sys.stderr)
            
    return 0

if __name__ == "__main__":
    sys.exit(main())

class MediaProcessor:
    def __init__(self, app_context):
        self.app_context = app_context
        self.temp_files = set()
    
    def register_temp_file(self, path: str) -> None:
        self.temp_files.add(path)
        self.app_context.register_temp_file(path)
    
    def cleanup(self) -> None:
        """Limpa recursos do processador"""
        for path in self.temp_files:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                logger.warning(f"Erro ao remover arquivo temporário: {path}: {e}")
        self.temp_files.clear()