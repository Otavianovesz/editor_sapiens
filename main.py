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
from contextlib import contextmanager

class ApplicationError(Exception):
    """Erro base para exceções da aplicação"""
    pass

# Removido LoggerAdapter pois agora usamos nossa própria implementação de Logger

@contextmanager
def safe_logging_context():
    """Context manager para garantir inicialização segura do logging"""
    logger = None
    try:
        logger = setup_logging()
        yield logger
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao configurar logging: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if logger:
            logger.log("Finalizando contexto de logging", "DEBUG")

def setup_logging() -> logging.Logger:
    """
    Configura o sistema de logging usando nossa implementação robusta.
    """
    try:
        from utils.logger import initialize_global_logging, Logger
        from queue import Queue
        
        # Inicializa configuração global
        initialize_global_logging()
        
        # Cria queue para logs da UI
        log_queue = Queue()
        
        # Cria nossa implementação de logger
        logger = Logger(log_queue)
        logger.log("================ INICIANDO APLICAÇÃO ================", "INFO")
        
        return logger
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao configurar logging: {e}", file=sys.stderr)
        raise

class ApplicationContext:
    """Contexto global da aplicação para gerenciamento de recursos"""
    def __init__(self, logger):
        self.logger = logger
        self.temp_files = set()
        self.active_processors = set()
        self._lock = threading.Lock()
        self._is_shutting_down = False
        self.task_id = 'Global'  # Task ID padrão para operações globais
    
    def register_temp_file(self, path: str) -> None:
        with self._lock:
            self.temp_files.add(path)
    
    def register_processor(self, processor: object) -> None:
        with self._lock:
            self.active_processors.add(processor)
            
    def check_processors_health(self) -> bool:
        """Verifica a saúde dos processadores registrados"""
        with self._lock:
            for processor in self.active_processors:
                try:
                    if hasattr(processor, 'is_healthy') and not processor.is_healthy():
                        return False
                except Exception as e:
                    self.logger.warning(f"Erro ao verificar saúde do processador {processor.__class__.__name__}: {e}")
                    return False
            return True

    def cleanup(self) -> None:
        """Limpa todos os recursos registrados"""
        with self._lock:
            if self._is_shutting_down:
                return
            
            self._is_shutting_down = True
            
            # Limpa arquivos temporários
            for path in self.temp_files:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception as e:
                    self.logger.warning(f"Falha ao remover arquivo temporário {path}: {e}")
            
            # Limpa processadores ativos
            for processor in self.active_processors:
                try:
                    if hasattr(processor, 'cleanup'):
                        processor.cleanup()
                except Exception as e:
                    self.logger.warning(f"Falha ao limpar processador {processor.__class__.__name__}: {e}")
            
            self.temp_files.clear()
            self.active_processors.clear()
            self._is_shutting_down = False

def cleanup_threads(app_context: ApplicationContext, logger: logging.Logger) -> None:
    """Realiza limpeza sistemática de recursos do sistema."""
    def safe_thread_stop(thread: threading.Thread) -> None:
        try:
            if hasattr(thread, '_stop'):
                thread._stop()
        except Exception as e:
            logger.warning(f"Falha ao interromper thread {thread.name}: {e}")

    try:
        # 1. Limpa recursos da aplicação
        app_context.cleanup()
        
        # 2. Coleta de lixo forçada
        gc.collect()
        
        # 3. Limpeza de progressbar
        try:
            from tqdm import tqdm
            tqdm._instances.clear()
        except Exception as e:
            logger.debug(f"Ignorando erro na limpeza tqdm: {e}")

        # 4. Interrupção controlada de threads
        current = threading.current_thread()
        for thread in threading.enumerate():
            if thread is not current and not thread.daemon:
                safe_thread_stop(thread)
                
    except Exception as e:
        logger.error(f"Erro durante cleanup: {e}", exc_info=True)

def setup_thread_excepthook(logger: logging.Logger, app_context: ApplicationContext) -> None:
    """Configura handler global para exceções não tratadas em threads."""
    def custom_thread_excepthook(args: threading.ExceptHookArgs) -> None:
        error_msg = (
            f"Erro não tratado em thread {args.thread.name}: "
            f"{args.exc_type.__name__}: {args.exc_value}"
        )
        
        logger.critical(error_msg, 
                     exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
        
        if isinstance(args.exc_value, (SystemExit, KeyboardInterrupt)):
            logger.info("Encerrando aplicação por sinal de interrupção")
            cleanup_threads(app_context, logger)
            os._exit(1)

    threading.excepthook = custom_thread_excepthook

def signal_handler(signum: int, frame: Optional[FrameType], 
                  logger: logging.Logger, app_context: ApplicationContext) -> NoReturn:
    """Handler unificado para sinais do sistema."""
    logger.info(f"Sinal recebido: {signum}")
    cleanup_threads(app_context, logger)
    sys.exit(0)

def main() -> int:
    """Ponto de entrada principal com gestão robusta de erros."""
    try:
        # 1. Habilita fault handler
        faulthandler.enable()
        
        # 2. Inicializa logging de forma segura
        with safe_logging_context() as logger:
            # 3. Cria contexto da aplicação
            app_context = ApplicationContext(logger)
            
            # 4. Configura handlers de exceção
            setup_thread_excepthook(logger, app_context)
            
            # 5. Configura sinais
            import signal
            signal.signal(signal.SIGINT, 
                        lambda s, f: signal_handler(s, f, logger, app_context))
            signal.signal(signal.SIGTERM, 
                        lambda s, f: signal_handler(s, f, logger, app_context))

            try:
                # 6. Importa e inicializa App
                from ui.app import App
                
                # Verifica saúde do contexto antes de iniciar
                if not app_context.check_processors_health():
                    raise ApplicationError("Falha na inicialização de processadores críticos")
                    
                app = App(app_context)
                app.mainloop()
                
                # Verifica se houve erro na execução
                if not app_context.check_processors_health():
                    raise ApplicationError("Processadores em estado inválido após execução")
                    
                return 0
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
            except Exception as e:
                logger.critical(f"Erro fatal na aplicação: {e}", exc_info=True)
                try:
                    messagebox.showerror("Erro Fatal", str(e))
                except Exception:
                    print(f"Erro fatal: {e}", file=sys.stderr)
                return 1
            finally:
                try:
                    cleanup_threads(app_context, logger)
                except Exception as e:
                    logger.error(f"Erro durante limpeza final: {e}", exc_info=True)
                finally:
                    logger.info("================ ENCERRANDO APLICAÇÃO ================")
                    # Força o flush dos logs
                    for handler in logging.getLogger().handlers:
                        handler.flush()
    except Exception as e:
        print(f"Erro fatal durante inicialização: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
