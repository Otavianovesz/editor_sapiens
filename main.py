# -*- coding: utf-8 -*-

import sys
import logging
import faulthandler
from tkinter import messagebox

def setup_logging():
    """Configura o logger global para salvar em arquivo."""
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(threadName)s: %(message)s')
    try:
        log_file_handler = logging.FileHandler('sapiens.log', mode='a', encoding='utf-8')
        log_file_handler.setFormatter(log_formatter)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(log_file_handler)
        logging.info("================ INICIANDO APLICAÇÃO ================")
    except Exception as e:
        print(f"Falha ao configurar o logging em arquivo: {e}", file=sys.stderr)


def cleanup_threads():
    """Limpa threads órfãs e recursos do sistema."""
    import threading
    import tqdm
    import gc
    import os
    
    # 1. Força coleta de lixo
    gc.collect()
    
    # 2. Limpa monitores tqdm
    try:
        tqdm.tqdm._instances.clear()
    except Exception:
        pass
    
    # 3. Interrompe threads não-daemon
    for thread in threading.enumerate():
        if thread is not threading.current_thread() and not thread.daemon:
            try:
                thread._stop()
            except Exception:
                pass

def setup_thread_excepthook():
    """Configura tratamento global de exceções em threads."""
    import threading
    import sys
    import queue
    
    def custom_thread_excepthook(args):
        import os
        
        # Log da exceção
        logging.critical(
            f"Erro não tratado em thread {args.thread.name}: {args.exc_value}",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
        )
        
        # Força encerramento em caso de erro fatal
        if isinstance(args.exc_value, (SystemExit, KeyboardInterrupt)):
            cleanup_threads()
            os._exit(1)
    
    threading.excepthook = custom_thread_excepthook

def main():
    """Ponto de entrada principal da aplicação."""
    # 1. Configurações iniciais
    faulthandler.enable()
    setup_logging()
    setup_thread_excepthook()
    
    # 2. Configura tratamento de sinais
    import signal
    def signal_handler(signum, frame):
        logging.info(f"Sinal recebido: {signum}")
        cleanup_threads()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 3. Importa App com tratamento de erro
        try:
            from ui.app import App
        except ImportError as e:
            error_message = (
                f"Erro de importação: {e}\n\n"
                "Dependências essenciais não encontradas.\n"
                "Por favor, instale as dependências com:\n"
                "pip install -r requirements.txt"
            )
            try:
                import tkinter as tk_error
                root_error = tk_error.Tk()
                root_error.withdraw()
                messagebox.showerror("Erro Crítico de Dependências", error_message)
            except Exception:
                print(error_message, file=sys.stderr)
            logging.critical(f"Erro de importação de dependência: {e}", exc_info=True)
            return 1

        # 4. Inicia aplicação com tratamento de erro
        try:
            app = App()
            app.mainloop()
        except Exception as e:
            logging.critical("Erro fatal na aplicação.", exc_info=True)
            try:
                messagebox.showerror(
                    "Erro Fatal",
                    f"Erro fatal na aplicação.\n\nDetalhes: {e}"
                )
            except Exception:
                print(f"Erro fatal: {e}", file=sys.stderr)
            return 1
            
    finally:
        # 5. Limpeza final
        try:
            cleanup_threads()
            logging.info("================ ENCERRANDO APLICAÇÃO ================\n")
        except Exception as e:
            logging.error(f"Erro na limpeza final: {e}", exc_info=True)
            
    return 0

if __name__ == "__main__":
    sys.exit(main())

if __name__ == "__main__":
    main()
