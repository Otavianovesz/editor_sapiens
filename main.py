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


def main():
    """Ponto de entrada principal da aplicação."""
    faulthandler.enable()  # Habilita captura de falhas nativas
    setup_logging()

    try:
        # A importação é feita aqui dentro para que o erro de dependência
        # possa ser capturado e exibido ao usuário.
        from ui.app import App
    except ImportError as e:
        error_message = (f"Erro de importação: {e}\n\nDependências essenciais não encontradas.\n"
                         "Por favor, instale as dependências com:\n"
                         "pip install -r requirements.txt")
        try:
            # Tenta mostrar um messagebox se tkinter estiver disponível
            import tkinter as tk_error
            root_error = tk_error.Tk()
            root_error.withdraw()
            messagebox.showerror("Erro Crítico de Dependências", error_message)
        except Exception:
            # Fallback para o console se a UI não puder ser iniciada
            print(error_message, file=sys.stderr)
        logging.critical(f"Erro de importação de dependência: {e}", exc_info=True)
        sys.exit(1)

    try:
        app = App()
        app.mainloop()
    except Exception as e:
        logging.critical("Erro fatal e não capturado na aplicação.", exc_info=True)
        messagebox.showerror("Erro Fatal", f"Ocorreu um erro inesperado e a aplicação será encerrada.\n\nDetalhes: {e}")
    finally:
        logging.info("================ ENCERRANDO APLICAÇÃO ================\n")

if __name__ == "__main__":
    main()
