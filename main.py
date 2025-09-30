# -*- coding: utf-8 -*-

import sys
import logging
from tkinter import messagebox
import traceback

def setup_logging():
    """
    Configura o logger global para salvar em arquivo e formatar as mensagens
    de forma clara, incluindo o nome da thread de execução.
    """
    # Define o formato do log para incluir informações úteis de depuração
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(threadName)s: %(message)s')
    try:
        # Usa o modo 'a' (append) para não apagar logs antigos a cada execução
        log_file_handler = logging.FileHandler('sapiens.log', mode='a', encoding='utf-8')
        log_file_handler.setFormatter(log_formatter)

        # Configura o logger raiz
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG) # Captura todos os níveis de log
        root_logger.addHandler(log_file_handler)

        logging.info("================ INICIANDO APLICAÇÃO ================")
    except Exception as e:
        # Fallback para o console se o logging em arquivo falhar
        print(f"Falha crítica ao configurar o logging em arquivo: {e}", file=sys.stderr)
        traceback.print_exc()

def main():
    """Ponto de entrada principal da aplicação."""
    setup_logging()

    try:
        # A importação é feita aqui dentro para que um erro de dependência
        # possa ser capturado e exibido de forma amigável ao usuário.
        from ui.app import App
    except ImportError as e:
        error_message = (
            f"Erro de importação: {e}\n\n"
            "Dependências essenciais para a execução não foram encontradas.\n"
            "Por favor, certifique-se de que todas as bibliotecas necessárias estão instaladas, "
            "executando, por exemplo:\n"
            "pip install -r requirements.txt"
        )
        try:
            # Tenta mostrar um messagebox gráfico se o tkinter estiver minimamente disponível
            import tkinter as tk_error
            root_error = tk_error.Tk()
            root_error.withdraw() # Esconde a janela principal do tkinter
            messagebox.showerror("Erro Crítico de Dependências", error_message)
        except Exception:
            # Fallback para o console se a UI não puder ser iniciada de forma alguma
            print(error_message, file=sys.stderr)

        logging.critical(f"Erro de importação de dependência: {e}", exc_info=True)
        sys.exit(1) # Encerra a aplicação se dependências críticas faltam

    try:
        # CORREÇÃO: A classe App é instanciada sem argumentos.
        # A chamada `App(app_context)` estava incorreta e causava o TypeError.
        app = App()
        app.mainloop()
    except Exception as e:
        # Captura qualquer erro fatal e não previsto que possa ocorrer durante a execução
        logging.critical("Erro fatal e não capturado na aplicação.", exc_info=True)
        messagebox.showerror("Erro Fatal", f"Ocorreu um erro inesperado e a aplicação será encerrada.\n\nDetalhes: {e}")
    finally:
        logging.info("================ ENCERRANDO APLICAÇÃO ================\n")

if __name__ == "__main__":
    main()