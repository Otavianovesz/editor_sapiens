# -*- coding: utf-8 -*-

import sqlite3
import queue
import threading
import logging
import json
import time
from tkinter import messagebox
from typing import List, Dict, Any

from utils.constants import *

class DatabaseManager:
    """ Gerenciador de banco de dados com thread de escrita dedicada e otimizações. """
    def __init__(self, db_path: str = "editor_sapiens.db"):
        """Inicializa o DatabaseManager com proteções extras e verificação de estado."""
        self.db_path = db_path
        self._write_queue: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
        self._writer_thread = None
        self._writer_started = threading.Event()
        self._writer_stop = threading.Event()
        self._write_lock = threading.Lock()
        self._init_lock = threading.Lock()
        self._initialized = False
        self.is_writer_healthy = False
        self._read_conn = None

        try:
            self._read_conn = sqlite3.connect(
                self.db_path, 
                check_same_thread=True, 
                detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._read_conn.row_factory = sqlite3.Row
            self._apply_performance_pragmas(self._read_conn)
        except sqlite3.Error as e:
            logging.critical(f"Falha CRÍTICA ao abrir conexão de leitura do DB: {e}", exc_info=True)
            messagebox.showerror(
                "Erro de Banco de Dados", 
                f"Não foi possível abrir o banco de dados. A aplicação pode não funcionar. Erro: {e}"
            )
            return

        # Inicializa schema se necessário
        if self._read_conn:
            self._initialize_db_schema()

        # Inicialização protegida da thread de escrita
        self._start_writer_thread()
        
        # Verifica se a thread iniciou corretamente
        if not self._writer_started.wait(timeout=5):
            logging.error("Timeout: A thread de escrita do DB não iniciou.")
        else:
            self._health_check_writer()

    def _start_writer_thread(self):
        """Inicia a thread de escrita de forma segura com verificações de estado."""
        with self._init_lock:
            if self._initialized:
                logging.warning("Tentativa de inicializar DatabaseManager novamente ignorada")
                return
            
            if self._writer_thread is not None and self._writer_thread.is_alive():
                logging.warning("Thread de escrita já está ativa")
                return
                
            try:
                self._writer_thread = threading.Thread(
                    target=self._writer_loop,
                    name="DBWriterThread",
                    daemon=True
                )
                self._writer_thread.start()
                
                # Aguarda a thread inicializar
                if not self._writer_started.wait(timeout=5):
                    raise TimeoutError("Thread de escrita não iniciou no tempo esperado")
                
                self._health_check_writer()
                self._initialized = True
                logging.info("Thread de escrita iniciada com sucesso")
                
            except Exception as e:
                logging.critical(f"Falha ao iniciar thread de escrita: {e}", exc_info=True)
                self._writer_thread = None
                raise

    def _apply_performance_pragmas(self, conn):
        """Aplica configurações de performance ao SQLite."""
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA cache_size = -32768;") # Cache de 32MB
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA busy_timeout = 5000;")

    def _health_check_writer(self):
        res = self._enqueue_callable(lambda conn: conn.cursor().execute("SELECT 1").fetchone(), wait=True, timeout=2)
        if res.get("ok") and res.get("result"): self.is_writer_healthy = True; logging.info("Health check da thread de escrita: SUCESSO.")
        else: self.is_writer_healthy = False; logging.error(f"Health check da thread de escrita: FALHA. Erro: {res.get('error')}")

    def _initialize_db_schema(self):
        with self._read_conn as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS tarefas_fila (
                id TEXT PRIMARY KEY, video_path TEXT NOT NULL, status TEXT DEFAULT 'Aguardando',
                use_visual_analysis INTEGER DEFAULT 1, transcription_mode TEXT DEFAULT 'whisper',
                transcription_path TEXT, render_script_path TEXT, display_order REAL DEFAULT 0.0,
                operation_mode TEXT DEFAULT 'full_pipe', added_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                render_metadata TEXT)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS presets (name TEXT PRIMARY KEY, config TEXT NOT NULL)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS log_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, level TEXT, message TEXT)''')

    def _writer_loop(self):
        """Thread principal de escrita no banco de dados com proteções extras."""
        write_conn = None
        current_op = None
        
        try:
            # Inicialização da conexão com retry
            for attempt in range(3):
                try:
                    write_conn = sqlite3.connect(
                        self.db_path, 
                        check_same_thread=True, 
                        detect_types=sqlite3.PARSE_DECLTYPES,
                        timeout=20  # Aumenta timeout para evitar bloqueios
                    )
                    self._apply_performance_pragmas(write_conn)
                    break
                except sqlite3.Error as e:
                    if attempt == 2:  # Última tentativa
                        raise
                    logging.warning(f"Tentativa {attempt + 1} de conectar ao DB falhou: {e}")
                    time.sleep(1)
                    
            self._writer_started.set()
            
        except Exception as e:
            logging.critical(f"Falha fatal ao iniciar conexão de escrita do DB: {e}", exc_info=True)
            self._writer_started.set()
            return

        try:
            while not self._writer_stop.is_set():
                # Reset da operação atual
                current_op = None
                
                try:
                    # Usa timeout para poder checar periodicamente o evento de parada
                    current_op = self._write_queue.get(timeout=1.0)
                    
                    # Sentinel check
                    if current_op is None:
                        # Marca tarefa como concluída antes de quebrar o loop
                        self._write_queue.task_done()
                        break
                        
                    try:
                        with self._write_lock:
                            with write_conn:
                                result = None
                                if current_op.get("callable"):
                                    result = current_op["callable"](write_conn)
                                else:
                                    cur = write_conn.cursor()
                                    cur.execute(current_op["sql"], current_op.get("params", ()))
                                    result = {"rowcount": cur.rowcount}
                                
                                if current_op.get("wait_for_result") and "response_q" in current_op:
                                    current_op["response_q"].put({"ok": True, "result": result})
                                    
                    except Exception as e:
                        logging.error(f"Erro na operação de escrita do DB: {e}", exc_info=True)
                        if current_op.get("wait_for_result") and "response_q" in current_op:
                            current_op["response_q"].put({"ok": False, "error": str(e)})
                    
                    finally:
                        # Só marca como concluído se realmente pegamos uma tarefa
                        if current_op is not None:
                            self._write_queue.task_done()
                            
                except queue.Empty:
                    # Timeout normal, continua o loop
                    continue
                    
                except Exception as e:
                    logging.error(f"Erro no loop de escrita do DB: {e}", exc_info=True)
                    # Não chama task_done() aqui pois podemos não ter obtido uma tarefa
                    
        except Exception as e:
            logging.critical(f"Erro fatal no loop de escrita: {e}", exc_info=True)
            
        finally:
            # Cleanup da conexão
            if write_conn:
                try:
                    write_conn.close()
                except Exception as e:
                    logging.error(f"Erro ao fechar conexão de escrita: {e}", exc_info=True)

    def _enqueue_operation(self, op: dict, wait: bool, timeout: float):
        """Enfileira uma operação de forma thread-safe com validação"""
        if not isinstance(op, dict):
            logging.error(f"Operação inválida: esperado dict, recebido {type(op)}")
            return {"ok": False, "error": "Operação inválida"}
            
        try:
            # Prepara queue de resposta se necessário
            if wait:
                op["response_q"] = queue.Queue(maxsize=1)
                op["wait_for_result"] = True
            
            # Tenta enfileirar com timeout
            try:
                self._write_queue.put(op, timeout=timeout)
            except queue.Full:
                error_msg = f"Fila de escrita cheia (timeout={timeout}s)"
                logging.error(error_msg)
                return {"ok": False, "error": error_msg}
                
            # Se não precisa esperar resposta, retorna
            if not wait:
                return {"ok": True}
                
            # Aguarda resposta com timeout
            try:
                result = op["response_q"].get(timeout=timeout)
                op["response_q"].task_done()  # Limpa a fila de resposta
                return result
            except queue.Empty:
                error_msg = f"Timeout ({timeout}s) aguardando resposta do DB"
                logging.error(error_msg)
                return {"ok": False, "error": error_msg}
                
        except Exception as e:
            error_msg = f"Erro ao enfileirar operação: {e}"
            logging.error(error_msg, exc_info=True)
            return {"ok": False, "error": error_msg}

    def _enqueue_sql(self, sql: str, params: tuple=(), wait: bool=False, timeout: float=5.0): return self._enqueue_operation({"sql": sql, "params": params, "wait_for_result": wait}, wait, timeout)
    def _enqueue_callable(self, func, wait: bool=False, timeout: float=5.0): return self._enqueue_operation({"callable": func, "wait_for_result": wait}, wait, timeout)

    def _execute_read_query(self, query, params=()):
        if not self._read_conn: return []
        try:
            with self._read_conn as conn: return [dict(row) for row in conn.cursor().execute(query, params).fetchall()]
        except sqlite3.Error as e: logging.error(f"Erro de leitura no DB: {e}"); return []

    def get_all_tasks(self) -> List[Dict[str, Any]]: return self._execute_read_query("SELECT * FROM tarefas_fila ORDER BY display_order, added_timestamp")
    def get_tasks_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids: return []
        return self._execute_read_query(f"SELECT * FROM tarefas_fila WHERE id IN ({','.join('?' for _ in ids)})", tuple(ids))
    def get_all_presets(self) -> List[Dict[str, Any]]:
        presets = self._execute_read_query("SELECT name, config FROM presets ORDER BY name")
        for p in presets:
            try: p['config'] = json.loads(p['config'])
            except (json.JSONDecodeError, TypeError): p['config'] = {}
        return presets

    def add_task(self, item_id: str, video_path: str, order: float, wait: bool = False):
        """ Adiciona uma nova tarefa à fila. """
        return self._enqueue_sql("INSERT INTO tarefas_fila (id, video_path, display_order, status) VALUES (?, ?, ?, ?)",
                                 (item_id, video_path, order, STATUS_QUEUED), wait=wait)

    def delete_tasks(self, item_ids: List[str], wait: bool = False):
        """ Deleta uma ou mais tarefas pelo ID. """
        if not item_ids: return {"ok": True}
        return self._enqueue_callable(lambda conn: conn.executemany("DELETE FROM tarefas_fila WHERE id = ?", [(i,) for i in item_ids]), wait=wait)

    def clear_finished_tasks(self, wait: bool = False):
        """ Remove tarefas que já foram concluídas ou resultaram em erro. """
        return self._enqueue_sql("DELETE FROM tarefas_fila WHERE status IN (?, ?, ?)",
                                 (STATUS_COMPLETED, STATUS_ERROR, STATUS_INTERRUPTED), wait=wait)

    def update_task_status(self, item_id: str, status: str, wait: bool=False): return self._enqueue_sql("UPDATE tarefas_fila SET status = ? WHERE id = ?", (status, item_id), wait=wait)
    def update_task_config(self, item_id: str, config: Dict[str, Any], wait: bool=False):
        if not config: return {"ok": True}
        fields = ', '.join([f"{k} = ?" for k in config.keys()]); values = list(config.values()) + [item_id]
        return self._enqueue_sql(f"UPDATE tarefas_fila SET {fields} WHERE id = ?", tuple(values), wait=wait)
    def update_task_order(self, task_id: str, new_order: float, wait: bool=False): return self.update_task_config(task_id, {'display_order': new_order}, wait)
    def save_preset(self, name: str, config: Dict[str, Any]): return self._enqueue_sql("INSERT OR REPLACE INTO presets (name, config) VALUES (?, ?)", (name, json.dumps(config)), wait=True)
    def delete_preset(self, name: str): return self._enqueue_sql("DELETE FROM presets WHERE name = ?", (name,), wait=True)

    def recover_interrupted_tasks(self, wait: bool=False):
        logging.info("Executando recuperação de tarefas interrompidas...")
        def _recover(conn):
            cur = conn.cursor()
            cur.execute("UPDATE tarefas_fila SET status = ? WHERE status LIKE ? OR status = ?", (STATUS_INTERRUPTED, f"%{STATUS_PROCESSING}%", STATUS_AWAIT_RENDER))
            return {"changed": cur.rowcount}
        res = self._enqueue_callable(_recover, wait=wait, timeout=10)
        if res.get("ok"):
            changed = res.get("result", {}).get("changed", 0)
            if changed > 0: logging.warning(f"{changed} tarefa(s) foram recuperadas.")
        else: logging.error(f"Falha ao recuperar tarefas: {res.get('error')}")

    def close(self):
        """
        Fecha o manager do DB de forma ordenada e thread-safe com proteções adicionais:
        1) Verifica estado de inicialização
        2) Sinaliza parada para thread de escrita
        3) Processa operações pendentes
        4) Aguarda thread encerrar com timeout
        5) Fecha conexões e limpa recursos
        """
        if not self._initialized:
            logging.info("DatabaseManager: close() chamado em instância não inicializada")
            return

        try:
            logging.info("DatabaseManager: iniciando processo de encerramento...")
            
            # Verifica se a thread existe e está ativa
            if self._writer_thread is None or not self._writer_thread.is_alive():
                logging.warning("DatabaseManager: thread de escrita não está ativa")
            else:
                # Sinaliza para thread parar
                self._writer_stop.set()
                
                # Envia sentinel com proteção contra deadlock
                try:
                    if not self._write_queue.full():
                        self._write_queue.put(None, timeout=5)
                except queue.Full:
                    logging.warning("DatabaseManager: fila cheia ao tentar enviar sinal de parada")
                except Exception as e:
                    logging.error(f"DatabaseManager: erro ao enviar sinal de parada: {e}")
                
                # Aguarda processamento das operações pendentes com timeout
                try:
                    if self._write_queue.join(timeout=10):
                        logging.info("DatabaseManager: todas operações pendentes processadas")
                    else:
                        logging.warning("DatabaseManager: timeout aguardando operações pendentes")
                except Exception as e:
                    logging.warning(f"DatabaseManager: erro ao aguardar fila: {e}", exc_info=True)
                
                # Aguarda thread encerrar com timeout
                try:
                    self._writer_thread.join(timeout=10)
                    if self._writer_thread.is_alive():
                        logging.warning("DatabaseManager: thread de escrita não encerrou no timeout")
                except Exception as e:
                    logging.error(f"DatabaseManager: erro ao aguardar thread: {e}", exc_info=True)
            
        except Exception as e:
            logging.error(f"DatabaseManager: erro durante close(): {e}", exc_info=True)
            
        finally:
            # Limpa recursos
            try:
                # Fecha conexão de leitura
                if self._read_conn:
                    try:
                        self._read_conn.close()
                        logging.info("DatabaseManager: conexão de leitura fechada")
                    except Exception as e:
                        logging.error(f"DatabaseManager: erro ao fechar conexão de leitura: {e}", exc_info=True)
                    finally:
                        self._read_conn = None
                
                # Limpa outras estruturas
                self._write_queue = queue.Queue(maxsize=1000)  # Nova fila limpa
                self._writer_thread = None
                self._initialized = False
                self.is_writer_healthy = False
                
                logging.info("DatabaseManager: limpeza de recursos concluída")
                
            except Exception as e:
                logging.error(f"DatabaseManager: erro durante limpeza final: {e}", exc_info=True)

