# -*- coding: utf-8 -*-

import sqlite3
import queue
import threading
import logging
import json
from tkinter import messagebox
from typing import List, Dict, Any

from utils.constants import *

class DatabaseManager:
    """ Gerenciador de banco de dados com thread de escrita dedicada e otimizações. """
    def __init__(self, db_path: str = "editor_sapiens.db"):
        self.db_path = db_path
        self._write_queue: "queue.Queue[dict]" = queue.Queue()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="DBWriterThread", daemon=True)
        self._writer_started = threading.Event()
        self.is_writer_healthy = False

        try:
            self._read_conn = sqlite3.connect(self.db_path, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._read_conn.row_factory = sqlite3.Row
            self._apply_performance_pragmas(self._read_conn)
        except sqlite3.Error as e:
            logging.critical(f"Falha CRÍTICA ao abrir conexão de leitura do DB: {e}", exc_info=True)
            messagebox.showerror("Erro de Banco de Dados", f"Não foi possível abrir o banco de dados. A aplicação pode não funcionar. Erro: {e}")
            self._read_conn = None

        if self._read_conn: self._initialize_db_schema()
        self._writer_thread.start()
        if not self._writer_started.wait(timeout=5): logging.error("Timeout: A thread de escrita do DB não iniciou.")
        else: self._health_check_writer()

    def _apply_performance_pragmas(self, conn):
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
                operation_mode TEXT DEFAULT 'full_pipe', added_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS presets (name TEXT PRIMARY KEY, config TEXT NOT NULL)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS log_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, level TEXT, message TEXT)''')

    def _writer_loop(self):
        try:
            write_conn = sqlite3.connect(self.db_path, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._apply_performance_pragmas(write_conn)
            self._writer_started.set()
        except Exception as e: logging.critical(f"Falha ao iniciar a conexão de escrita do DB: {e}", exc_info=True); self._writer_started.set(); return

        while True:
            op: dict = self._write_queue.get()
            if op is None: break
            try:
                with write_conn:
                    if op.get("callable"): result = op["callable"](write_conn)
                    else:
                        cur = write_conn.cursor(); cur.execute(op["sql"], op.get("params", ())); result = {"rowcount": cur.rowcount}
                    if op.get("wait_for_result") and "response_q" in op: op["response_q"].put({"ok": True, "result": result})
            except Exception as e:
                logging.error(f"Erro na operação de escrita do DB: {e}", exc_info=True)
                if op.get("wait_for_result") and "response_q" in op: op["response_q"].put({"ok": False, "error": str(e)})
            finally: self._write_queue.task_done()
        write_conn.close()

    def _enqueue_operation(self, op: dict, wait: bool, timeout: float):
        if wait: op["response_q"] = queue.Queue(maxsize=1)
        self._write_queue.put(op)
        if wait:
            try: return op["response_q"].get(timeout=timeout)
            except queue.Empty: return {"ok": False, "error": f"Timeout ({timeout}s) na operação do DB"}
        return {"ok": True}

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
    Fecha o manager do DB de forma ordenada:
    1) coloca o sentinel (None) para sinalizar parada ao writer thread;
    2) aguarda a fila ser processada (join);
    3) aguarda a thread encerrar;
    4) fecha a conexão de leitura.
    """
    try:
        logging.info("DatabaseManager: sinalizando thread de escrita para encerrar...")
        # sinaliza para encerrar após processar tudo que já está na fila
        self._write_queue.put(None)

        # aguarda a fila ser totalmente processada (task_done em writer loop)
        try:
            self._write_queue.join()
        except Exception as e:
            logging.warning(f"DatabaseManager: erro ao aguardar join da fila: {e}")

        # aguarda o término da thread de escrita
        self._writer_thread.join(timeout=10)
    except Exception as e:
        logging.warning(f"DatabaseManager: erro durante close(): {e}", exc_info=True)
    finally:
        if self._read_conn:
            try:
                self._read_conn.close()
                logging.info("DatabaseManager: conexão de leitura fechada.")
            except Exception as e:
                logging.warning(f"DatabaseManager: falha ao fechar conexão de leitura: {e}")

