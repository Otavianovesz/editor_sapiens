# -*- coding: utf-8 -*-

import sqlite3
import queue
import threading
import logging
import json
from tkinter import messagebox
from typing import List, Dict, Any, Optional, Callable

from utils.constants import *

# A versão atual do esquema do banco de dados. Incremente este número a cada alteração estrutural.
DB_SCHEMA_VERSION = 2

class DatabaseManager:
    """
    Gerenciador de banco de dados robusto com thread de escrita dedicada, otimizações
    de performance e um sistema de migração de esquema para garantir a compatibilidade
    e evitar erros como 'no such column'.
    """
    def __init__(self, db_path: str = "editor_sapiens.db"):
        self.db_path = db_path
        self._write_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="DBWriterThread", daemon=True)
        self._writer_started = threading.Event()
        self.is_writer_healthy = False
        self._read_conn: Optional[sqlite3.Connection] = None

        try:
            # A conexão de leitura é otimizada para velocidade e concorrência
            self._read_conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._read_conn.row_factory = sqlite3.Row
        except sqlite3.OperationalError:
            # Ocorre se o DB não existir e tentarmos abrir em modo 'ro' (read-only)
            # A thread de escrita cuidará da criação.
            pass
        except sqlite3.Error as e:
            logging.critical(f"Falha CRÍTICA ao abrir conexão de leitura do DB: {e}", exc_info=True)
            messagebox.showerror("Erro de Banco de Dados", f"Não foi possível abrir o banco de dados. A aplicação pode não funcionar. Erro: {e}")

        # A thread de escrita é iniciada independentemente da conexão de leitura
        self._writer_thread.start()
        if not self._writer_started.wait(timeout=5):
            logging.error("Timeout: A thread de escrita do DB não iniciou a tempo.")
        else:
            self._run_migrations_and_health_check()

    def _apply_performance_pragmas(self, conn: sqlite3.Connection):
        """Aplica configurações PRAGMA para otimizar performance e segurança."""
        conn.execute("PRAGMA journal_mode = WAL;")      # Essencial para concorrência e evitar bloqueios
        conn.execute("PRAGMA synchronous = NORMAL;")    # Equilíbrio seguro entre velocidade e durabilidade
        conn.execute("PRAGMA cache_size = -32768;")     # Aloca 32MB de cache
        conn.execute("PRAGMA temp_store = MEMORY;")     # Operações temporárias em memória
        conn.execute("PRAGMA busy_timeout = 5000;")     # Espera 5s se o DB estiver ocupado

    def _run_migrations_and_health_check(self):
        """Executa as migrações de esquema e verifica a saúde da thread de escrita."""
        def migration_and_check(conn: sqlite3.Connection):
            self._initialize_db_schema(conn)
            self._migrate_schema(conn)
            return conn.cursor().execute("SELECT 1").fetchone()

        res = self._enqueue_callable(migration_and_check, wait=True, timeout=5)
        if res.get("ok") and res.get("result"):
            self.is_writer_healthy = True
            logging.info("Health check e migração do DB: SUCESSO.")
        else:
            self.is_writer_healthy = False
            logging.error(f"Health check e migração do DB: FALHA. Erro: {res.get('error')}")

    def _initialize_db_schema(self, conn: sqlite3.Connection):
        """Cria as tabelas iniciais se elas não existirem."""
        conn.execute("CREATE TABLE IF NOT EXISTS db_version (version INTEGER PRIMARY KEY);")
        conn.execute('''CREATE TABLE IF NOT EXISTS tarefas_fila (
            id TEXT PRIMARY KEY,
            video_path TEXT NOT NULL,
            status TEXT DEFAULT 'Aguardando',
            -- CORREÇÃO: Análise visual agora é DESATIVADA por padrão para novas tarefas.
            use_visual_analysis INTEGER DEFAULT 0,
            transcription_mode TEXT DEFAULT 'whisper',
            transcription_path TEXT,
            render_script_path TEXT,
            display_order REAL DEFAULT 0.0,
            operation_mode TEXT DEFAULT 'full_pipe',
            added_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS presets (
            name TEXT PRIMARY KEY,
            config TEXT NOT NULL
            )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS log_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            level TEXT,
            message TEXT
            )''')
        conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection):
        """
        Sistema de migração. Adiciona novas colunas ou faz alterações em
        bancos de dados existentes de forma segura.
        """
        cursor = conn.cursor()
        cursor.execute("SELECT version FROM db_version")
        current_version = (cursor.fetchone() or [0])[0]

        if current_version < DB_SCHEMA_VERSION:
            logging.info(f"Atualizando esquema do DB da versão {current_version} para {DB_SCHEMA_VERSION}...")

            # --- Migração para Versão 2 ---
            if current_version < 2:
                try:
                    # Adiciona a coluna 'render_metadata' que causava o erro
                    cursor.execute("ALTER TABLE tarefas_fila ADD COLUMN render_metadata TEXT;")
                    logging.info("Coluna 'render_metadata' adicionada à tabela 'tarefas_fila'.")
                except sqlite3.OperationalError as e:
                    # Ignora o erro se a coluna já existir, por segurança
                    if "duplicate column name" not in str(e): raise

            # Adicionar futuras migrações aqui em blocos "if current_version < X:"

            # Atualiza a versão no DB
            cursor.execute("INSERT OR REPLACE INTO db_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
            conn.commit()
            logging.info("Esquema do DB atualizado com sucesso.")

    def _writer_loop(self):
        """Loop principal da thread de escrita, processa operações da fila."""
        try:
            write_conn = sqlite3.connect(self.db_path, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._apply_performance_pragmas(write_conn)
            self._writer_started.set()
        except Exception as e:
            logging.critical(f"Falha ao iniciar a conexão de escrita do DB: {e}", exc_info=True)
            self._writer_started.set()
            return

        while True:
            op: Optional[Dict[str, Any]] = self._write_queue.get()
            if op is None: break # Sinal para terminar a thread

            try:
                with write_conn:
                    if "callable" in op:
                        result = op["callable"](write_conn)
                    else:
                        cur = write_conn.cursor()
                        cur.execute(op["sql"], op.get("params", ()))
                        result = {"rowcount": cur.rowcount}

                    if op.get("wait_for_result") and "response_q" in op:
                        op["response_q"].put({"ok": True, "result": result})

            except Exception as e:
                logging.error(f"Erro na operação de escrita do DB: {e}", exc_info=True)
                if op.get("wait_for_result") and "response_q" in op:
                    op["response_q"].put({"ok": False, "error": str(e)})
            finally:
                self._write_queue.task_done()
        write_conn.close()

    def _enqueue_operation(self, op: Dict[str, Any], wait: bool, timeout: float) -> Dict[str, Any]:
        """Adiciona uma operação à fila de escrita e opcionalmente aguarda o resultado."""
        if wait: op["response_q"] = queue.Queue(maxsize=1)
        self._write_queue.put(op)
        if wait:
            try:
                return op["response_q"].get(timeout=timeout)
            except queue.Empty:
                return {"ok": False, "error": f"Timeout ({timeout}s) na operação do DB"}
        return {"ok": True}

    def _enqueue_sql(self, sql: str, params: tuple=(), wait: bool=False, timeout: float=5.0):
        return self._enqueue_operation({"sql": sql, "params": params, "wait_for_result": wait}, wait, timeout)

    def _enqueue_callable(self, func: Callable, wait: bool=False, timeout: float=5.0):
        return self._enqueue_operation({"callable": func, "wait_for_result": wait}, wait, timeout)

    def _execute_read_query(self, query: str, params: tuple=()) -> List[Dict[str, Any]]:
        """Executa uma query de leitura (SELECT) de forma segura."""
        if not self._read_conn: return []
        try:
            # A conexão de leitura usa 'with' para garantir o fechamento correto do cursor
            with self._read_conn:
                return [dict(row) for row in self._read_conn.cursor().execute(query, params).fetchall()]
        except sqlite3.Error as e:
            logging.error(f"Erro de leitura no DB: {e}"); return []

    # --- MÉTODOS PÚBLICOS DA API ---

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        return self._execute_read_query("SELECT * FROM tarefas_fila ORDER BY display_order, added_timestamp")

    def get_tasks_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids: return []
        placeholders = ','.join('?' for _ in ids)
        return self._execute_read_query(f"SELECT * FROM tarefas_fila WHERE id IN ({placeholders})", tuple(ids))

    def get_all_presets(self) -> List[Dict[str, Any]]:
        presets = self._execute_read_query("SELECT name, config FROM presets ORDER BY name")
        for p in presets:
            try:
                p['config'] = json.loads(p['config'])
            except (json.JSONDecodeError, TypeError):
                p['config'] = {}
        return presets

    def add_task(self, item_id: str, video_path: str, order: float, wait: bool = False):
        return self._enqueue_sql("INSERT INTO tarefas_fila (id, video_path, display_order, status) VALUES (?, ?, ?, ?)",
                                 (item_id, video_path, order, STATUS_QUEUED), wait=wait)

    def delete_tasks(self, item_ids: List[str], wait: bool = False):
        if not item_ids: return {"ok": True}
        return self._enqueue_callable(lambda conn: conn.executemany("DELETE FROM tarefas_fila WHERE id = ?", [(i,) for i in item_ids]), wait=wait)

    def clear_finished_tasks(self, wait: bool = False):
        return self._enqueue_sql("DELETE FROM tarefas_fila WHERE status IN (?, ?, ?)",
                                 (STATUS_COMPLETED, STATUS_ERROR, STATUS_INTERRUPTED), wait=wait)

    def update_task_status(self, item_id: str, status: str, wait: bool=False):
        return self._enqueue_sql("UPDATE tarefas_fila SET status = ? WHERE id = ?", (status, item_id), wait=wait)

    def update_task_config(self, item_id: str, config: Dict[str, Any], wait: bool=False):
        if not config: return {"ok": True}
        fields = ', '.join([f"{k} = ?" for k in config.keys()])
        values = tuple(config.values()) + (item_id,)
        return self._enqueue_sql(f"UPDATE tarefas_fila SET {fields} WHERE id = ?", values, wait=wait)

    def update_task_order(self, task_id: str, new_order: float, wait: bool=False):
        return self.update_task_config(task_id, {'display_order': new_order}, wait=wait)

    def save_preset(self, name: str, config: Dict[str, Any]):
        return self._enqueue_sql("INSERT OR REPLACE INTO presets (name, config) VALUES (?, ?)", (name, json.dumps(config)), wait=True)

    def delete_preset(self, name: str):
        return self._enqueue_sql("DELETE FROM presets WHERE name = ?", (name,), wait=True)

    def recover_interrupted_tasks(self, wait: bool=False):
        logging.info("Executando recuperação de tarefas interrompidas...")
        def _recover(conn: sqlite3.Connection):
            cur = conn.cursor()
            cur.execute("UPDATE tarefas_fila SET status = ? WHERE status LIKE ? OR status = ?",
                        (STATUS_INTERRUPTED, f"%{STATUS_PROCESSING}%", STATUS_AWAIT_RENDER))
            return {"changed": cur.rowcount}

        res = self._enqueue_callable(_recover, wait=wait, timeout=10)
        if res.get("ok"):
            changed = res.get("result", {}).get("changed", 0)
            if changed > 0:
                logging.warning(f"{changed} tarefa(s) em processamento foram recuperadas e marcadas como 'Interrompido'.")
        else:
            logging.error(f"Falha ao recuperar tarefas interrompidas: {res.get('error')}")

    def close(self):
        """Encerra a thread de escrita e fecha as conexões com o banco de dados."""
        logging.info("Fechando gerenciador de banco de dados...")
        self._write_queue.put(None)
        self._writer_thread.join(timeout=3)
        if self._writer_thread.is_alive():
            logging.warning("A thread de escrita do DB não encerrou a tempo.")
        if self._read_conn:
            self._read_conn.close()
        logging.info("Gerenciador de banco de dados fechado.")