# -*- coding: utf-8 -*-

import sqlite3
import queue
import threading
import logging
import json
from tkinter import messagebox
from typing import List, Dict, Any, Optional, Callable

from utils.constants import *

# The current version of the database schema. Increment this number with each structural change.
DB_SCHEMA_VERSION = 2

class DatabaseManager:
    """Robust database manager with a dedicated writer thread, performance optimizations,
    and a schema migration system to ensure compatibility and prevent errors like 'no such column'.
    """
    def __init__(self, db_path: str = "editor_sapiens.db"):
        """Initializes the DatabaseManager.

        Args:
            db_path (str, optional): The path to the database file.
                Defaults to "editor_sapiens.db".
        """
        self.db_path = db_path
        self._write_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="DBWriterThread", daemon=True)
        self._writer_started = threading.Event()
        self.is_writer_healthy = False
        self._read_conn: Optional[sqlite3.Connection] = None

        try:
            # The read connection is optimized for speed and concurrency
            self._read_conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._read_conn.row_factory = sqlite3.Row
        except sqlite3.OperationalError:
            # Occurs if the DB does not exist and we try to open it in 'ro' (read-only) mode.
            # The writer thread will handle the creation.
            pass
        except sqlite3.Error as e:
            logging.critical(f"CRITICAL failure when opening the DB read connection: {e}", exc_info=True)
            messagebox.showerror("Database Error", f"Could not open the database. The application may not work. Error: {e}")

        # The writer thread is started regardless of the read connection.
        self._writer_thread.start()
        if not self._writer_started.wait(timeout=5):
            logging.error("Timeout: The DB writer thread did not start in time.")
        else:
            self._run_migrations_and_health_check()

    def _apply_performance_pragmas(self, conn: sqlite3.Connection):
        """Applies PRAGMA settings to optimize performance and security.

        Args:
            conn (sqlite3.Connection): The database connection.
        """
        conn.execute("PRAGMA journal_mode = WAL;")      # Essential for concurrency and avoiding locks
        conn.execute("PRAGMA synchronous = NORMAL;")    # Safe balance between speed and durability
        conn.execute("PRAGMA cache_size = -32768;")     # Allocates 32MB of cache
        conn.execute("PRAGMA temp_store = MEMORY;")     # Temporary operations in memory
        conn.execute("PRAGMA busy_timeout = 5000;")     # Waits 5s if the DB is busy

    def _run_migrations_and_health_check(self):
        """Executes schema migrations and checks the health of the writer thread."""
        def migration_and_check(conn: sqlite3.Connection):
            self._initialize_db_schema(conn)
            self._migrate_schema(conn)
            return conn.cursor().execute("SELECT 1").fetchone()

        res = self._enqueue_callable(migration_and_check, wait=True, timeout=5)
        if res.get("ok") and res.get("result"):
            self.is_writer_healthy = True
            logging.info("DB health check and migration: SUCCESS.")
        else:
            self.is_writer_healthy = False
            logging.error(f"DB health check and migration: FAILED. Error: {res.get('error')}")

    def _initialize_db_schema(self, conn: sqlite3.Connection):
        """Creates the initial tables if they don't exist.

        Args:
            conn (sqlite3.Connection): The database connection.
        """
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
        """Migration system. Adds new columns or makes changes to
        existing databases safely.

        Args:
            conn (sqlite3.Connection): The database connection.
        """
        cursor = conn.cursor()
        cursor.execute("SELECT version FROM db_version")
        current_version = (cursor.fetchone() or [0])[0]

        if current_version < DB_SCHEMA_VERSION:
            logging.info(f"Updating DB schema from version {current_version} to {DB_SCHEMA_VERSION}...")

            # --- Migration to Version 2 ---
            if current_version < 2:
                try:
                    # Adds the 'render_metadata' column that was causing the error
                    cursor.execute("ALTER TABLE tarefas_fila ADD COLUMN render_metadata TEXT;")
                    logging.info("Column 'render_metadata' added to table 'tarefas_fila'.")
                except sqlite3.OperationalError as e:
                    # Ignores the error if the column already exists, for safety
                    if "duplicate column name" not in str(e): raise

            # Add future migrations here in "if current_version < X:" blocks

            # Updates the version in the DB
            cursor.execute("INSERT OR REPLACE INTO db_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
            conn.commit()
            logging.info("DB schema updated successfully.")

    def _writer_loop(self):
        """Main loop of the writer thread, processes operations from the queue."""
        try:
            write_conn = sqlite3.connect(self.db_path, check_same_thread=True, detect_types=sqlite3.PARSE_DECLTYPES)
            self._apply_performance_pragmas(write_conn)
            self._writer_started.set()
        except Exception as e:
            logging.critical(f"Failed to start the DB write connection: {e}", exc_info=True)
            self._writer_started.set()
            return

        while True:
            op: Optional[Dict[str, Any]] = self._write_queue.get()
            if op is None: break # Signal to end the thread

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
                logging.error(f"Error in DB write operation: {e}", exc_info=True)
                if op.get("wait_for_result") and "response_q" in op:
                    op["response_q"].put({"ok": False, "error": str(e)})
            finally:
                self._write_queue.task_done()
        write_conn.close()

    def _enqueue_operation(self, op: Dict[str, Any], wait: bool, timeout: float) -> Dict[str, Any]:
        """Adds an operation to the writer queue and optionally waits for the result.

        Args:
            op (Dict[str, Any]): The operation to enqueue.
            wait (bool): Whether to wait for the result.
            timeout (float): The timeout in seconds.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        if wait: op["response_q"] = queue.Queue(maxsize=1)
        self._write_queue.put(op)
        if wait:
            try:
                return op["response_q"].get(timeout=timeout)
            except queue.Empty:
                return {"ok": False, "error": f"Timeout ({timeout}s) in DB operation"}
        return {"ok": True}

    def _enqueue_sql(self, sql: str, params: tuple=(), wait: bool=False, timeout: float=5.0):
        """Enqueues an SQL operation.

        Args:
            sql (str): The SQL query.
            params (tuple, optional): The parameters for the query. Defaults to ().
            wait (bool, optional): Whether to wait for the result. Defaults to False.
            timeout (float, optional): The timeout in seconds. Defaults to 5.0.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_operation({"sql": sql, "params": params, "wait_for_result": wait}, wait, timeout)

    def _enqueue_callable(self, func: Callable, wait: bool=False, timeout: float=5.0):
        """Enqueues a callable operation.

        Args:
            func (Callable): The function to execute.
            wait (bool, optional): Whether to wait for the result. Defaults to False.
            timeout (float, optional): The timeout in seconds. Defaults to 5.0.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_operation({"callable": func, "wait_for_result": wait}, wait, timeout)

    def _execute_read_query(self, query: str, params: tuple=()) -> List[Dict[str, Any]]:
        """Executes a read query (SELECT) safely.

        Args:
            query (str): The SQL query.
            params (tuple, optional): The parameters for the query. Defaults to ().

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the rows.
        """
        if not self._read_conn: return []
        try:
            # The read connection uses 'with' to ensure the cursor is closed correctly.
            with self._read_conn:
                return [dict(row) for row in self._read_conn.cursor().execute(query, params).fetchall()]
        except sqlite3.Error as e:
            logging.error(f"DB read error: {e}"); return []

    # --- PUBLIC API METHODS ---

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Gets all tasks from the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the tasks.
        """
        return self._execute_read_query("SELECT * FROM tarefas_fila ORDER BY display_order, added_timestamp")

    def get_tasks_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        """Gets tasks by their IDs.

        Args:
            ids (List[str]): A list of task IDs.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the tasks.
        """
        if not ids: return []
        placeholders = ','.join('?' for _ in ids)
        return self._execute_read_query(f"SELECT * FROM tarefas_fila WHERE id IN ({placeholders})", tuple(ids))

    def get_all_presets(self) -> List[Dict[str, Any]]:
        """Gets all presets from the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the presets.
        """
        presets = self._execute_read_query("SELECT name, config FROM presets ORDER BY name")
        for p in presets:
            try:
                p['config'] = json.loads(p['config'])
            except (json.JSONDecodeError, TypeError):
                p['config'] = {}
        return presets

    def add_task(self, item_id: str, video_path: str, order: float, wait: bool = False):
        """Adds a new task to the database.

        Args:
            item_id (str): The ID of the task.
            video_path (str): The path to the video file.
            order (float): The display order of the task.
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_sql("INSERT INTO tarefas_fila (id, video_path, display_order, status) VALUES (?, ?, ?, ?)",
                                 (item_id, video_path, order, STATUS_QUEUED), wait=wait)

    def delete_tasks(self, item_ids: List[str], wait: bool = False):
        """Deletes tasks from the database.

        Args:
            item_ids (List[str]): A list of task IDs to delete.
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        if not item_ids: return {"ok": True}
        return self._enqueue_callable(lambda conn: conn.executemany("DELETE FROM tarefas_fila WHERE id = ?", [(i,) for i in item_ids]), wait=wait)

    def clear_finished_tasks(self, wait: bool = False):
        """Clears all finished, errored, or interrupted tasks from the database.

        Args:
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_sql("DELETE FROM tarefas_fila WHERE status IN (?, ?, ?)",
                                 (STATUS_COMPLETED, STATUS_ERROR, STATUS_INTERRUPTED), wait=wait)

    def update_task_status(self, item_id: str, status: str, wait: bool=False):
        """Updates the status of a task.

        Args:
            item_id (str): The ID of the task to update.
            status (str): The new status.
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_sql("UPDATE tarefas_fila SET status = ? WHERE id = ?", (status, item_id), wait=wait)

    def update_task_config(self, item_id: str, config: Dict[str, Any], wait: bool=False):
        """Updates the configuration of a task.

        Args:
            item_id (str): The ID of the task to update.
            config (Dict[str, Any]): A dictionary with the configuration to update.
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        if not config: return {"ok": True}
        fields = ', '.join([f"{k} = ?" for k in config.keys()])
        values = tuple(config.values()) + (item_id,)
        return self._enqueue_sql(f"UPDATE tarefas_fila SET {fields} WHERE id = ?", values, wait=wait)

    def update_task_order(self, task_id: str, new_order: float, wait: bool=False):
        """Updates the display order of a task.

        Args:
            task_id (str): The ID of the task to update.
            new_order (float): The new display order.
            wait (bool, optional): Whether to wait for the result. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self.update_task_config(task_id, {'display_order': new_order}, wait=wait)

    def save_preset(self, name: str, config: Dict[str, Any]):
        """Saves a preset to the database.

        Args:
            name (str): The name of the preset.
            config (Dict[str, Any]): The configuration of the preset.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_sql("INSERT OR REPLACE INTO presets (name, config) VALUES (?, ?)", (name, json.dumps(config)), wait=True)

    def delete_preset(self, name: str):
        """Deletes a preset from the database.

        Args:
            name (str): The name of the preset to delete.

        Returns:
            Dict[str, Any]: A dictionary with the result of the operation.
        """
        return self._enqueue_sql("DELETE FROM presets WHERE name = ?", (name,), wait=True)

    def recover_interrupted_tasks(self, wait: bool=False):
        """Recovers interrupted tasks, marking them as 'Interrupted'."""
        logging.info("Executing recovery of interrupted tasks...")
        def _recover(conn: sqlite3.Connection):
            cur = conn.cursor()
            cur.execute("UPDATE tarefas_fila SET status = ? WHERE status LIKE ? OR status = ?",
                        (STATUS_INTERRUPTED, f"%{STATUS_PROCESSING}%", STATUS_AWAIT_RENDER))
            return {"changed": cur.rowcount}

        res = self._enqueue_callable(_recover, wait=wait, timeout=10)
        if res.get("ok"):
            changed = res.get("result", {}).get("changed", 0)
            if changed > 0:
                logging.warning(f"{changed} task(s) in progress were recovered and marked as 'Interrupted'.")
        else:
            logging.error(f"Failed to recover interrupted tasks: {res.get('error')}")

    def close(self):
        """Ends the writer thread and closes the database connections."""
        logging.info("Closing database manager...")
        self._write_queue.put(None)
        self._writer_thread.join(timeout=3)
        if self._writer_thread.is_alive():
            logging.warning("The DB writer thread did not close in time.")
        if self._read_conn:
            self._read_conn.close()
        logging.info("Database manager closed.")
