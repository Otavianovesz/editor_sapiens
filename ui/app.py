# -*- coding: utf-8 -*-

import os
import sys
import uuid
import queue
import threading
import datetime
from typing import Dict, Any, Optional, List
import json
import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk, Menu
import traceback
import logging
from threading import Lock

# Importa constantes globais
from utils.constants import *

# Importa módulos do projeto
from core.database import DatabaseManager
from core.config import Config
from core.orchestrator import Orchestrator
from utils.logger import Logger
from utils.constants import *
from .presets_window import PresetsManager
from .settings_window import AdvancedSettings

# Fallback para CTkToolTip para garantir compatibilidade com versões antigas de CTk
if not hasattr(ctk, "CTkToolTip"):
    class _DummyToolTip:
        def __init__(self, widget, text="", message=None):
            self.widget = widget
            self.text = message if message is not None else text
            try:
                widget.bind("<Enter>", self._on_enter)
                widget.bind("<Leave>", self._on_leave)
            except Exception: pass
        def _on_enter(self, e):
            try:
                root = self.widget.winfo_toplevel()
                if hasattr(root, "set_status_text"): root.set_status_text(self.text)
            except Exception: pass
        def _on_leave(self, e):
            try:
                root = self.widget.winfo_toplevel()
                if hasattr(root, "set_status_text"): root.set_status_text("")
            except Exception: pass
    ctk.CTkToolTip = _DummyToolTip

class App(ctk.CTk):
    """Classe principal da aplicação, responsável por toda a interface gráfica."""
    def __init__(self, app_context=None):
        super().__init__()
        self.app_context = app_context
        self.title("Editor Sapiens (Arconte)")
        self.geometry("1600x900")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.log_queue, self.progress_queue = queue.Queue(), queue.Queue()
        self.db = DatabaseManager()
        self.logger = Logger(self.log_queue, self.db)
        self.config = Config()
        self.orchestrator = Orchestrator(self.logger, self.config)
        
        # Registrar recursos no app_context se disponível
        if self.app_context:
            self.app_context.register_processor(self.orchestrator)
            self.app_context.register_processor(self.db)
        def _thread_excepthook(args):
            # args é threading.ExceptHookArgs: (exc_type, exc_value, exc_traceback, thread)
            try:
                # Log primário no arquivo de log
                logging.critical(f"Unhandled exception in thread {args.thread.name}: {args.exc_value}", 
                               exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
                
                # Extrai o traceback completo
                tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
                
                # Tenta enviar para o logger de UI e progress_queue
                if hasattr(self, "logger"):
                    self.logger.log(f"Erro crítico em thread {args.thread.name}: {args.exc_value}", "CRITICAL")
                    self.logger.log(f"Traceback completo:\n{tb}", "DEBUG")
                    
                    # Notifica a UI sobre o erro através da progress_queue
                    try:
                        if hasattr(self, "progress_queue"):
                            self.progress_queue.put({
                                'type': 'error',
                                'message': f"Erro crítico em thread: {args.exc_value}",
                                'task_id': getattr(args.thread, 'task_id', 'unknown')
                            })
                    except Exception:
                        pass
                
                # Força uma atualização da UI para mostrar o erro
                if hasattr(self, "after"):
                    self.after(100, lambda: messagebox.showerror("Erro Crítico", 
                             f"Ocorreu um erro crítico em uma thread:\n\n{args.exc_value}"))
                
            except Exception as e:
                logging.exception("Falha ao processar exceção de thread")
        threading.excepthook = _thread_excepthook

        # Também reforçamos o excepthook global (main thread)
        def _main_excepthook(exc_type, exc_value, exc_traceback):
            logging.critical(f"Unhandled exception (main thread): {exc_value}", exc_info=(exc_type, exc_value, exc_traceback))
            try:
                tb = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                if hasattr(self, "logger"):
                    self.logger.log(f"Unhandled exception (main thread): {exc_value}", "CRITICAL")
                    self.logger.log(tb, "DEBUG")
            except Exception:
                logging.exception("Falha ao enviar exceção principal para self.logger.")
        sys.excepthook = _main_excepthook
        self.is_running_task = False
        self.stop_event = threading.Event()
        self.current_task_id = None

        self._create_widgets()
        self.after(100, self._post_init_startup)
        self.after(200, self._process_queues)
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    def set_status_text(self, text: str):
        self.status_var.set(text)

    def _post_init_startup(self):
        """Tarefas a serem executadas após a inicialização da UI."""
        if not self.db.is_writer_healthy:
            self.logger.log("Writer DB não respondeu. Operando em modo limitado.", "ERROR")
        else:
            self.logger.log("Writer DB pronto.", "INFO")
        self.db.recover_interrupted_tasks(wait=True)
        self._load_and_display_queue()

    def _on_closing(self, force: bool = False):
        """Manipula o fechamento da aplicação de forma segura."""
        try:
            import traceback as _tb
            stack = "".join(_tb.format_stack())
            self.logger.log("Fechando a aplicação... (invocado _on_closing)", "INFO")
            self.logger.log("Stack do _on_closing:\n" + stack, "DEBUG")
        except Exception:
            logging.exception("Falha ao registrar stack no _on_closing.")

        # Se há tarefa em execução e não for forçado, confirmar com o usuário
        if self.is_running_task and not force:
            try:
                answer = messagebox.askyesno("Sair", "Tarefa em andamento. Deseja realmente sair?")
            except Exception:
                self.logger.log("Falha ao mostrar diálogo de confirmação; cancelando fechamento.", "WARNING")
                return

            if not answer:
                self.logger.log("Fechamento cancelado pelo usuário (tarefa em andamento).", "INFO")
                return

        # Inicia processo de finalização segura
        try:
            # 1. Sinaliza parada global
            self.stop_event.set()
            
            # 2. Interrompe tarefas ativas
            if self.is_running_task:
                try:
                    self.orchestrator.interrupt_current_task()
                    # Aguarda um pouco para a thread terminar
                    import time
                    time.sleep(0.5)
                except Exception as e:
                    self.logger.log(f"Erro ao interromper tarefa: {e}", "ERROR")

            # 3. Limpa filas
            try:
                while True:
                    try:
                        self.progress_queue.get_nowait()
                    except queue.Empty:
                        break
                while True:
                    try:
                        self.log_queue.get_nowait()
                    except queue.Empty:
                        break
            except Exception as e:
                self.logger.log(f"Erro ao limpar filas: {e}", "WARNING")

            # 4. Finaliza o banco de dados
            try:
                if hasattr(self.db, "_write_queue"):
                    # Sinaliza para a thread de escrita parar
                    self.db._write_queue.put(None)
                    # Aguarda thread terminar (com timeout)
                    if hasattr(self.db, "_writer_thread"):
                        self.db._writer_thread.join(timeout=2.0)
                self.db.close()
            except Exception as e:
                self.logger.log(f"Erro ao finalizar banco de dados: {e}", "ERROR")

            # 5. Cancela callbacks pendentes
            try:
                pending = self.tk.call('after', 'info')
                for event_id in pending:
                    try:
                        self.after_cancel(event_id)
                    except Exception:
                        pass
            except Exception as e:
                self.logger.log(f"Erro ao cancelar callbacks: {e}", "WARNING")

            # 6. Limpa recursos via app_context se disponível
            try:
                if self.app_context:
                    self.app_context.cleanup()
                # Limpa referências cíclicas
                self.orchestrator = None
                self.logger = None
                self.db = None
            except Exception as e:
                self.logger.log(f"Erro ao limpar recursos: {e}", "ERROR")

            # 7. Destrói a janela
            try:
                # Força GC antes de destruir
                import gc
                gc.collect()
                self.quit()
                self.destroy()
            except Exception as e:
                self.logger.log(f"Erro ao destruir janela: {e}", "ERROR")
                # Força encerramento se destroy() falhar
                import os, signal
                os._exit(0)

        except Exception:
            logging.exception("Erro fatal no processo de fechamento.")
            # Força encerramento em último caso
            import os, signal
            os._exit(1)

    def _create_widgets(self):
        """Cria e organiza todos os widgets da interface principal."""
        self.grid_columnconfigure(0, weight=5); self.grid_columnconfigure(1, weight=3); self.grid_rowconfigure(0, weight=1)
        self._create_menubar()
        self._create_center_panel()
        self._create_right_panel()
        self._create_bottom_panel()

    def _create_menubar(self):
        self.menubar = Menu(self); self.configure(menu=self.menubar)
        file_menu = Menu(self.menubar, tearoff=0); file_menu.add_command(label="Adicionar Tarefas...", command=self._add_tasks); file_menu.add_separator(); file_menu.add_command(label="Sair", command=self._on_closing); self.menubar.add_cascade(label="Arquivo", menu=file_menu)
        edit_menu = Menu(self.menubar, tearoff=0); edit_menu.add_command(label="Remover Selecionados", command=self._remove_tasks); edit_menu.add_command(label="Limpar Concluídas/Erradas", command=self._clear_tasks); self.menubar.add_cascade(label="Editar", menu=edit_menu)
        process_menu = Menu(self.menubar, tearoff=0); process_menu.add_command(label="Iniciar Fila", command=self._start_queue); process_menu.add_command(label="Parar Fila", command=self._stop_queue); self.menubar.add_cascade(label="Processamento", menu=process_menu)
        tools_menu = Menu(self.menubar, tearoff=0); tools_menu.add_command(label="Gerenciador de Presets...", command=self._open_presets_manager); tools_menu.add_command(label="Configurações Avançadas...", command=self._open_advanced_settings); self.menubar.add_cascade(label="Ferramentas", menu=tools_menu)
    
    def _create_center_panel(self):
        center_panel = ctk.CTkFrame(self); center_panel.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        center_panel.grid_rowconfigure(1, weight=1); center_panel.grid_columnconfigure(0, weight=1)
        toolbar = ctk.CTkFrame(center_panel); toolbar.grid(row=0, column=0, padx=10, pady=(10,5), sticky="ew")
        ctk.CTkButton(toolbar, text="➕ Adicionar", command=self._add_tasks).pack(side="left", padx=5)
        ctk.CTkButton(toolbar, text="➖ Remover", command=self._remove_tasks).pack(side="left", padx=5)
        self.start_button = ctk.CTkButton(toolbar, text="▶️ Iniciar Fila", command=self._start_queue); self.start_button.pack(side="left", padx=5)
        self.stop_button = ctk.CTkButton(toolbar, text="⏹️ Parar", command=self._stop_queue, state="disabled"); self.stop_button.pack(side="left", padx=5)
        ctk.CTkButton(toolbar, text="🔄 Atualizar", command=self._load_and_display_queue, width=100).pack(side="right", padx=5)
        style = ttk.Style(self); style.theme_use("default"); style.configure("Treeview", background="#2b2b2b", foreground="white", fieldbackground="#2b2b2b", rowheight=32)
        style.map('Treeview', background=[('selected', '#3a7ebf')]); style.configure("Treeview.Heading", background="#565b5e", foreground="white", font=('CTkFont', 12, 'bold'))
        self.tree = ttk.Treeview(center_panel, columns=("Status", "Arquivo", "Modo", "👁️", "Progresso"), show="headings")
        self.tree.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="nsew")
        self.tree.heading("Status", text="Status"); self.tree.column("Status", width=150, anchor="w")
        self.tree.heading("Arquivo", text="Nome do Arquivo"); self.tree.column("Arquivo", width=350, anchor="w")
        self.tree.heading("Modo", text="Modo"); self.tree.column("Modo", width=120, anchor="center")
        self.tree.heading("👁️", text="👁️"); self.tree.column("👁️", width=40, anchor="center")
        self.tree.heading("Progresso", text="Progresso"); self.tree.column("Progresso", width=150, anchor="w")
        self.tree.bind("<<TreeviewSelect>>", self._on_task_selection_change); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<Control-a>", self._select_all_tasks); self.tree.bind("<Control-A>", self._select_all_tasks)

    def _create_right_panel(self):
        self.inspector_panel = ctk.CTkFrame(self, width=450); self.inspector_panel.grid(row=0, column=1, padx=(0,10), pady=10, sticky="nsew"); self.inspector_panel.grid_propagate(False)
        self.inspector_label = ctk.CTkLabel(self.inspector_panel, text="Inspetor de Tarefa", font=ctk.CTkFont(size=16, weight="bold")); self.inspector_label.pack(pady=10, padx=10, fill="x")
        self.inspector_content_frame = ctk.CTkScrollableFrame(self.inspector_panel, fg_color="transparent"); self.inspector_content_frame.pack(expand=True, fill="both", padx=5)

    def _create_bottom_panel(self):
        bottom_panel = ctk.CTkFrame(self, height=200); bottom_panel.grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="nsew"); bottom_panel.grid_propagate(False)
        bottom_panel.grid_rowconfigure(1, weight=1); bottom_panel.grid_columnconfigure(0, weight=1)
        self.status_var = ctk.StringVar(); status_bar = ctk.CTkLabel(bottom_panel, textvariable=self.status_var, anchor="w"); status_bar.grid(row=0, column=0, sticky="we", padx=5, pady=(2,0))
        self.log_textbox = ctk.CTkTextbox(bottom_panel, state="disabled", font=("Consolas", 12)); self.log_textbox.grid(row=1, column=0, sticky="nswe", padx=5, pady=5)

    def _select_all_tasks(self, event=None):
        self.tree.selection_set(self.tree.get_children())
        return "break"

    def _on_task_selection_change(self, event=None):
        for widget in self.inspector_content_frame.winfo_children(): widget.destroy()
        selected_ids = self.tree.selection(); tasks = self.db.get_tasks_by_ids(list(selected_ids))
        if not tasks: self.inspector_label.configure(text="Inspetor (Nenhuma Tarefa Selecionada)"); return
        self.inspector_label.configure(text=f"Inspetor ({len(tasks)} Tarefa(s) Selecionada(s))")
        def get_common(key): vals={t.get(key) for t in tasks}; return vals.pop() if len(vals)==1 else None
        op_mode=get_common('operation_mode')
        self._create_widget_group("Modo de Operação", [('radio','operation_mode','Pipeline Completo','full_pipe'), ('radio','operation_mode','Apenas Roteirizar','sapiens_only'), ('radio','operation_mode','Apenas Renderizar','render_only')], get_common)
        if op_mode in ['full_pipe', 'sapiens_only']: self._create_widget_group("Config. de Roteiro", [('check','use_visual_analysis','Usar Análise Visual 👁️'), ('radio','transcription_mode','Gerar com Whisper','whisper'), ('radio','transcription_mode','Usar Arquivo Externo','file'), ('file','transcription_path',"Arquivo de Transcrição:",(("JSON","*.json"),),'transcription_mode','file')], get_common)
        if op_mode in ['full_pipe', 'render_only']: self._create_widget_group("Config. de Renderização", [('file','render_script_path',"Arquivo de Roteiro:",(("JSON","*.json"),))], get_common)

    def _create_widget_group(self, title, widgets_conf, get_common_func):
        frame = ctk.CTkFrame(self.inspector_content_frame); frame.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(frame, text=title, font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        for conf in widgets_conf:
            k, text, v_type = conf[1], conf[2], conf[0]
            if len(conf) > 4 and get_common_func(conf[4]) != conf[5]: continue
            common_val = get_common_func(k)
            if v_type == 'check': var = ctk.BooleanVar(value=bool(common_val if common_val is not None else 1))
            else: var = ctk.StringVar(value=str(common_val if common_val is not None else ''))
            if v_type == 'radio': ctk.CTkRadioButton(frame, text=text, variable=var, value=conf[3], command=lambda k=k, v=conf[3]: self._update_selected_tasks({k:v})).pack(anchor="w", padx=20, pady=2)
            elif v_type == 'check': ctk.CTkCheckBox(frame, text=text, variable=var, command=lambda k=k, v=var: self._update_selected_tasks({k: int(v.get())})).pack(anchor="w", padx=10, pady=5)
            elif v_type == 'file':
                sub_frame=ctk.CTkFrame(frame,fg_color="transparent"); sub_frame.pack(fill='x',padx=10,pady=2); ctk.CTkLabel(sub_frame,text=text).pack(side='left')
                entry=ctk.CTkEntry(sub_frame,textvariable=var); entry.pack(side='left',fill='x',expand=True,padx=5)
                def browse(k=k,v=var,ft=conf[3]): p=filedialog.askopenfilename(filetypes=ft); (v.set(p), self._update_selected_tasks({k: p})) if p else None
                ctk.CTkButton(sub_frame,text="Procurar...",width=80,command=browse).pack(side='left')

    def _show_context_menu(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid: return
        if iid not in self.tree.selection(): self.tree.selection_set(iid)
        menu = Menu(self, tearoff=0, background="#333", foreground="white", activebackground="#0078D7")
        menu.add_command(label="⬆️ Priorizar (Mover para o Topo)", command=self._prioritize_tasks)
        menu.add_command(label="📥 Clonar Tarefa(s)", command=self._clone_tasks)
        menu.add_separator()
        menu.add_command(label="❌ Remover Selecionados", command=self._remove_tasks)
        try: menu.tk_popup(event.x_root, event.y_root)
        finally: menu.grab_release()

    def _prioritize_tasks(self):
        selected_ids = self.tree.selection(); tasks = self.db.get_all_tasks()
        if not selected_ids or not tasks: return
        min_order = tasks[0].get('display_order', 0.0)
        self.logger.log(f"Priorizando {len(selected_ids)} tarefa(s).", "INFO")
        for i, task_id in enumerate(selected_ids):
            self.db.update_task_order(task_id, min_order - 1 - i, wait=(i == len(selected_ids) - 1))
        self._load_and_display_queue()

    def _clone_tasks(self):
        selected_ids = self.tree.selection(); tasks_to_clone = self.db.get_tasks_by_ids(list(selected_ids))
        if not tasks_to_clone: return
        self.logger.log(f"Clonando {len(tasks_to_clone)} tarefa(s).", "INFO")
        max_order = max([t.get('display_order') or 0 for t in self.db.get_all_tasks()], default=0)
        for i, task in enumerate(tasks_to_clone):
            new_id = str(uuid.uuid4())
            config_to_clone = {k: v for k, v in task.items() if k not in ['id', 'video_path', 'display_order', 'added_timestamp', 'status']}
            self.db.add_task(new_id, task['video_path'], max_order + 1 + i, wait=False)
            if config_to_clone: self.db.update_task_config(new_id, config_to_clone, wait=(i == len(tasks_to_clone) - 1))
        self._load_and_display_queue()

    def _update_selected_tasks(self, update_dict):
        selected_ids = self.tree.selection()
        if not selected_ids: return
        self.logger.log(f"Atualizando {len(selected_ids)} tarefas com {update_dict}", "DEBUG")
        for i, task_id in enumerate(selected_ids):
            self.db.update_task_config(task_id, update_dict, wait=(i == len(selected_ids) - 1))
        self.after(50, self._load_and_display_queue); self.after(100, self._on_task_selection_change)

    def _add_tasks(self):
        files = filedialog.askopenfilenames(title="Selecione vídeos", filetypes=(("Vídeos","*.mp4 *.mov *.avi *.mkv"),))
        if not files: return
        self.logger.log(f"Adicionando {len(files)} nova(s) tarefa(s) à fila.", "INFO")
        max_order = max([t.get('display_order') or 0 for t in self.db.get_all_tasks()], default=0)
        for i, f in enumerate(files):
            self.db.add_task(str(uuid.uuid4()), f, max_order + 1 + i, wait=(i == len(files) - 1))
        self._load_and_display_queue()

    def _remove_tasks(self):
        ids = self.tree.selection()
        if ids and messagebox.askyesno("Remover", f"Remover {len(ids)} tarefa(s)?"):
            self.logger.log(f"Removendo {len(ids)} tarefa(s): {ids}", "INFO")
            self.db.delete_tasks(list(ids), wait=True); self._load_and_display_queue()

    def _clear_tasks(self):
        if messagebox.askyesno("Limpar Tarefas", "Deseja remover todas as tarefas concluídas, com erro ou interrompidas?"):
            self.logger.log("Limpando tarefas concluídas e com erro.", "INFO")
            self.db.clear_finished_tasks(wait=True); self._load_and_display_queue()

    def _start_queue(self):
        if self.is_running_task: return
        self.logger.log("Iniciando processamento da fila.", "INFO")
        self.start_button.configure(state="disabled"); self.stop_button.configure(state="normal")
        self.stop_event.clear(); self._start_next_task()

    def _stop_queue(self):
        if not self.is_running_task: return
        self.logger.log("Parada da fila solicitada pelo usuário.", "WARNING")
        self.stop_event.set(); self.orchestrator.interrupt_current_task()
        self.stop_button.configure(state="disabled")

    def _start_next_task(self):
        if self.stop_event.is_set(): self._queue_done(); return
        next_task = next((t for t in self.db.get_all_tasks() if t['status'] in [STATUS_QUEUED, STATUS_INTERRUPTED, STATUS_AWAIT_RENDER]), None)
        if not next_task: self.logger.log("Fila concluída. Nenhuma tarefa pendente.", "SUCCESS"); self._queue_done(); return
        self.is_running_task, self.current_task_id = True, next_task['id']
        self.logger.log(f"Iniciando próxima tarefa: {os.path.basename(next_task['video_path'])} (ID: {self.current_task_id[:8]})", "INFO")
        self.db.update_task_status(self.current_task_id, STATUS_PROCESSING, wait=True); self._load_and_display_queue()
        threading.Thread(target=self._run_task_thread, args=(next_task,), daemon=True, name=f"TaskThread-{next_task['id'][:8]}").start()

    def _run_task_thread(self, task_config):
        """
        Executa uma tarefa em uma thread separada com melhor gerenciamento de estado e erro.
        """
        mode = task_config.get('operation_mode', 'full_pipe')
        task_id = task_config['id']
        
        # Log inicial detalhado do contexto da tarefa
        self.logger.log(f"=== Iniciando Nova Tarefa ===", "INFO", task_id)
        self.logger.log(f"ID: {task_id}", "DEBUG", task_id)
        self.logger.log(f"Modo: {mode}", "INFO", task_id)
        self.logger.log(f"Arquivo: {os.path.basename(task_config.get('video_path', 'N/A'))}", "INFO", task_id)
        self.logger.log(f"Análise Visual: {'Sim' if task_config.get('use_visual_analysis') else 'Não'}", "INFO", task_id)
        
        if mode == 'full_pipe':
            self.logger.log("Pipeline completo: Transcrição → Análise → Renderização", "INFO", task_id)
        elif mode == 'sapiens_only':
            self.logger.log("Apenas roteirização: Transcrição → Análise", "INFO", task_id)
        elif mode == 'render_only':
            self.logger.log("Apenas renderização do roteiro existente", "INFO", task_id)
        
        def send_progress(percentage, stage):
            """Helper para enviar atualizações de progresso."""
            try:
                self.progress_queue.put({
                    'type': 'progress',
                    'task_id': task_id,
                    'percentage': percentage,
                    'stage': stage
                })
            except Exception as e:
                self.logger.log(f"Erro ao enviar progresso: {e}", "ERROR", task_id)
        
        try:
            self.logger.log(f"Iniciando thread de tarefa no modo: {mode}", "DEBUG", task_id)
            send_progress(0, 'Iniciando')
            
            if mode in ['sapiens_only', 'full_pipe']:
                # Pipeline de transcrição e análise
                self.logger.log("Iniciando pipeline Sapiens...", "INFO", task_id)
                self.orchestrator.run_sapiens_task(self.progress_queue, task_config, self.stop_event)
                
            elif mode == 'render_only':
                # Pipeline de renderização
                script_path = task_config.get('render_script_path')
                if not script_path:
                    raise ValueError("Caminho do arquivo de roteiro não especificado")
                    
                self.logger.log("Validando arquivo de roteiro...", "INFO", task_id)
                
                # Usa o método de validação dedicado
                try:
                    script_data = self._validate_script_json(script_path, task_id)
                except ValueError as e:
                    raise ValueError(f"Validação do roteiro falhou: {e}")
                    
                # Atualiza configuração com metadados do script
                try:
                    with threading.Lock():
                        self.db.update_task_config(task_id, {
                            'render_metadata': {
                                'total_segments': len(script_data['segments']),
                                'total_duration': sum(seg['end'] - seg['start'] for seg in script_data['segments']),
                                'validation_timestamp': str(datetime.datetime.now())
                            }
                        }, wait=True)
                except Exception as e:
                    self.logger.log(f"Aviso: Falha ao atualizar metadados: {e}", "WARNING", task_id)
                
                self.logger.log("Iniciando pipeline de renderização...", "INFO", task_id)
                send_progress(0, 'Preparando Renderização')
                
                # Executa renderização
                self.orchestrator.run_render_task(self.progress_queue, task_config, self.stop_event)
                
            else:
                raise ValueError(f"Modo de operação inválido: {mode}")
                
        except Exception as e:
            self.logger.log(f"Erro crítico na thread de tarefa: {e}", "ERROR", task_id, exc_info=True)
            self.logger.log("Stack trace completo:\n" + "".join(traceback.format_exc()), "DEBUG", task_id)
            
            try:
                # Notifica a UI sobre o erro
                self.progress_queue.put({
                    'type': 'error',
                    'task_id': task_id,
                    'message': str(e),
                    'details': {
                        'mode': mode,
                        'timestamp': str(datetime.datetime.now()),
                        'traceback': traceback.format_exc()
                    }
                })
            except Exception as notify_error:
                self.logger.log(f"Erro ao notificar UI: {notify_error}", "ERROR", task_id)
                
        finally:
            self.logger.log("Thread de tarefa finalizada.", "DEBUG", task_id)

    def _queue_done(self):
        self.is_running_task, self.current_task_id = False, None
        self.start_button.configure(state="normal"); self.stop_button.configure(state="disabled")
        self._load_and_display_queue()

    def _process_queues(self):
        """
        Processa as filas de log e progresso de forma thread-safe.
        """
        if not self.winfo_exists():
            return
            
        try:
            # Processa logs em lote para reduzir atualizações da UI
            logs = []
            while True:
                try:
                    logs.append(self.log_queue.get_nowait())
                except queue.Empty:
                    break
                    
            if logs:
                try:
                    self.log_textbox.configure(state="normal")
                    for msg in logs:
                        self.log_textbox.insert("end", msg)
                    self.log_textbox.see("end")
                    self.log_textbox.configure(state="disabled")
                except Exception as e:
                    logging.error(f"Erro ao processar logs: {e}", exc_info=True)
            
            # Processa itens da fila de progresso
            while True:
                try:
                    q_item = self.progress_queue.get_nowait()
                    task_id = q_item.get('task_id')
                    
                    if not task_id:
                        continue
                        
                    if q_item['type'] == 'progress':
                        percentage = q_item.get('percentage', 0)  # Valor default de 0%
                        stage = q_item.get('stage', STATUS_PROCESSING)
                        
                        def update_progress():
                            if not self.winfo_exists():
                                return
                            try:
                                if isinstance(percentage, (int, float)):
                                    progress_text = self._text_progress_bar(float(percentage))
                                else:
                                    progress_text = self._text_progress_bar(0)
                                    self.logger.log(f"Aviso: percentage inválido ({percentage})", "WARNING")
                                    
                                self._update_tree_item(
                                    task_id,
                                    {
                                        'Progresso': progress_text,
                                        'Status': f"⚙️ {stage}"
                                    }
                                )
                            except Exception as e:
                                logging.error(f"Erro ao atualizar progresso: {e}", exc_info=True)
                                
                        self.after(0, update_progress)
                        
                    elif q_item['type'] in ['error', 'interrupted', 'done']:
                        self.after(0, lambda: self._task_done_handler(task_id, q_item))
                        
                    elif q_item['type'] == 'sapiens_done':
                        self.after(0, lambda: self._sapiens_done_handler(task_id, q_item))
                        
                except queue.Empty:
                    break
                except Exception as e:
                    logging.error(f"Erro ao processar item da fila: {e}", exc_info=True)
                    
        except Exception as e:
            logging.error(f"Erro em _process_queues: {e}", exc_info=True)
        finally:
            # Agenda próxima execução
            if self.winfo_exists():
                self.after(100, self._process_queues)

    def _task_done_handler(self, task_id: str, q_item: Dict[str, Any]):
        """
        Manipula a conclusão de uma tarefa de forma thread-safe.
        """
        try:
            # 1. Atualiza o status no banco de dados
            status_map = {
                'error': STATUS_ERROR,
                'interrupted': STATUS_INTERRUPTED,
                'done': STATUS_COMPLETED
            }
            self.db.update_task_status(task_id, status_map[q_item['type']])
            
            # 2. Atualiza o estado interno de forma thread-safe
            with threading.Lock():
                self.is_running_task = False
            
            # 3. Agenda atualizações da UI com delays seguros
            if not self.stop_event.is_set():
                # Primeiro agenda a atualização da UI
                self.after(50, lambda: self._safe_update_ui(task_id))
                # Depois agenda o início da próxima tarefa
                self.after(100, self._start_next_task)
            else:
                # Em caso de parada, primeiro atualiza UI, depois finaliza
                self.after(50, lambda: self._safe_update_ui(task_id))
                self.after(100, self._queue_done)
                
        except Exception as e:
            self.logger.log(f"Erro em _task_done_handler: {e}", "ERROR", task_id, exc_info=True)
            
    def _safe_update_ui(self, task_id: str):
        """
        Método thread-safe para atualizar a UI.
        """
        try:
            if not self.winfo_exists():
                return
            self._load_and_display_queue()
        except Exception as e:
            self.logger.log(f"Erro ao atualizar UI: {e}", "ERROR", task_id, exc_info=True)

    def _validate_script_json(self, script_path, task_id):
        """Valida o arquivo JSON do roteiro."""
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Validações básicas
            if not isinstance(data, dict):
                raise ValueError("JSON inválido: deve ser um objeto/dicionário")
                
            if 'segments' not in data:
                raise ValueError("JSON inválido: propriedade 'segments' não encontrada")
                
            if not isinstance(data['segments'], list):
                raise ValueError("JSON inválido: 'segments' deve ser uma lista")
                
            if not data['segments']:
                raise ValueError("JSON inválido: lista de segmentos está vazia")
                
            # Valida cada segmento
            for i, segment in enumerate(data['segments']):
                if not isinstance(segment, dict):
                    raise ValueError(f"Segmento {i} inválido: deve ser um objeto/dicionário")
                    
                required_fields = ['start', 'end', 'text']
                for field in required_fields:
                    if field not in segment:
                        raise ValueError(f"Segmento {i} inválido: campo '{field}' não encontrado")
                        
                if not isinstance(segment['start'], (int, float)):
                    raise ValueError(f"Segmento {i} inválido: 'start' deve ser numérico")
                    
                if not isinstance(segment['end'], (int, float)):
                    raise ValueError(f"Segmento {i} inválido: 'end' deve ser numérico")
                    
                if segment['start'] >= segment['end']:
                    raise ValueError(f"Segmento {i} inválido: 'start' deve ser menor que 'end'")
                    
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Erro ao decodificar JSON: {str(e)}")
        except Exception as e:
            raise ValueError(f"Erro ao validar script: {str(e)}")

    def _prepare_render_task(self, task_id, script_path):
        """Prepara a tarefa para renderização."""
        try:
            # 1. Valida o script JSON
            script_data = self._validate_script_json(script_path, task_id)
            
            # 2. Atualiza configuração de forma atômica
            with threading.Lock():
                self.db.update_task_config(task_id, {
                    'render_script_path': script_path,
                    'render_config': {
                        'total_segments': len(script_data['segments']),
                        'total_duration': sum(seg['end'] - seg['start'] for seg in script_data['segments']),
                        'timestamp': str(datetime.datetime.now())
                    }
                }, wait=True)
                self.db.update_task_status(task_id, STATUS_AWAIT_RENDER, wait=True)
                
            return True
            
        except Exception as e:
            self.logger.log(f"Erro ao preparar renderização: {e}", "ERROR", task_id, exc_info=True)
            return False

    def _sapiens_done_handler(self, task_id, q_item):
        """Manipula a conclusão do pipeline Sapiens com transição segura para renderização."""
        try:
            if not self.winfo_exists():
                return
                
            tasks = self.db.get_tasks_by_ids([task_id])
            if not tasks:
                raise ValueError("Tarefa não encontrada no banco de dados")
            task_config = tasks[0]
            
            if task_config.get('operation_mode') == 'full_pipe':
                # 1. Validações iniciais
                if 'script_path' not in q_item:
                    raise ValueError("Caminho do script não fornecido")
                    
                script_path = q_item['script_path']
                if not os.path.exists(script_path):
                    raise ValueError(f"Arquivo de script não encontrado: {script_path}")
                    
                self.logger.log(f"Pipeline 'Sapiens' concluído. Preparando renderização...", "INFO", task_id)
                
                # 2. Prepara a tarefa para renderização
                if not self._prepare_render_task(task_id, script_path):
                    raise ValueError("Falha ao preparar tarefa para renderização")
                
                # 3. Atualiza UI
                def update_ui():
                    if not self.winfo_exists():
                        return
                    self._load_and_display_queue()
                self.after(0, update_ui)
                
                # 4. Inicia renderização com delay de segurança
                def start_render_safe():
                    if not self.winfo_exists():
                        return
                        
                    try:
                        # Obtém configuração atualizada
                        updated_tasks = self.db.get_tasks_by_ids([task_id])
                        if not updated_tasks:
                            raise ValueError("Tarefa não encontrada após atualização")
                        updated_task = updated_tasks[0]
                        
                        self.logger.log("Iniciando thread de renderização...", "INFO", task_id)
                        
                        # Sinal inicial de progresso
                        self.progress_queue.put({
                            'type': 'progress',
                            'task_id': task_id,
                            'percentage': 0,
                            'stage': 'Iniciando Renderização'
                        })
                        
                        # Inicia thread de renderização
                        render_thread = threading.Thread(
                            target=self.orchestrator.run_render_task,
                            args=(self.progress_queue, updated_task, self.stop_event),
                            daemon=True,
                            name=f"RenderThread-{task_id[:8]}"
                        )
                        render_thread.start()
                        
                    except Exception as e:
                        self.logger.log(f"Erro ao iniciar renderização: {e}", "ERROR", task_id, exc_info=True)
                        self._task_done_handler(task_id, {
                            'type': 'error',
                            'task_id': task_id,
                            'message': f"Erro ao iniciar renderização: {e}"
                        })
                
                self.after(100, start_render_safe)
                
            else:
                self._task_done_handler(task_id, {'type': 'done', 'task_id': task_id})
                
        except Exception as e:
            self.logger.log(f"Erro crítico em _sapiens_done_handler: {e}", "ERROR", task_id, exc_info=True)
            self._task_done_handler(task_id, {
                'type': 'error',
                'task_id': task_id,
                'message': f"Erro crítico no handler: {e}"
            })

    def _load_and_display_queue(self):
        selection = self.tree.selection(); valid_selection = [s for s in selection if self.tree.exists(s)]
        self.tree.delete(*self.tree.get_children())
        status_map = {STATUS_QUEUED:'🕘', STATUS_COMPLETED:'✅', STATUS_ERROR:'❌', STATUS_INTERRUPTED:'⏸️', STATUS_AWAIT_RENDER:'▶️', STATUS_PROCESSING:'⚙️'}
        mode_map = {'full_pipe': 'Completo', 'sapiens_only': 'Roteiro', 'render_only': 'Render'}
        for task in self.db.get_all_tasks():
            status_icon = "▶️" if task['status'] == STATUS_PROCESSING and task['id'] == self.current_task_id else status_map.get(task['status'], '⚙️')
            self.tree.insert("", "end", iid=task['id'], values=(f"{status_icon} {task['status']}", os.path.basename(task.get('video_path','')), mode_map.get(task.get('operation_mode'),'N/D'), "✓" if task.get('use_visual_analysis') else "✗", ""))
        if valid_selection:
            try: self.tree.selection_set(valid_selection)
            except Exception as e: self.logger.log(f"Não foi possível restaurar a seleção da árvore: {e}", "DEBUG")

    def _update_tree_item(self, iid, values_dict):
        if not self.tree.exists(iid): return
        current = list(self.tree.item(iid, 'values')); cols = self.tree['columns']
        for col_name, val in values_dict.items():
            if col_name in cols: current[cols.index(col_name)] = val
        self.tree.item(iid, values=tuple(current))

    def _text_progress_bar(self, p, w=15): p=max(0,min(100,p)); f=int(w*p//100); return f"|{'█'*f}{'░'*(w-f)}| {p:.1f}%"
    def _open_presets_manager(self): PresetsManager(self, self.db, lambda: self.db.get_tasks_by_ids(list(self.tree.selection())))
    def _open_advanced_settings(self): AdvancedSettings(self, self.config, self.logger)


