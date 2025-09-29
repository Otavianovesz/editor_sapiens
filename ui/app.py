# -*- coding: utf-8 -*-

import os
import sys
import uuid
import queue
import threading
import json
import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk, Menu
import traceback
import logging
import threading
import sys

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
    def __init__(self):
        super().__init__()
        self.title("Editor Sapiens (Arconte)")
        self.geometry("1600x900")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.log_queue, self.progress_queue = queue.Queue(), queue.Queue()
        self.db = DatabaseManager()
        self.logger = Logger(self.log_queue, self.db)
        self.config = Config()
        self.orchestrator = Orchestrator(self.logger, self.config)
        def _thread_excepthook(args):
            # args é threading.ExceptHookArgs: (exc_type, exc_value, exc_traceback, thread)
            try:
                logging.critical(f"Unhandled exception in thread {args.thread.name}: {args.exc_value}", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
            except Exception:
                logging.exception("Falha ao logar exceção de thread.")
            # tenta também enviar para o logger de UI (se disponível)
            try:
                tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
                if hasattr(self, "logger"):
                    self.logger.log(f"Unhandled exception in thread {args.thread.name}: {args.exc_value}", "CRITICAL")
                    self.logger.log(tb, "DEBUG")
                else:
                    logging.debug(tb)
            except Exception:
                logging.exception("Falha ao enviar exceção de thread para self.logger.")
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
            # registra a stack para entender quem chamou
            self.logger.log("Stack do _on_closing:\n" + stack, "DEBUG")
        except Exception:
            logging.exception("Falha ao registrar stack no _on_closing.")

        # Se há tarefa em execução e não for forçado, confirmar com o usuário
        if self.is_running_task and not force:
            try:
                answer = messagebox.askyesno("Sair", "Tarefa em andamento. Deseja realmente sair?")
            except Exception:
                # se messagebox falhar por algum motivo, assume NÃO para evitar fechar à força
                self.logger.log("Falha ao mostrar diálogo de confirmação; cancelando fechamento.", "WARNING")
                return

            if not answer:
                self.logger.log("Fechamento cancelado pelo usuário (tarefa em andamento).", "INFO")
                return

            # usuário confirmou: sinaliza parada e interrompe a tarefa
            try:
                self.stop_event.set()
                self.orchestrator.interrupt_current_task()
            except Exception as e:
                self.logger.log(f"Erro ao tentar interromper tarefa: {e}", "ERROR")

        # Se chegou aqui, pode fechar com segurança (ou foi chamado com force=True)
        try:
            # garante que todas as escritas pendentes no DB sejam concluídas
            if hasattr(self.db, "_write_queue") and hasattr(self.db, "_writer_thread"):
                try:
                    self.db._write_queue.join()
                except Exception:
                    pass

            # fecha o DB de forma tolerante
            try:
                self.db.close()
            except Exception as e:
                self.logger.log(f"Falha ao fechar DB durante o encerramento: {e}", "WARNING")

            # finalmente destrói a janela principal
            try:
                self.destroy()
            except Exception as e:
                self.logger.log(f"Falha ao destruir janela principal: {e}", "ERROR")
        except Exception:
            logging.exception("Erro inesperado no processo de fechamento.")

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
        mode = task_config.get('operation_mode', 'full_pipe')
        try:
            if mode in ['sapiens_only', 'full_pipe']:
                self.orchestrator.run_sapiens_task(self.progress_queue, task_config, self.stop_event)
            elif mode == 'render_only':
                script_path = task_config.get('render_script_path')
                if not script_path or not os.path.exists(script_path):
                    raise ValueError("Arquivo de roteiro para render não foi encontrado ou é inválido.")
                
                # Validação do arquivo JSON para fornecer um erro mais claro.
                try:
                    with open(script_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if not isinstance(data.get('segments'), list) or not data['segments']:
                        raise ValueError("Roteiro inválido: a chave 'segments' está ausente ou a lista está vazia.")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Falha ao ler o arquivo de roteiro (JSON mal formatado): {e}")

                self.orchestrator.run_render_task(self.progress_queue, task_config, self.stop_event)
            else:
                raise ValueError(f"Modo de operação desconhecido: {mode}")
        except Exception as e:
            self.logger.log(f"Erro na thread da tarefa: {e}", "CRITICAL", task_config['id'], exc_info=True)
            self.progress_queue.put({'type': 'error', 'message': str(e), 'task_id': task_config['id']})

    def _queue_done(self):
        self.is_running_task, self.current_task_id = False, None
        self.start_button.configure(state="normal"); self.stop_button.configure(state="disabled")
        self._load_and_display_queue()

    def _process_queues(self):
        try:
            while msg := self.log_queue.get_nowait(): self.log_textbox.configure(state="normal"); self.log_textbox.insert("end", msg); self.log_textbox.see("end"); self.log_textbox.configure(state="disabled")
        except queue.Empty: pass
        try:
            while q_item := self.progress_queue.get_nowait():
                if q_item['type'] == 'progress': self._update_tree_item(q_item['task_id'], {'Progresso': self._text_progress_bar(q_item['percentage']), 'Status': f"⚙️ {q_item.get('stage', STATUS_PROCESSING)}"})
                elif q_item['type'] in ['error', 'interrupted', 'done']: self._task_done_handler(q_item['task_id'], q_item)
                elif q_item['type'] == 'sapiens_done': self._sapiens_done_handler(q_item['task_id'], q_item)
        except queue.Empty: pass
        self.after(100, self._process_queues)

    def _task_done_handler(self, task_id, q_item):
        status_map = {'error': STATUS_ERROR, 'interrupted': STATUS_INTERRUPTED, 'done': STATUS_COMPLETED}
        self.db.update_task_status(task_id, status_map[q_item['type']])
        self.is_running_task = False
        if not self.stop_event.is_set(): self.after(500, self._start_next_task)
        else: self._queue_done()
        self._load_and_display_queue()

    def _sapiens_done_handler(self, task_id, q_item):
        task_config = self.db.get_tasks_by_ids([task_id])[0]
        if task_config.get('operation_mode') == 'full_pipe':
            self.logger.log(f"Pipeline 'Sapiens' concluído. Roteiro gerado. Iniciando renderização...", "INFO", task_id)
            self.db.update_task_config(task_id, {'render_script_path': q_item['script_path']}, wait=True)
            self.db.update_task_status(task_id, STATUS_AWAIT_RENDER, wait=True)
            self._load_and_display_queue()
            updated_task = self.db.get_tasks_by_ids([task_id])[0]
            threading.Thread(target=self.orchestrator.run_render_task, args=(self.progress_queue, updated_task, self.stop_event), daemon=True).start()
        else: self._task_done_handler(task_id, {'type': 'done', 'task_id': task_id})

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


