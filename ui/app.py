# -*- coding: utf-8 -*-

import os
import sys
import uuid
import queue
import threading
import json
import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk, Menu
from typing import Dict, Any, List, Optional

# Imports project modules
from core.database import DatabaseManager
from core.config import Config
from core.orchestrator import Orchestrator
from utils.logger import Logger
from utils.constants import *
from .presets_window import PresetsManager
from .settings_window import AdvancedSettings
from core.processing_modules import AudioTranscriber, ContentAnalyzer, ScriptComposer, VisualAnalyzer, SubtitleParser, MediaProcessor


# Fallback for CTkToolTip to ensure compatibility with older versions of CTk
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
    """The main application class."""
    def __init__(self):
        """Initializes the main application window."""
        super().__init__()
        self.title("Editor Sapiens (Arconte)")
        self.geometry("1600x900")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.db = DatabaseManager()
        self.logger = Logger(self.log_queue, self.db)
        self.config = Config()

        # Instantiation of the modules
        audio_transcriber = AudioTranscriber(self.logger, self.config)
        content_analyzer = ContentAnalyzer(self.logger, self.config)
        script_composer = ScriptComposer(self.logger)
        visual_analyzer = VisualAnalyzer(self.logger, self.config)
        subtitle_parser = SubtitleParser(self.logger)
        media_processor = MediaProcessor(self.logger)

        # Dictionary of modules
        modules = {
            'transcriber': audio_transcriber,
            'content': content_analyzer,
            'composer': script_composer,
            'visual': visual_analyzer,
            'parser': subtitle_parser,
            'media_processor': media_processor,
        }

        # --- CORRECTION APPLIED HERE ---
        # The logger (self.logger) is now passed as the first argument,
        # followed by the config and the modules.
        self.orchestrator = Orchestrator(self.logger, self.config, modules)

        self.is_running_task = False
        self.stop_event = threading.Event()
        self.current_task_id: Optional[str] = None
        self.is_ui_blocked = False

        self._create_widgets()
        self.after(100, self._post_init_startup)
        self.after(200, self._process_queues)
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    def set_status_text(self, text: str):
        """Sets the text in the bottom status bar.

        Args:
            text (str): The text to be displayed.
        """
        self.status_var.set(text)

    def _post_init_startup(self):
        """Tasks to be executed after the complete initialization of the UI."""
        if not self.db.is_writer_healthy:
            self.logger.log("Writer DB did not respond. Operating in read-only mode.", "ERROR")
        else:
            self.logger.log("Writer DB ready and database schema validated.", "INFO")
        self.db.recover_interrupted_tasks(wait=True)
        self._load_and_display_queue()

    def _on_closing(self):
        """Handles the closing of the main window safely."""
        self.logger.log("Closing the application...", "INFO")
        if self.is_running_task:
            if messagebox.askyesno("Exit", "A task is in progress. Do you really want to interrupt it and exit?"):
                self.stop_event.set()
                self.orchestrator.interrupt_current_task()
            else:
                return # Cancels the closing
        self.db.close()
        self.destroy()

    def _create_widgets(self):
        """Creates and organizes all the widgets of the main interface."""
        self.grid_columnconfigure(0, weight=5); self.grid_columnconfigure(1, weight=3); self.grid_rowconfigure(0, weight=1)
        
        self.main_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.main_frame.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.main_frame.grid_columnconfigure(0, weight=5); self.main_frame.grid_columnconfigure(1, weight=3); self.main_frame.grid_rowconfigure(0, weight=1)

        self._create_menubar()
        self._create_center_panel(self.main_frame)
        self._create_right_panel(self.main_frame)
        self._create_bottom_panel()

    def _create_menubar(self):
        """Creates the menu bar."""
        self.menubar = Menu(self); self.configure(menu=self.menubar)
        file_menu = Menu(self.menubar, tearoff=0); file_menu.add_command(label="Add Tasks...", command=self._add_tasks); file_menu.add_separator(); file_menu.add_command(label="Exit", command=self._on_closing); self.menubar.add_cascade(label="File", menu=file_menu)
        edit_menu = Menu(self.menubar, tearoff=0); edit_menu.add_command(label="Remove Selected", command=self._remove_tasks); edit_menu.add_command(label="Clear Completed/Errored", command=self._clear_tasks); self.menubar.add_cascade(label="Edit", menu=edit_menu)
        process_menu = Menu(self.menubar, tearoff=0); process_menu.add_command(label="Start Queue", command=self._start_queue); process_menu.add_command(label="Stop Queue", command=self._stop_queue); self.menubar.add_cascade(label="Processing", menu=process_menu)
        tools_menu = Menu(self.menubar, tearoff=0); tools_menu.add_command(label="Presets Manager...", command=self._open_presets_manager); tools_menu.add_command(label="Advanced Settings...", command=self._open_advanced_settings); self.menubar.add_cascade(label="Tools", menu=tools_menu)

    def _create_center_panel(self, parent):
        """Creates the center panel.

        Args:
            parent: The parent widget.
        """
        center_panel = ctk.CTkFrame(parent); center_panel.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        center_panel.grid_rowconfigure(1, weight=1); center_panel.grid_columnconfigure(0, weight=1)
        toolbar = ctk.CTkFrame(center_panel); toolbar.grid(row=0, column=0, padx=10, pady=(10,5), sticky="ew")
        self.add_button = ctk.CTkButton(toolbar, text="➕ Add", command=self._add_tasks); self.add_button.pack(side="left", padx=5)
        self.remove_button = ctk.CTkButton(toolbar, text="➖ Remove", command=self._remove_tasks); self.remove_button.pack(side="left", padx=5)
        self.start_button = ctk.CTkButton(toolbar, text="▶️ Start Queue", command=self._start_queue); self.start_button.pack(side="left", padx=5)
        self.stop_button = ctk.CTkButton(toolbar, text="⏹️ Stop", command=self._stop_queue, state="disabled"); self.stop_button.pack(side="left", padx=5)
        self.refresh_button = ctk.CTkButton(toolbar, text="🔄 Refresh", command=self._load_and_display_queue, width=100); self.refresh_button.pack(side="right", padx=5)
        style = ttk.Style(self); style.theme_use("default"); style.configure("Treeview", background="#2b2b2b", foreground="white", fieldbackground="#2b2b2b", rowheight=32)
        style.map('Treeview', background=[('selected', '#3a7ebf')]); style.configure("Treeview.Heading", background="#565b5e", foreground="white", font=('CTkFont', 12, 'bold'))
        self.tree = ttk.Treeview(center_panel, columns=("Status", "File", "Mode", "👁️", "Progress"), show="headings")
        self.tree.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="nsew")
        self.tree.heading("Status", text="Status"); self.tree.column("Status", width=150, anchor="w")
        self.tree.heading("File", text="File Name"); self.tree.column("File", width=350, anchor="w")
        self.tree.heading("Mode", text="Mode"); self.tree.column("Mode", width=120, anchor="center")
        self.tree.heading("👁️", text="👁️"); self.tree.column("👁️", width=40, anchor="center")
        self.tree.heading("Progress", text="Progress"); self.tree.column("Progress", width=150, anchor="w")
        self.tree.bind("<<TreeviewSelect>>", self._on_task_selection_change); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<Control-a>", self._select_all_tasks); self.tree.bind("<Control-A>", self._select_all_tasks)

    def _create_right_panel(self, parent):
        """Creates the right panel.

        Args:
            parent: The parent widget.
        """
        self.inspector_panel = ctk.CTkFrame(parent, width=450); self.inspector_panel.grid(row=0, column=1, padx=(0,10), pady=10, sticky="nsew"); self.inspector_panel.grid_propagate(False)
        self.inspector_label = ctk.CTkLabel(self.inspector_panel, text="Task Inspector", font=ctk.CTkFont(size=16, weight="bold")); self.inspector_label.pack(pady=10, padx=10, fill="x")
        self.inspector_content_frame = ctk.CTkScrollableFrame(self.inspector_panel, fg_color="transparent"); self.inspector_content_frame.pack(expand=True, fill="both", padx=5)

    def _create_bottom_panel(self):
        """Creates the bottom panel."""
        bottom_panel = ctk.CTkFrame(self, height=200); bottom_panel.grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="nsew"); bottom_panel.grid_propagate(False)
        bottom_panel.grid_rowconfigure(1, weight=1); bottom_panel.grid_columnconfigure(0, weight=1)
        self.status_var = ctk.StringVar(); status_bar = ctk.CTkLabel(bottom_panel, textvariable=self.status_var, anchor="w"); status_bar.grid(row=0, column=0, sticky="we", padx=5, pady=(2,0))
        self.log_textbox = ctk.CTkTextbox(bottom_panel, state="disabled", font=("Consolas", 12)); self.log_textbox.grid(row=1, column=0, sticky="nswe", padx=5, pady=5)

    def _select_all_tasks(self, event=None):
        """Selects all tasks in the task list."""
        self.tree.selection_set(self.tree.get_children())
        return "break"

    def _on_task_selection_change(self, event=None):
        """Updates the inspector panel based on the selected tasks."""
        for widget in self.inspector_content_frame.winfo_children(): widget.destroy()
        selected_ids = self.tree.selection()
        if not selected_ids:
            self.inspector_label.configure(text="Inspector (No Task Selected)")
            return
        tasks = self.db.get_tasks_by_ids(list(selected_ids))
        if not tasks: return
        self.inspector_label.configure(text=f"Inspector ({len(tasks)} Task(s) Selected)")
        def get_common_value(key: str) -> Any:
            values = {t.get(key) for t in tasks}
            return values.pop() if len(values) == 1 else None
        op_mode = get_common_value('operation_mode')
        self._create_widget_group("Operation Mode", [('radio', 'operation_mode', 'Complete Pipeline', 'full_pipe'), ('radio', 'operation_mode', 'Script Only', 'sapiens_only'), ('radio', 'operation_mode', 'Render Only', 'render_only')], get_common_value)
        if op_mode in ['full_pipe', 'sapiens_only']:
            transcription_file_types = (("Transcription Files", "*.json *.srt *.vtt"), ("JSON", "*.json"), ("SRT", "*.srt"), ("VTT", "*.vtt"))
            self._create_widget_group("Script Config.", [('check', 'use_visual_analysis', 'Use Visual Analysis 👁️', {'tooltip': 'Feature in development.'}), ('radio', 'transcription_mode', 'Generate with Whisper', 'whisper'), ('radio', 'transcription_mode', 'Use External File', 'file'), ('file', 'transcription_path', "Transcription File:", transcription_file_types, 'transcription_mode', 'file')], get_common_value)
        if op_mode in ['full_pipe', 'render_only']:
            self._create_widget_group("Render Config.", [('file', 'render_script_path', "Script File:", (("JSON", "*.json"),))], get_common_value)

    def _create_widget_group(self, title: str, widgets_conf: list, get_common_func: callable):
        """Creates a group of widgets in the inspector panel.

        Args:
            title (str): The title of the group.
            widgets_conf (list): A list of widget configurations.
            get_common_func (callable): A function to get the common value of a key.
        """
        frame = ctk.CTkFrame(self.inspector_content_frame); frame.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(frame, text=title, font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        for conf in widgets_conf:
            widget_type, key, text = conf[0], conf[1], conf[2]
            if len(conf) > 4 and get_common_func(conf[4]) != conf[5]: continue
            common_val = get_common_func(key)
            var = ctk.BooleanVar() if widget_type == 'check' else ctk.StringVar()
            if common_val is not None: var.set(common_val)
            if widget_type == 'radio': ctk.CTkRadioButton(frame, text=text, variable=var, value=conf[3], command=lambda k=key, v=conf[3]: self._update_selected_tasks({k: v})).pack(anchor="w", padx=20, pady=2)
            elif widget_type == 'check':
                extra_options = conf[3] if len(conf) > 3 else {}
                tooltip_text = extra_options.pop('tooltip', None)
                command = lambda k=key, v=var: self._update_selected_tasks({k: int(v.get())})
                cb = ctk.CTkCheckBox(frame, text=text, variable=var, command=command, **extra_options)
                if tooltip_text: ctk.CTkToolTip(cb, message=tooltip_text)
                cb.pack(anchor="w", padx=10, pady=5)
            elif widget_type == 'file':
                file_types = conf[3]
                sub_frame=ctk.CTkFrame(frame,fg_color="transparent"); sub_frame.pack(fill='x',padx=10,pady=2)
                ctk.CTkLabel(sub_frame,text=text).pack(side='left')
                entry=ctk.CTkEntry(sub_frame,textvariable=var); entry.pack(side='left',fill='x',expand=True,padx=5)
                def browse(k=key, v=var, ft=file_types):
                    p = filedialog.askopenfilename(filetypes=ft)
                    if p: v.set(p); self._update_selected_tasks({k: p})
                ctk.CTkButton(sub_frame,text="Browse...",width=80,command=browse).pack(side='left')

    def _show_context_menu(self, event):
        """Shows the context menu for the task list.

        Args:
            event: The event that triggered the context menu.
        """
        iid = self.tree.identify_row(event.y)
        if not iid: return
        if iid not in self.tree.selection(): self.tree.selection_set(iid)
        menu = Menu(self, tearoff=0, background="#333", foreground="white", activebackground="#0078D7")
        menu.add_command(label="⬆️ Prioritize (Move to Top)", command=self._prioritize_tasks)
        menu.add_command(label="📥 Clone Task(s)", command=self._clone_tasks)
        menu.add_separator()
        menu.add_command(label="❌ Remove Selected", command=self._remove_tasks)
        try: menu.tk_popup(event.x_root, event.y_root)
        finally: menu.grab_release()

    def _prioritize_tasks(self):
        """Prioritizes the selected tasks."""
        selected_ids = self.tree.selection(); tasks = self.db.get_all_tasks()
        if not selected_ids or not tasks: return
        min_order = tasks[0].get('display_order', 0.0)
        self.logger.log(f"Prioritizing {len(selected_ids)} task(s).", "INFO")
        for i, task_id in enumerate(selected_ids): self.db.update_task_order(task_id, min_order - 1 - i, wait=(i == len(selected_ids) - 1))
        self._load_and_display_queue()

    def _clone_tasks(self):
        """Clones the selected tasks."""
        selected_ids = self.tree.selection(); tasks_to_clone = self.db.get_tasks_by_ids(list(selected_ids))
        if not tasks_to_clone: return
        self.logger.log(f"Cloning {len(tasks_to_clone)} task(s).", "INFO")
        max_order = max([t.get('display_order', 0.0) for t in self.db.get_all_tasks()], default=0.0)
        for i, task in enumerate(tasks_to_clone):
            new_id = str(uuid.uuid4())
            config_to_clone = {k: v for k, v in task.items() if k not in ['id', 'video_path', 'display_order', 'added_timestamp', 'status']}
            self.db.add_task(new_id, task['video_path'], max_order + 1 + i, wait=False)
            if config_to_clone: self.db.update_task_config(new_id, config_to_clone, wait=(i == len(tasks_to_clone) - 1))
        self._load_and_display_queue()

    def _update_selected_tasks(self, update_dict: Dict[str, Any]):
        """Updates the selected tasks with the given configuration.

        Args:
            update_dict (Dict[str, Any]): A dictionary with the configuration to update.
        """
        selected_ids = self.tree.selection()
        if not selected_ids: return
        self.logger.log(f"Updating {len(selected_ids)} tasks with {update_dict}", "DEBUG")
        for i, task_id in enumerate(selected_ids): self.db.update_task_config(task_id, update_dict, wait=(i == len(selected_ids) - 1))
        self.after(50, self._load_and_display_queue); self.after(100, self._on_task_selection_change)

    def _add_tasks(self):
        """Adds new tasks to the queue."""
        files = filedialog.askopenfilenames(title="Select videos", filetypes=(("Videos","*.mp4 *.mov *.avi *.mkv"), ("All Files", "*.*")))
        if not files: return
        self.logger.log(f"Adding {len(files)} new task(s) to the queue.", "INFO")
        max_order = max([t.get('display_order', 0.0) for t in self.db.get_all_tasks()], default=0.0)
        for i, f in enumerate(files): self.db.add_task(str(uuid.uuid4()), f, max_order + 1 + i, wait=(i == len(files) - 1))
        self._load_and_display_queue()

    def _remove_tasks(self):
        """Removes the selected tasks from the queue."""
        ids = self.tree.selection()
        if ids and messagebox.askyesno("Remove", f"Are you sure you want to remove {len(ids)} task(s)?"):
            self.logger.log(f"Removing {len(ids)} task(s).", "INFO"); self.db.delete_tasks(list(ids), wait=True); self._load_and_display_queue()

    def _clear_tasks(self):
        """Removes all completed, errored, or interrupted tasks from the queue."""
        if messagebox.askyesno("Clear Tasks", "Do you want to remove all completed, errored, or interrupted tasks?"):
            self.logger.log("Clearing finished tasks.", "INFO"); self.db.clear_finished_tasks(wait=True); self._load_and_display_queue()

    def _start_queue(self):
        """Starts processing the queue."""
        if self.is_running_task: return
        self.logger.log("Starting queue processing.", "INFO")
        self.start_button.configure(state="disabled"); self.stop_button.configure(state="normal")
        self.stop_event.clear(); self._start_next_task()

    def _stop_queue(self):
        """Stops processing the queue."""
        if not self.is_running_task: return
        self.logger.log("Queue stop requested by the user.", "WARNING")
        self.stop_event.set(); self.orchestrator.interrupt_current_task()
        self.stop_button.configure(text="Stopping...", state="disabled")

    def _set_ui_blocking(self, is_blocking: bool, message: str = ""):
        """Blocks or unblocks the UI during critical operations such as loading the model.

        Args:
            is_blocking (bool): Whether to block the UI.
            message (str, optional): The message to be displayed. Defaults to "".
        """
        if is_blocking == self.is_ui_blocked: return # Avoids redundant work
        
        self.is_ui_blocked = is_blocking
        state = "disabled" if is_blocking else "normal"
        
        # Blocks/unblocks the main buttons and the menu
        self.start_button.configure(state=state)
        self.add_button.configure(state=state)
        self.remove_button.configure(state=state)
        self.refresh_button.configure(state=state)
        self.menubar.entryconfig("File", state=state)
        self.menubar.entryconfig("Edit", state=state)
        self.menubar.entryconfig("Tools", state=state)
        
        if is_blocking:
            self.set_status_text(f"Wait: {message}")
        else:
            self.set_status_text("")

    def _start_next_task(self):
        """Starts the next task in the queue."""
        if self.stop_event.is_set(): self._queue_done("Queue stopped by the user."); return
        next_task = next((t for t in self.db.get_all_tasks() if t['status'] in [STATUS_QUEUED, STATUS_INTERRUPTED, STATUS_AWAIT_RENDER]), None)
        if not next_task: self._queue_done("Queue completed. No pending tasks."); return
        self.is_running_task, self.current_task_id = True, next_task['id']
        self.logger.log(f"Starting next task: {os.path.basename(next_task['video_path'])} (ID: {self.current_task_id[:8]})", "INFO")
        self.db.update_task_status(self.current_task_id, STATUS_PROCESSING, wait=True); self._load_and_display_queue()
        threading.Thread(target=self._run_task_thread, args=(next_task,), daemon=True, name=f"TaskThread-{next_task['id'][:8]}").start()

    def _run_task_thread(self, task_config: Dict[str, Any]):
        """Runs a task in a separate thread.

        Args:
            task_config (Dict[str, Any]): The configuration for the task.
        """
        mode = task_config.get('operation_mode', 'full_pipe')
        try:
            if mode in ['sapiens_only', 'full_pipe']: self.orchestrator.run_sapiens_task(self.progress_queue, task_config, self.stop_event)
            elif mode == 'render_only':
                script_path = task_config.get('render_script_path')
                if not script_path or not os.path.exists(script_path): raise ValueError("Script file for rendering not found or invalid.")
                self.orchestrator.run_render_task(self.progress_queue, task_config, self.stop_event)
            else: raise ValueError(f"Unknown operation mode: {mode}")
        except Exception as e:
            self.logger.log(f"Unexpected error in the task thread: {e}", "CRITICAL", task_config['id'], exc_info=True)
            self.progress_queue.put({'type': 'error', 'message': f"Unexpected error: {e}", 'task_id': task_config['id']})

    def _queue_done(self, message: str):
        """Handles the completion of the queue.

        Args:
            message (str): The message to be displayed.
        """
        self.logger.log(message, "SUCCESS" if "completed" in message else "INFO")
        self.is_running_task, self.current_task_id = False, None
        self.start_button.configure(state="normal")
        self.stop_button.configure(text="⏹️ Stop", state="disabled")
        self._set_ui_blocking(False) # Ensures that the UI is unblocked at the end of the queue
        self._load_and_display_queue()

    def _process_queues(self):
        """Processes the log and progress queues."""
        try:
            while msg := self.log_queue.get_nowait():
                self.log_textbox.configure(state="normal"); self.log_textbox.insert("end", msg); self.log_textbox.see("end"); self.log_textbox.configure(state="disabled")
        except queue.Empty: pass
        try:
            while q_item := self.progress_queue.get_nowait():
                task_id, item_type = q_item['task_id'], q_item['type']
                if item_type == 'progress':
                    stage = q_item.get('stage', STATUS_PROCESSING)
                    # Logic to block the UI during model loading
                    if 'Model' in stage:
                        self._set_ui_blocking(True, stage)
                    else:
                        self._set_ui_blocking(False)
                    self._update_tree_item(task_id, {'Progress': self._text_progress_bar(q_item['percentage']), 'Status': f"⚙️ {stage}"})
                elif item_type in ['error', 'interrupted', 'done']: self._task_done_handler(task_id, q_item)
                elif item_type == 'sapiens_done': self._sapiens_done_handler(task_id, q_item)
        except queue.Empty: pass
        self.after(100, self._process_queues)

    def _task_done_handler(self, task_id: str, q_item: Dict[str, Any]):
        """Handles the completion of a task.

        Args:
            task_id (str): The ID of the task.
            q_item (Dict[str, Any]): The queue item.
        """
        status_map = {'error': STATUS_ERROR, 'interrupted': STATUS_INTERRUPTED, 'done': STATUS_COMPLETED}
        self.db.update_task_status(task_id, status_map[q_item['type']])
        self.is_running_task = False
        self._set_ui_blocking(False) # Ensures unblocking in case of error/interruption
        if not self.stop_event.is_set(): self.after(500, self._start_next_task)
        else: self._queue_done("Queue stopped by the user.")
        self._load_and_display_queue()

    def _sapiens_done_handler(self, task_id: str, q_item: Dict[str, Any]):
        """Handles the completion of the Sapiens part of the pipeline.

        Args:
            task_id (str): The ID of the task.
            q_item (Dict[str, Any]): The queue item.
        """
        self._set_ui_blocking(False) # Unblocks the UI after the Sapiens stage
        task_config = self.db.get_tasks_by_ids([task_id])[0]
        if not task_config:
            self.logger.log(f"Task {task_id} not found in the DB. Aborting.", "ERROR")
            self._task_done_handler(task_id, {'type': 'error', 'message': 'Task disappeared from the DB.'}); return
        if task_config.get('operation_mode') == 'full_pipe':
            self.logger.log("'Sapiens' pipeline completed. Starting rendering...", "INFO", task_id)
            self.db.update_task_config(task_id, {'render_script_path': q_item['script_path']}, wait=True)
            self.db.update_task_status(task_id, STATUS_AWAIT_RENDER, wait=True)
            self._load_and_display_queue()
            updated_task = self.db.get_tasks_by_ids([task_id])[0]
            threading.Thread(target=self.orchestrator.run_render_task, args=(self.progress_queue, updated_task, self.stop_event), daemon=True).start()
        else: self._task_done_handler(task_id, {'type': 'done', 'task_id': task_id})

    def _load_and_display_queue(self):
        """Loads and displays the task queue."""
        selection = self.tree.selection(); scroll_pos = self.tree.yview()
        self.tree.delete(*self.tree.get_children())
        status_map = {STATUS_QUEUED:'🕘', STATUS_COMPLETED:'✅', STATUS_ERROR:'❌', STATUS_INTERRUPTED:'⏸️', STATUS_AWAIT_RENDER:'▶️', STATUS_PROCESSING:'⚙️'}
        mode_map = {'full_pipe': 'Complete', 'sapiens_only': 'Script', 'render_only': 'Render'}
        for task in self.db.get_all_tasks():
            status_icon = "▶️" if task['status'] == STATUS_PROCESSING and task['id'] == self.current_task_id else status_map.get(task['status'], '⚙️')
            self.tree.insert("", "end", iid=task['id'], values=(f"{status_icon} {task['status']}", os.path.basename(task.get('video_path','')), mode_map.get(task.get('operation_mode'),'N/A'), "✓" if task.get('use_visual_analysis') else "✗", ""))
        if selection:
            try: self.tree.selection_set(selection)
            except Exception: pass
        self.tree.yview_moveto(scroll_pos[0])

    def _update_tree_item(self, iid: str, values_dict: Dict[str, str]):
        """Updates an item in the task list.

        Args:
            iid (str): The ID of the item to update.
            values_dict (Dict[str, str]): A dictionary with the values to update.
        """
        if not self.tree.exists(iid): return
        current_values = list(self.tree.item(iid, 'values')); cols = self.tree['columns']
        for col_name, val in values_dict.items():
            if col_name in cols: current_values[cols.index(col_name)] = val
        self.tree.item(iid, values=tuple(current_values))

    def _text_progress_bar(self, p: float, w: int=15) -> str:
        """Creates a text progress bar.

        Args:
            p (float): The percentage of the progress.
            w (int, optional): The width of the progress bar. Defaults to 15.

        Returns:
            str: The text progress bar.
        """
        p=max(0,min(100,p)); f=int(w*p//100); return f"|{'█'*f}{'░'*(w-f)}| {p:.1f}%"

    def _open_presets_manager(self):
        """Opens the presets manager window."""
        PresetsManager(self, self.db, lambda: self.db.get_tasks_by_ids(list(self.tree.selection())))
    def _open_advanced_settings(self):
        """Opens the advanced settings window."""
        AdvancedSettings(self, self.config, self.logger)
