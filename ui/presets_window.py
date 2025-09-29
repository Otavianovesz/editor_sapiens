# -*- coding: utf-8 -*-

import json
import customtkinter as ctk
from tkinter import messagebox
from typing import Callable, List, Dict, Any

from core.database import DatabaseManager

class PresetsManager(ctk.CTkToplevel):
    """
    Janela para criar, visualizar, editar, excluir e aplicar presets de configuração
    às tarefas selecionadas.
    """
    def __init__(self, master, db: DatabaseManager, get_selected_tasks_func: Callable[[], List[Dict[str, Any]]]):
        super().__init__(master)
        self.title("Gerenciador de Presets")
        self.db = db
        self.get_selected_tasks = get_selected_tasks_func
        self.geometry("800x500")
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=2)
        self.grid_rowconfigure(0, weight=1)
        self.selected_preset_name = None
        self._create_widgets()
        self._load_presets()
        self.transient(master)
        self.grab_set()


    def _create_widgets(self):
        left_frame = ctk.CTkFrame(self)
        left_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        left_frame.grid_rowconfigure(1, weight=1)
        ctk.CTkLabel(left_frame, text="Presets Salvos", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=10)
        self.presets_list_frame = ctk.CTkScrollableFrame(left_frame, fg_color="transparent")
        self.presets_list_frame.grid(row=1, column=0, sticky="nsew", padx=5)

        right_frame = ctk.CTkFrame(self)
        right_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        right_frame.grid_rowconfigure(2, weight=1)
        right_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(right_frame, text="Configuração do Preset", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky="w")
        self.name_entry = ctk.CTkEntry(right_frame, placeholder_text="Nome do Preset")
        self.name_entry.grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="ew")
        self.config_textbox = ctk.CTkTextbox(right_frame, font=("Consolas", 12))
        self.config_textbox.grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="nsew")

        btn_frame = ctk.CTkFrame(right_frame, fg_color="transparent")
        btn_frame.grid(row=3, column=0, columnspan=2, pady=10)
        ctk.CTkButton(btn_frame, text="➕ Novo", command=self._new_preset).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="💾 Salvar", command=self._save_preset).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="❌ Excluir", command=self._delete_preset).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="➡️ Aplicar às Tarefas", command=self._apply_preset).pack(side="left", padx=5)

    def _load_presets(self):
        for widget in self.presets_list_frame.winfo_children():
            widget.destroy()
        presets = self.db.get_all_presets()
        for preset in presets:
            btn = ctk.CTkButton(self.presets_list_frame, text=preset['name'], fg_color="transparent", anchor="w", command=lambda p=preset: self._select_preset(p))
            btn.pack(fill="x", pady=2)

    def _select_preset(self, preset):
        self.selected_preset_name = preset['name']
        self.name_entry.delete(0, "end")
        self.name_entry.insert(0, preset['name'])
        self.config_textbox.delete("1.0", "end")
        self.config_textbox.insert("1.0", json.dumps(preset['config'], indent=4, ensure_ascii=False))

    def _new_preset(self):
        self.selected_preset_name = None
        self.name_entry.delete(0, "end")
        self.config_textbox.delete("1.0", "end")
        tasks = self.get_selected_tasks()
        if tasks:
            base_config = {k:v for k,v in tasks[0].items() if k in ['operation_mode', 'use_visual_analysis', 'transcription_mode']}
            self.config_textbox.insert("1.0", json.dumps(base_config, indent=4))
            messagebox.showinfo("Novo Preset", "Configuração baseada na primeira tarefa selecionada foi carregada.", parent=self)

    def _save_preset(self):
        name = self.name_entry.get().strip()
        if not name:
            messagebox.showerror("Erro", "O nome do preset não pode ser vazio.", parent=self)
            return
        try:
            config = json.loads(self.config_textbox.get("1.0", "end"))
        except json.JSONDecodeError:
            messagebox.showerror("Erro", "O JSON da configuração é inválido.", parent=self)
            return
        self.db.save_preset(name, config)
        self._load_presets()
        self.master.logger.log(f"Preset '{name}' salvo.", "INFO")

    def _delete_preset(self):
        if self.selected_preset_name and messagebox.askyesno("Confirmar", f"Excluir o preset '{self.selected_preset_name}'?", parent=self):
            self.db.delete_preset(self.selected_preset_name)
            self._new_preset()
            self._load_presets()
            self.master.logger.log(f"Preset '{self.selected_preset_name}' excluído.", "INFO")

    def _apply_preset(self):
        tasks = self.get_selected_tasks()
        if not tasks:
            messagebox.showwarning("Aviso", "Nenhuma tarefa selecionada para aplicar o preset.", parent=self)
            return
        try:
            config = json.loads(self.config_textbox.get("1.0", "end"))
        except json.JSONDecodeError:
            messagebox.showerror("Erro", "O JSON da configuração é inválido.", parent=self)
            return
            
        for i, task in enumerate(tasks):
            is_last = (i == len(tasks) - 1)
            self.master.db.update_task_config(task['id'], config, wait=is_last)
            
        self.master.after(100, self.master._load_and_display_queue)
        self.master.logger.log(f"Preset '{self.name_entry.get()}' aplicado a {len(tasks)} tarefa(s).", "INFO")
        messagebox.showinfo("Sucesso", f"Preset aplicado a {len(tasks)} tarefa(s).", parent=self)
