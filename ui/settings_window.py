# -*- coding: utf-8 -*-

import re
import json
import customtkinter as ctk
from tkinter import messagebox

from core.config import Config
from utils.logger import Logger

class AdvancedSettings(ctk.CTkToplevel):
    """Janela para edição detalhada das configurações da aplicação (config_sapiens.json)."""
    def __init__(self, master, config_manager: Config, logger: Logger):
        super().__init__(master)
        self.title("Configurações Avançadas")
        self.config = config_manager
        self.logger = logger
        self.geometry("800x750") # Aumentei um pouco a altura para acomodar os novos sliders
        self.transient(master)
        self.grab_set()
        
        self.widget_vars = {}
        
        container = ctk.CTkScrollableFrame(self)
        container.pack(fill="both", expand=True, padx=10, pady=10)
        
        self._create_all_widgets(container)

        btn_frame = ctk.CTkFrame(self)
        btn_frame.pack(fill="x", padx=10, pady=10)
        ctk.CTkButton(btn_frame, text="Salvar e Fechar", command=self._save_and_close).pack(side="right", padx=5)
        ctk.CTkButton(btn_frame, text="Restaurar Padrões", command=self._restore_defaults).pack(side="right", padx=5)
        ctk.CTkButton(btn_frame, text="Cancelar", command=self.destroy).pack(side="right", padx=5)

    def _create_all_widgets(self, container):
        # --- Seção de Transcrição ---
        transcription_frame = ctk.CTkFrame(container); transcription_frame.pack(fill="x", pady=(5, 10), ipady=10)
        ctk.CTkLabel(transcription_frame, text="Transcrição (Whisper)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_input_widget(transcription_frame, 'whisper_model_size', "Modelo Whisper:", "Modelos maiores são mais precisos, porém mais lentos.", ["tiny", "base", "small", "medium", "large-v2", "large-v3"])
        self._create_input_widget(transcription_frame, 'whisper_language', "Idioma:", "Código do idioma (ex: pt, en). Deixe em branco para detecção automática.")
        self._create_input_widget(transcription_frame, 'whisper_device', "Dispositivo:", "'cuda' para GPU NVIDIA (rápido), 'cpu' para processador (lento).", ["cuda", "cpu"])
        self._create_input_widget(transcription_frame, 'whisper_compute_type', "Tipo de Computação:", "'float16' (padrão), 'int8' (mais rápido, menos VRAM), 'float32' (mais preciso).", ["float16", "int8", "float32"])

        # --- Seção de Análise de Cortes ---
        analysis_frame = ctk.CTkFrame(container); analysis_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(analysis_frame, text="Análise de Cortes", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_slider_with_label(analysis_frame, 'pause_threshold_s', "Sensibilidade à Pausa (s):", 0.1, 2.0, "{:.2f}s", "Tempo mínimo de silêncio para ser considerado uma pausa.")
        self._create_slider_with_label(analysis_frame, 'cut_threshold', "Limite de Corte:", -20.0, 0.0, "{:.1f}", "Nota de corte. Pausas com nota <= a este valor serão cortadas.")
        self._create_slider_with_label(analysis_frame, 'min_segment_duration_s', "Duração Mínima de Segmento (s):", 0.1, 1.0, "{:.2f}s", "Evita 'flashes' de vídeo muito curtos.")
        self._create_slider_with_label(analysis_frame, 'filler_word_context_pause', "Pausa de Contexto (Filler Word):", 0.0, 1.0, "{:.2f}s", "Aumenta a chance de cortar pausas próximas a palavras de preenchimento.")
        
        # --- NOVOS SLIDERS PARA MARGEM DE SEGURANÇA ---
        self._create_slider_with_label(analysis_frame, 'segment_padding_start_s', "Margem Início do Segmento (s):", 0.0, 0.5, "{:.2f}s", "Tempo de segurança adicionado antes do início de um segmento para evitar cortes secos.")
        self._create_slider_with_label(analysis_frame, 'segment_padding_end_s', "Margem Fim do Segmento (s):", 0.0, 0.5, "{:.2f}s", "Tempo de segurança adicionado ao final de um segmento para dar 'respiro' à fala.")

        # --- Seção de Palavras de Preenchimento ---
        filler_frame = ctk.CTkFrame(analysis_frame, fg_color="transparent"); filler_frame.pack(fill="both", padx=10, pady=8, expand=True)
        filler_label = ctk.CTkLabel(filler_frame, text="Palavras de Preenchimento (separadas por vírgula):"); filler_label.pack(anchor="w")
        ctk.CTkToolTip(filler_label, message="Lista de palavras que, quando detectadas, aumentam a chance de corte.")
        self.filler_textbox = ctk.CTkTextbox(filler_frame, height=100); self.filler_textbox.insert("1.0", ", ".join(self.config.get('filler_words', []))); self.filler_textbox.pack(fill="x", expand=True, pady=(0,5))
        self.widget_vars['filler_words'] = (self.filler_textbox, 'list_str')
        
        # --- Seção de Análise Visual ---
        visual_frame = ctk.CTkFrame(container); visual_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(visual_frame, text="Análise Visual e Sensibilidade", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_slider_with_label(visual_frame, 'visual_analysis_fps', "FPS da Análise Visual:", 1, 15, "{:.0f} FPS", "Frames por segundo a serem analisados.", steps=14)
        self._create_slider_with_label(visual_frame, 'gesture_sensitivity_velocity', "Sensibilidade de Gestos:", 0.01, 0.5, "{:.2f}", "Limiar da velocidade do movimento para ser um gesto.")
        self._create_slider_with_label(visual_frame, 'gaze_sensitivity_yaw', "Sensibilidade Olhar Horizontal:", 0.1, 1.5, "{:.2f}", "Sensibilidade para desvios de olhar para os lados.")
        self._create_slider_with_label(visual_frame, 'gaze_sensitivity_pitch', "Sensibilidade Olhar Vertical:", 0.1, 1.5, "{:.2f}", "Sensibilidade para desvios de olhar para cima/baixo.")

        # --- Seção de Pontuação ---
        scores_frame = ctk.CTkFrame(container); scores_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(scores_frame, text="Sistema de Pontuação (Scores)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_score_inputs(scores_frame, self.config.get('scores', {}))

        # --- Seção de Renderização ---
        render_frame = ctk.CTkFrame(container); render_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(render_frame, text="Renderização Final", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_input_widget(render_frame, 'render_preset', "Preset de Renderização:", "Equilíbrio velocidade vs. tamanho/qualidade.", ["ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"])

    def _create_slider_with_label(self, parent, key, text, min_val, max_val, format_str, tooltip_text, steps=None):
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        label = ctk.CTkLabel(frame, text=text, width=250, anchor="w"); label.pack(side="left", padx=(0, 10))
        ctk.CTkToolTip(label, message=tooltip_text)
        var = ctk.DoubleVar(value=self.config.get(key)); self.widget_vars[key] = (var, type(self.config.get(key)))
        value_label = ctk.CTkLabel(frame, text=format_str.format(var.get()), width=60); value_label.pack(side="right", padx=(10, 0))
        def update_label(value): value_label.configure(text=format_str.format(float(value)))
        slider = ctk.CTkSlider(frame, from_=min_val, to=max_val, variable=var, command=update_label, number_of_steps=steps if steps else None)
        slider.pack(side="left", fill="x", expand=True)

    def _create_input_widget(self, parent, key, text, tooltip_text, options=None):
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        label = ctk.CTkLabel(frame, text=text, width=250, anchor="w"); label.pack(side="left", padx=(0, 10))
        ctk.CTkToolTip(label, message=tooltip_text)
        var = ctk.StringVar(value=str(self.config.get(key))); self.widget_vars[key] = (var, 'str')
        widget = ctk.CTkComboBox(frame, variable=var, values=options) if options else ctk.CTkEntry(frame, textvariable=var)
        widget.pack(side="left", fill="x", expand=True)

    def _create_score_inputs(self, parent, scores_dict):
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        tooltip_map = {"pause_long": "Pontuação para pausas longas.", "pause_medium": "Pontuação para pausas médias.", "looking_away": "Penalidade ao desviar o olhar.", "gesturing": "Bônus ao gesticular."}
        for i, (key, value) in enumerate(scores_dict.items()):
            sub_frame = ctk.CTkFrame(frame, fg_color="transparent"); sub_frame.grid(row=i//2, column=i%2, padx=5, pady=2, sticky="ew")
            label = ctk.CTkLabel(sub_frame, text=f"{key}:", width=120); label.pack(side="left")
            if key in tooltip_map: ctk.CTkToolTip(label, message=tooltip_map[key])
            var = ctk.StringVar(value=str(value)); self.widget_vars[f"scores_{key}"] = (var, 'score_int')
            entry = ctk.CTkEntry(sub_frame, textvariable=var, width=80); entry.pack(side="left", padx=5)

    def _restore_defaults(self):
        if not messagebox.askyesno("Restaurar Padrões", "Tem certeza que deseja restaurar todas as configurações para os valores padrão?", parent=self): return
        self.config.settings = self.config.default_settings.copy()
        self.destroy()
        self.master._open_advanced_settings() # Recria a janela com os valores padrão
        self.logger.log("Configurações restauradas para os padrões.", "INFO")

    def _save_and_close(self):
        current_scores = self.config.get('scores', {}).copy()
        try:
            for key, (widget_var, var_type) in self.widget_vars.items():
                if key.startswith("scores_"):
                    current_scores[key.split("_", 1)[1]] = int(widget_var.get())
                elif var_type == 'list_str':
                    text_content = widget_var.get("1.0", "end").strip()
                    self.config.set(key, [word.strip() for word in re.split(r'[,\n]', text_content) if word.strip()])
                elif var_type == 'str':
                     self.config.set(key, widget_var.get())
                else: # float, int, etc.
                    self.config.set(key, var_type(widget_var.get()))

            self.config.set('scores', current_scores)

            if self.config.save():
                self.logger.log("Configurações avançadas salvas.", "INFO")
                self.destroy()
            else:
                messagebox.showerror("Erro", "Não foi possível salvar o arquivo de configuração.", parent=self)

        except (ValueError, json.JSONDecodeError) as e:
            messagebox.showerror("Erro de Valor", f"Um dos valores inseridos é inválido. Por favor, verifique os campos.\n\nDetalhes: {e}", parent=self)
            return