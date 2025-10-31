# -*- coding: utf-8 -*-

import re
import json
import customtkinter as ctk
from tkinter import messagebox

from core.config import Config
from utils.logger import Logger

class AdvancedSettings(ctk.CTkToplevel):
    """Window for detailed editing of the application's settings (config_sapiens.json)."""
    def __init__(self, master, config_manager: Config, logger: Logger):
        """Initializes the AdvancedSettings window.

        Args:
            master: The parent window.
            config_manager (Config): The configuration manager instance.
            logger (Logger): The logger instance.
        """
        super().__init__(master)
        self.title("Advanced Settings")
        self.config = config_manager
        self.logger = logger
        self.geometry("800x750") # I increased the height a little to accommodate the new sliders
        self.transient(master)
        self.grab_set()
        
        self.widget_vars = {}
        
        container = ctk.CTkScrollableFrame(self)
        container.pack(fill="both", expand=True, padx=10, pady=10)
        
        self._create_all_widgets(container)

        btn_frame = ctk.CTkFrame(self)
        btn_frame.pack(fill="x", padx=10, pady=10)
        ctk.CTkButton(btn_frame, text="Save and Close", command=self._save_and_close).pack(side="right", padx=5)
        ctk.CTkButton(btn_frame, text="Restore Defaults", command=self._restore_defaults).pack(side="right", padx=5)
        ctk.CTkButton(btn_frame, text="Cancel", command=self.destroy).pack(side="right", padx=5)

    def _create_all_widgets(self, container):
        """Creates all the widgets for the window.

        Args:
            container: The parent widget.
        """
        # --- Transcription Section ---
        transcription_frame = ctk.CTkFrame(container); transcription_frame.pack(fill="x", pady=(5, 10), ipady=10)
        ctk.CTkLabel(transcription_frame, text="Transcription (Whisper)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_input_widget(transcription_frame, 'whisper_model_size', "Whisper Model:", "Larger models are more accurate, but slower.", ["tiny", "base", "small", "medium", "large-v2", "large-v3"])
        self._create_input_widget(transcription_frame, 'whisper_language', "Language:", "Language code (e.g., pt, en). Leave blank for automatic detection.")
        self._create_input_widget(transcription_frame, 'whisper_device', "Device:", "'cuda' for NVIDIA GPU (fast), 'cpu' for processor (slow).", ["cuda", "cpu"])
        self._create_input_widget(transcription_frame, 'whisper_compute_type', "Computation Type:", "'float16' (default), 'int8' (faster, less VRAM), 'float32' (more accurate).", ["float16", "int8", "float32"])

        # --- Cut Analysis Section ---
        analysis_frame = ctk.CTkFrame(container); analysis_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(analysis_frame, text="Cut Analysis", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_slider_with_label(analysis_frame, 'pause_threshold_s', "Pause Sensitivity (s):", 0.1, 2.0, "{:.2f}s", "Minimum silence time to be considered a pause.")
        self._create_slider_with_label(analysis_frame, 'cut_threshold', "Cut Threshold:", -20.0, 0.0, "{:.1f}", "Cut-off score. Pauses with a score <= this value will be cut.")
        self._create_slider_with_label(analysis_frame, 'min_segment_duration_s', "Minimum Segment Duration (s):", 0.1, 1.0, "{:.2f}s", "Avoids very short video 'flashes'.")
        self._create_slider_with_label(analysis_frame, 'filler_word_context_pause', "Context Pause (Filler Word):", 0.0, 1.0, "{:.2f}s", "Increases the chance of cutting pauses close to filler words.")
        
        # --- NEW SLIDERS FOR SAFETY MARGIN ---
        self._create_slider_with_label(analysis_frame, 'segment_padding_start_s', "Segment Start Margin (s):", 0.0, 0.5, "{:.2f}s", "Safety time added before the start of a segment to avoid abrupt cuts.")
        self._create_slider_with_label(analysis_frame, 'segment_padding_end_s', "Segment End Margin (s):", 0.0, 0.5, "{:.2f}s", "Safety time added to the end of a segment to give 'breathing room' to the speech.")

        # --- Filler Words Section ---
        filler_frame = ctk.CTkFrame(analysis_frame, fg_color="transparent"); filler_frame.pack(fill="both", padx=10, pady=8, expand=True)
        filler_label = ctk.CTkLabel(filler_frame, text="Filler Words (separated by comma):"); filler_label.pack(anchor="w")
        ctk.CTkToolTip(filler_label, message="List of words that, when detected, increase the chance of a cut.")
        self.filler_textbox = ctk.CTkTextbox(filler_frame, height=100); self.filler_textbox.insert("1.0", ", ".join(self.config.get('filler_words', []))); self.filler_textbox.pack(fill="x", expand=True, pady=(0,5))
        self.widget_vars['filler_words'] = (self.filler_textbox, 'list_str')
        
        # --- Visual Analysis Section ---
        visual_frame = ctk.CTkFrame(container); visual_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(visual_frame, text="Visual Analysis and Sensitivity", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_slider_with_label(visual_frame, 'visual_analysis_fps', "Visual Analysis FPS:", 1, 15, "{:.0f} FPS", "Frames per second to be analyzed.", steps=14)
        self._create_slider_with_label(visual_frame, 'gesture_sensitivity_velocity', "Gesture Sensitivity:", 0.01, 0.5, "{:.2f}", "Movement speed threshold to be a gesture.")
        self._create_slider_with_label(visual_frame, 'gaze_sensitivity_yaw', "Horizontal Gaze Sensitivity:", 0.1, 1.5, "{:.2f}", "Sensitivity to sideways gaze deviations.")
        self._create_slider_with_label(visual_frame, 'gaze_sensitivity_pitch', "Vertical Gaze Sensitivity:", 0.1, 1.5, "{:.2f}", "Sensitivity to upward/downward gaze deviations.")

        # --- Scoring System Section ---
        scores_frame = ctk.CTkFrame(container); scores_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(scores_frame, text="Scoring System (Scores)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_score_inputs(scores_frame, self.config.get('scores', {}))

        # --- Final Rendering Section ---
        render_frame = ctk.CTkFrame(container); render_frame.pack(fill="x", pady=10, ipady=10)
        ctk.CTkLabel(render_frame, text="Final Rendering", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        self._create_input_widget(render_frame, 'render_preset', "Rendering Preset:", "Balance between speed and size/quality.", ["ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"])

    def _create_slider_with_label(self, parent, key, text, min_val, max_val, format_str, tooltip_text, steps=None):
        """Creates a slider with a label.

        Args:
            parent: The parent widget.
            key (str): The configuration key.
            text (str): The label text.
            min_val (float): The minimum value of the slider.
            max_val (float): The maximum value of the slider.
            format_str (str): The format string for the value label.
            tooltip_text (str): The tooltip text.
            steps (int, optional): The number of steps for the slider. Defaults to None.
        """
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        label = ctk.CTkLabel(frame, text=text, width=250, anchor="w"); label.pack(side="left", padx=(0, 10))
        ctk.CTkToolTip(label, message=tooltip_text)
        var = ctk.DoubleVar(value=self.config.get(key)); self.widget_vars[key] = (var, type(self.config.get(key)))
        value_label = ctk.CTkLabel(frame, text=format_str.format(var.get()), width=60); value_label.pack(side="right", padx=(10, 0))
        def update_label(value): value_label.configure(text=format_str.format(float(value)))
        slider = ctk.CTkSlider(frame, from_=min_val, to=max_val, variable=var, command=update_label, number_of_steps=steps if steps else None)
        slider.pack(side="left", fill="x", expand=True)

    def _create_input_widget(self, parent, key, text, tooltip_text, options=None):
        """Creates an input widget.

        Args:
            parent: The parent widget.
            key (str): The configuration key.
            text (str): The label text.
            tooltip_text (str): The tooltip text.
            options (list, optional): A list of options for the combobox. Defaults to None.
        """
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        label = ctk.CTkLabel(frame, text=text, width=250, anchor="w"); label.pack(side="left", padx=(0, 10))
        ctk.CTkToolTip(label, message=tooltip_text)
        var = ctk.StringVar(value=str(self.config.get(key))); self.widget_vars[key] = (var, 'str')
        widget = ctk.CTkComboBox(frame, variable=var, values=options) if options else ctk.CTkEntry(frame, textvariable=var)
        widget.pack(side="left", fill="x", expand=True)

    def _create_score_inputs(self, parent, scores_dict):
        """Creates the score inputs.

        Args:
            parent: The parent widget.
            scores_dict (dict): A dictionary with the scores.
        """
        frame = ctk.CTkFrame(parent, fg_color="transparent"); frame.pack(fill="x", padx=10, pady=8)
        tooltip_map = {"pause_long": "Score for long pauses.", "pause_medium": "Score for medium pauses.", "looking_away": "Penalty for looking away.", "gesturing": "Bonus for gesturing."}
        for i, (key, value) in enumerate(scores_dict.items()):
            sub_frame = ctk.CTkFrame(frame, fg_color="transparent"); sub_frame.grid(row=i//2, column=i%2, padx=5, pady=2, sticky="ew")
            label = ctk.CTkLabel(sub_frame, text=f"{key}:", width=120); label.pack(side="left")
            if key in tooltip_map: ctk.CTkToolTip(label, message=tooltip_map[key])
            var = ctk.StringVar(value=str(value)); self.widget_vars[f"scores_{key}"] = (var, 'score_int')
            entry = ctk.CTkEntry(sub_frame, textvariable=var, width=80); entry.pack(side="left", padx=5)

    def _restore_defaults(self):
        """Restores the default settings."""
        if not messagebox.askyesno("Restore Defaults", "Are you sure you want to restore all settings to their default values?", parent=self): return
        self.config.settings = self.config.default_settings.copy()
        self.destroy()
        self.master._open_advanced_settings() # Recreates the window with the default values
        self.logger.log("Settings restored to defaults.", "INFO")

    def _save_and_close(self):
        """Saves the settings and closes the window."""
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
                self.logger.log("Advanced settings saved.", "INFO")
                self.destroy()
            else:
                messagebox.showerror("Error", "Could not save the configuration file.", parent=self)

        except (ValueError, json.JSONDecodeError) as e:
            messagebox.showerror("Value Error", f"One of the entered values is invalid. Please check the fields.\n\nDetails: {e}", parent=self)
            return
