# -*- coding: utf-8 -*-

import json
import logging

class Config:
    """
    Gerencia as configurações da aplicação, carregando e salvando em um arquivo JSON.
    Define todos os valores padrão para garantir a robustez do programa.
    """
    def __init__(self, config_path='config_sapiens.json'):
        self.config_path = config_path
        self.default_settings = {
            # Configurações de Transcrição (Whisper)
            "whisper_model_size": "large-v3",
            "whisper_language": "pt",
            "whisper_device": "cuda",
            "whisper_compute_type": "float16",

            # Configurações de Análise de Cortes
            "visual_analysis_fps": 5,
            "pause_threshold_s": 0.5,
            "min_segment_duration_s": 0.2,
            "cut_threshold": -7.0,
            "filler_word_context_pause": 0.25,

            # NOVOS PARÂMETROS: Margem de segurança para os cortes
            "segment_padding_start_s": 0.1,  # Tempo a subtrair do início de um segmento
            "segment_padding_end_s": 0.1,    # Tempo a adicionar ao final de um segmento

            # Sistema de Pontuação para Cortes
            "scores": {
                "pause_long": -10,
                "pause_medium": -7,
                "looking_away": -5,
                "gesturing": 8
            },

            # Lista de Palavras de Preenchimento
            "filler_words": [
                "uhm","hum","ahn","é","hã","bem","tipo","aí","daí","então","assim",
                "meio que","né","tá","viu","sabe","entende","certo","ok","beleza",
                "fechou","na verdade","quer dizer","ou seja","basicamente",
                "literalmente","simplesmente","realmente","praticamente",
                "cara","meu","véi","mano","bicho"
            ],

            # Sensibilidade da Análise Visual (Placeholder)
            "gesture_sensitivity_velocity": 0.1,
            "gaze_sensitivity_yaw": 0.8,
            "gaze_sensitivity_pitch": 0.7,

            # Configurações de Renderização
            "render_preset": "medium"
        }
        self.settings = self.default_settings.copy()
        self.load_config()

    def load_config(self):
        """Carrega as configurações do arquivo JSON, ou cria um novo se não existir."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                # Atualiza as configurações padrão com as salvas pelo usuário
                self.settings.update(json.load(f))
        except (FileNotFoundError, json.JSONDecodeError):
            # Se o arquivo não existe ou está corrompido, salva um novo com os padrões
            self.save()

    def save(self):
        """Salva as configurações atuais no arquivo JSON."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            logging.error(f"Falha ao salvar config_sapiens.json: {e}")
            return False

    def get(self, key, default=None):
        """Obtém um valor de configuração de forma segura."""
        return self.settings.get(key, default)

    def set(self, key, value):
        """Define um valor de configuração."""
        self.settings[key] = value