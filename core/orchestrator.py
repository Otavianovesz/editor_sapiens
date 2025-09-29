# -*- coding: utf-8 -*-

import os
import json
import logging
import queue
import threading
from typing import Optional

from .config import Config
from .renderer import VideoRenderer
from .processing_modules import (
    MediaProcessor,
    AudioTranscriber,
    VisualAnalyzer,
    ContentAnalyzer,
    ScriptComposer
)
from utils.logger import Logger

class Orchestrator:
    """
    Coordena a execução das tarefas, seja o pipeline completo do Sapiens
    ou apenas a renderização, utilizando os módulos de processamento.
    """
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.current_renderer: Optional[VideoRenderer] = None

    def interrupt_current_task(self):
        """Interrompe a tarefa de renderização em andamento, se houver."""
        if self.current_renderer:
            self.current_renderer.interrupt()

    def _get_module(self, name):
        """Fábrica para instanciar módulos de processamento sob demanda."""
        if name == 'media': return MediaProcessor(self.logger)
        if name == 'transcriber': return AudioTranscriber(self.logger, self.config)
        if name == 'visual': return VisualAnalyzer(self.logger, self.config)
        if name == 'content': return ContentAnalyzer(self.logger, self.config)
        if name == 'composer': return ScriptComposer(self.logger)
        return None

    def run_sapiens_task(self, pq: queue.Queue, task_config: dict, stop_event: threading.Event):
        """Executa o pipeline completo de análise e geração de roteiro."""
        task_id = task_config['id']
        self.logger.log(f"Iniciando pipeline 'Sapiens' para '{os.path.basename(task_config['video_path'])}'.", "INFO", task_id)
        try:
            if task_config.get('transcription_mode') == 'whisper':
                # Pipeline de transcrição com Whisper
                audio_path = self._get_module('media').extract_audio(task_config['video_path'], task_id)
                if not audio_path: raise RuntimeError("Extração de áudio falhou.")
                words = self._get_module('transcriber').transcribe(audio_path, pq, task_id, stop_event)
                self._get_module('media').cleanup(audio_path, task_id)
            else:
                # Carrega transcrição de um arquivo JSON externo
                file_path = task_config.get('transcription_path', '')
                self.logger.log(f"Carregando transcrição do arquivo: {file_path}", "INFO", task_id)
                with open(file_path, 'r', encoding='utf-8') as f: words = json.load(f)

            if stop_event.is_set() or not words: raise InterruptedError("Falha na transcrição.")

            visual_data = self._get_module('visual').analyze_video_in_single_pass(task_config['video_path'], pq, task_id, stop_event) if task_config.get('use_visual_analysis') else None
            segments = self._get_module('content').create_speech_segments(words, pq, task_config.get('use_visual_analysis'), visual_data, task_id, stop_event)
            
            if stop_event.is_set() or not segments: raise InterruptedError("Falha na análise.")
            
            script_path = self._get_module('composer').generate_and_save_json(segments, task_config['video_path'], task_id)
            if not script_path: raise RuntimeError("Falha ao salvar roteiro.")
            
            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError:
            pq.put({'type': 'interrupted', 'task_id': task_id})
        except Exception as e:
            logging.error(f"ERRO no pipeline Sapiens: {e}", exc_info=True)
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})

    def run_render_task(self, pq: queue.Queue, task_config: dict, stop_event: threading.Event):
        """Executa apenas a tarefa de renderização de vídeo a partir de um roteiro."""
        task_id = task_config['id']
        output_path = os.path.splitext(task_config['video_path'])[0] + "_editado.mp4"
        renderer = VideoRenderer(
            source_path=task_config['video_path'], 
            json_path=task_config.get('render_script_path', ''), 
            output_path=output_path, 
            preset=self.config.get("render_preset"), 
            logger=self.logger, 
            pq=pq, 
            task_id=task_id
        )
        self.current_renderer = renderer
        renderer.run(stop_event)
        self.current_renderer = None
