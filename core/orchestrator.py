# -*- coding: utf-8 -*-

import os
import json
import logging
import queue
import threading
from typing import Optional, Dict, Any

from .config import Config
from .renderer import VideoRenderer
from .processing_modules import (
    MediaProcessor,
    AudioTranscriber,
    VisualAnalyzer,
    ContentAnalyzer,
    ScriptComposer,
    SubtitleParser,
    SubtitleGenerator
)
from utils.logger import Logger

class Orchestrator:
    """
    Coordena a execução das tarefas do pipeline, agora utilizando um AudioTranscriber
    no padrão Singleton para garantir estabilidade e performance.
    """
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.current_renderer: Optional[VideoRenderer] = None
        # O AudioTranscriber é o único módulo que precisa ser persistente.
        self.transcriber = AudioTranscriber(logger, config)

    def interrupt_current_task(self):
        """Interrompe a tarefa de renderização em andamento, se houver."""
        if self.current_renderer:
            self.current_renderer.interrupt()

    def _get_module(self, name: str) -> Any:
        """
        Fábrica para instanciar módulos de processamento sob demanda.
        O Transcritor é tratado como um caso especial para reutilizar a instância Singleton.
        """
        if name == 'transcriber':
            return self.transcriber
        if name == 'media':
            return MediaProcessor(self.logger)
        if name == 'visual':
            return VisualAnalyzer(self.logger, self.config)
        if name == 'content':
            return ContentAnalyzer(self.logger, self.config)
        if name == 'composer':
            return ScriptComposer(self.logger)
        if name == 'subtitle_parser':
            return SubtitleParser(self.logger)
        if name == 'subtitle_generator':
            return SubtitleGenerator(self.logger)
        
        raise ValueError(f"Módulo desconhecido solicitado: {name}")

    def run_sapiens_task(self, pq: queue.Queue, task_config: Dict[str, Any], stop_event: threading.Event):
        """
        Executa o pipeline completo de análise e geração de roteiro e legenda.
        """
        task_id = task_config['id']
        video_path = task_config['video_path']
        self.logger.log(f"Iniciando pipeline 'Sapiens' para '{os.path.basename(video_path)}'.", "INFO", task_id)

        try:
            words = []
            # --- Etapa 1: Obtenção da Transcrição ---
            if task_config.get('transcription_mode') == 'whisper':
                audio_path = self._get_module('media').extract_audio(video_path, task_id)
                if not audio_path: raise RuntimeError("Extração de áudio falhou.")
                if stop_event.is_set(): raise InterruptedError("Tarefa interrompida durante a extração de áudio.")

                try:
                    words = self._get_module('transcriber').transcribe(audio_path, pq, task_id, stop_event)
                finally:
                    # Garante a limpeza dos arquivos e da memória da GPU mesmo se a transcrição falhar.
                    self._get_module('media').cleanup(audio_path, task_id)
                    self._get_module('transcriber').release_gpu_memory()

            else: # Usar arquivo externo
                file_path = task_config.get('transcription_path', '')
                if not file_path or not os.path.exists(file_path):
                    raise FileNotFoundError(f"Arquivo de transcrição externa não encontrado: {file_path}")

                self.logger.log(f"Carregando transcrição do arquivo: {os.path.basename(file_path)}", "INFO", task_id)
                _, extension = os.path.splitext(file_path.lower())

                if extension == '.json':
                    with open(file_path, 'r', encoding='utf-8') as f: words = json.load(f)
                elif extension in ['.srt', '.vtt']:
                    words = self._get_module('subtitle_parser').parse(file_path, task_id)
                else:
                    raise ValueError(f"Formato de arquivo de transcrição não suportado: {extension}")

            # --- Etapa 2: Validação e Análise de Conteúdo ---
            if stop_event.is_set(): raise InterruptedError("Tarefa interrompida após a etapa de transcrição.")
            if not words: raise RuntimeError("A etapa de transcrição não produziu resultados.")

            visual_data = None
            if task_config.get('use_visual_analysis'):
                 visual_data = self._get_module('visual').analyze_video_in_single_pass(video_path, pq, task_id, stop_event)
            if stop_event.is_set(): raise InterruptedError("Tarefa interrompida durante a análise visual.")

            segments = self._get_module('content').create_speech_segments(words, pq, task_config.get('use_visual_analysis'), visual_data, task_id, stop_event)
            if stop_event.is_set(): raise InterruptedError("Tarefa interrompida durante a análise de conteúdo.")
            if not segments: raise RuntimeError("A análise de conteúdo não produziu segmentos válidos.")

            # --- Etapa 3: Geração dos Artefatos Finais ---
            script_path = self._get_module('composer').generate_and_save_json(segments, video_path, task_id)
            if not script_path: raise RuntimeError("Falha crítica ao salvar o arquivo de roteiro JSON.")

            try:
                subtitle_path = os.path.splitext(video_path)[0] + "_editado.srt"
                self._get_module('subtitle_generator').generate_srt(words, segments, subtitle_path, task_id)
            except Exception as srt_e:
                self.logger.log(f"Não foi possível gerar o arquivo de legenda .srt. Erro: {srt_e}", "WARNING", task_id)

            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError:
            self.logger.log("Pipeline Sapiens interrompido pelo usuário.", "WARNING", task_id)
            pq.put({'type': 'interrupted', 'task_id': task_id})
        except Exception as e:
            logging.error(f"ERRO no pipeline Sapiens: {e}", exc_info=True)
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})

    def run_render_task(self, pq: queue.Queue, task_config: Dict[str, Any], stop_event: threading.Event):
        """Executa apenas a tarefa de renderização de vídeo a partir de um roteiro."""
        task_id = task_config['id']
        output_path = os.path.splitext(task_config['video_path'])[0] + "_editado.mp4"

        try:
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
        except Exception as e:
            logging.error(f"ERRO ao iniciar a tarefa de renderização: {e}", exc_info=True)
            pq.put({'type': 'error', 'message': f"Falha na inicialização da renderização: {e}", 'task_id': task_id})
        finally:
            self.current_renderer = None