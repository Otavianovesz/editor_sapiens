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
    Coordena a execução das tarefas do pipeline, gerenciando os módulos
    e o fluxo de dados para garantir a estabilidade e a precisão dos resultados.
    """
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.current_renderer: Optional[VideoRenderer] = None
        # O Transcritor é mantido como uma instância persistente para estabilidade.
        self.transcriber = AudioTranscriber(logger, config)

    def interrupt_current_task(self):
        """Interrompe a tarefa de renderização em andamento."""
        if self.current_renderer:
            self.current_renderer.interrupt()

    def _get_module(self, name: str) -> Any:
        """Fábrica para instanciar módulos de processamento sob demanda."""
        if name == 'transcriber':
            return self.transcriber
        # Módulos 'stateless' são criados a cada chamada para garantir a limpeza.
        if name == 'media': return MediaProcessor(self.logger)
        if name == 'visual': return VisualAnalyzer(self.logger, self.config)
        if name == 'content': return ContentAnalyzer(self.logger, self.config)
        if name == 'composer': return ScriptComposer(self.logger)
        if name == 'subtitle_parser': return SubtitleParser(self.logger)
        if name == 'subtitle_generator': return SubtitleGenerator(self.logger)
        raise ValueError(f"Módulo desconhecido solicitado: {name}")

    def run_sapiens_task(self, pq: queue.Queue, task_config: Dict[str, Any], stop_event: threading.Event):
        """
        Executa o pipeline de análise, gerando um roteiro de vídeo (com margens)
        e uma legenda sincronizada (com tempos exatos).
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
                if stop_event.is_set(): raise InterruptedError("Tarefa interrompida.")
                try:
                    words = self._get_module('transcriber').transcribe(audio_path, pq, task_id, stop_event)
                finally:
                    self._get_module('media').cleanup(audio_path, task_id)
                    self._get_module('transcriber').release_gpu_memory()
            else:
                file_path = task_config.get('transcription_path', '')
                if not file_path or not os.path.exists(file_path): raise FileNotFoundError(f"Arquivo de transcrição não encontrado: {file_path}")
                self.logger.log(f"Carregando transcrição de: {os.path.basename(file_path)}", "INFO", task_id)
                _, extension = os.path.splitext(file_path.lower())
                if extension == '.json':
                    with open(file_path, 'r', encoding='utf-8') as f: words = json.load(f)
                elif extension in ['.srt', '.vtt']:
                    words = self._get_module('subtitle_parser').parse(file_path, task_id)
                else: raise ValueError(f"Formato de arquivo de transcrição não suportado: {extension}")

            # --- Etapa 2: Análise de Conteúdo (Dupla Precisão) ---
            if stop_event.is_set(): raise InterruptedError("Tarefa interrompida.")
            if not words: raise RuntimeError("Transcrição não produziu resultados.")

            # O ContentAnalyzer agora retorna duas listas: uma para o vídeo, outra para a legenda.
            padded_segments, exact_segments = self._get_module('content').create_speech_segments(words, pq, task_id, stop_event)
            
            if stop_event.is_set(): raise InterruptedError("Tarefa interrompida.")
            if not padded_segments: raise RuntimeError("Análise de conteúdo não produziu segmentos válidos.")

            # --- Etapa 3: Geração dos Artefatos Finais ---
            
            # Usa os segmentos com PADDING para criar o roteiro do VÍDEO (cortes suaves)
            script_path = self._get_module('composer').generate_and_save_json(padded_segments, video_path, task_id)
            if not script_path: raise RuntimeError("Falha ao salvar o roteiro JSON.")

            # Usa os segmentos EXATOS para criar a LEGENDA (sincronização perfeita)
            try:
                subtitle_path = os.path.splitext(video_path)[0] + "_editado.srt"
                self._get_module('subtitle_generator').generate_srt(words, exact_segments, subtitle_path, task_id)
            except Exception as srt_e:
                self.logger.log(f"Não foi possível gerar a legenda .srt. Erro: {srt_e}", "WARNING", task_id)

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