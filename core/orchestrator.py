# core/orchestrator.py

import os
import traceback
import logging
import json
import tempfile
from typing import Dict
from .subtitles import TimelineRemapper, SubtitleGenerator
from .renderer import VideoRenderer
from core.exceptions import InterruptedError

class Orchestrator:
    def __init__(self, logger, config, modules):
        self._modules = modules
        self.config = config
        self.logger = logger
        self.current_renderer = None

    def _get_module(self, name: str):
        return self._modules.get(name)

    def interrupt_current_task(self):
        # Esta função pode ser usada para interrupções mais complexas no futuro
        pass

    def run_sapiens_task(self, pq, task_config: Dict, stop_event):
        task_id = task_config['id']
        video_path = task_config['video_path']
        
        try:
            self.logger.info(f"[{task_id}] Iniciando processo Sapiens...")

            # ETAPA 1: Transcrição
            if task_config.get('transcription_mode') == 'file' and os.path.exists(task_config.get('transcription_path', '')):
                self.logger.info(f"[{task_id}] Carregando transcrição do arquivo: {os.path.basename(task_config['transcription_path'])}")
                words = self._get_module('parser').parse(task_config['transcription_path'], task_id)
            else:
                self.logger.info(f"[{task_id}] Iniciando extração de áudio e transcrição...")
                media_processor = self._get_module('media_processor')
                transcriber = self._get_module('transcriber')
                audio_path = media_processor.extract_audio(task_config['video_path'], task_id)
                words = transcriber.transcribe(audio_path, pq, task_id, stop_event)
                media_processor.cleanup(audio_path, task_id)
            
            if stop_event.is_set() or not words: raise InterruptedError("Falha na transcrição.")
            self.logger.info(f"[{task_id}] Transcrição concluída.")
            
            # ETAPA 2: Análise de Conteúdo
            content_analyzer = self._get_module('content')
            segments = content_analyzer.create_speech_segments(words, pq, task_config, task_id, stop_event)
            if stop_event.is_set() or not segments: raise InterruptedError("Falha na análise de conteúdo.")
            self.logger.info(f"[{task_id}] Análise de conteúdo e definição de cortes concluída.")
            
            # ETAPA 3: Mapeamento da Linha do Tempo e Roteiro
            remapper = TimelineRemapper(segments)
            composer = self._get_module('composer')
            script_path = composer.generate_and_save_json(segments, video_path, task_id)
            if not script_path: raise RuntimeError("Falha ao salvar o roteiro de edição.")
            self.logger.info(f"[{task_id}] Roteiro de edição salvo em '{os.path.basename(script_path)}'.")

            # ETAPA 4: Geração das Legendas Sincronizadas
            self.logger.info(f"[{task_id}] Gerando legendas sincronizadas para o vídeo editado...")
            subtitle_generator = SubtitleGenerator(remapper, self.logger, task_id)
            srt_path = subtitle_generator.generate_srt(words, video_path)
            
            if srt_path:
                self.logger.info(f"[{task_id}] Legendas sincronizadas salvas em '{os.path.basename(srt_path)}'")
            else:
                self.logger.warning(f"[{task_id}] Não foi possível gerar o arquivo de legendas.")

            # ETAPA 5: Notificação para a UI
            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError as e:
            self.logger.warning(f"[{task_id}] Processo interrompido: {e}")
            pq.put({'type': 'interrupted', 'message': str(e), 'task_id': task_id})
        except Exception as e:
            self.logger.critical(f"[{task_id}] Erro inesperado no orchestrator: {e}\n{traceback.format_exc()}")
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})

    # --- MÉTODO ADICIONADO ---
    def run_render_task(self, pq, task_config: Dict, stop_event):
        """
        Executa apenas a parte de renderização do vídeo, usando um roteiro JSON.
        """
        task_id = task_config['id']
        script_path = task_config.get('render_script_path')
        video_path = task_config.get('video_path')

        try:
            self.logger.info(f"[{task_id}] Iniciando tarefa de renderização...")
            if not script_path or not os.path.exists(script_path):
                raise FileNotFoundError(f"Arquivo de roteiro '{script_path}' não encontrado.")
            
            with open(script_path, 'r', encoding='utf-8') as f:
                script_data = json.load(f)
            segments = script_data.get("segments")
            if not segments:
                raise ValueError("Roteiro não contém a chave 'segments' ou ela está vazia.")

            output_path = os.path.splitext(video_path)[0] + "_editado.mp4"
            render_preset = self.config.get("render_preset", "medium")

            # Cria um diretório temporário para os clipes
            with tempfile.TemporaryDirectory(prefix="sapiens_") as temp_dir:
                self.logger.info(f"[{task_id}] Usando diretório temporário: {temp_dir}")

                renderer = VideoRenderer(
                    source_path=video_path,
                    output_path=output_path,
                    preset=render_preset,
                    logger=self.logger,
                    task_id=task_id
                )
                
                # A renderização é um processo de longa duração que precisa ser monitorado
                renderer.render_video(segments, temp_dir)

            if stop_event.is_set():
                raise InterruptedError("Renderização interrompida pelo usuário.")

            self.logger.info(f"[{task_id}] Renderização concluída com sucesso.")
            pq.put({'type': 'done', 'task_id': task_id})

        except InterruptedError as e:
            self.logger.warning(f"[{task_id}] Processo de renderização interrompido: {e}")
            pq.put({'type': 'interrupted', 'message': str(e), 'task_id': task_id})
        except Exception as e:
            self.logger.critical(f"[{task_id}] Erro inesperado na renderização: {e}\n{traceback.format_exc()}")
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})