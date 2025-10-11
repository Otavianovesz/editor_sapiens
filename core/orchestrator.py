# core/orchestrator.py

import os
import traceback
import logging # Import necessário para as constantes de nível
from typing import Dict
from .subtitles import TimelineRemapper, SubtitleGenerator

class Orchestrator:
    def __init__(self, config, modules):
        self._modules = modules
        self.config = config
        self.logger = config.logger

    def _get_module(self, name: str):
        return self._modules.get(name)

    def run_sapiens_task(self, pq, task_config: Dict, stop_event):
        task_id = task_config['id']
        video_path = task_config['video_path']
        
        try:
            # --- CHAMADA DE LOG CORRIGIDA ---
            self.logger.info(f"[{task_id}] Iniciando processo Sapiens...")

            # ETAPA 1: Transcrição
            transcriber = self._get_module('transcriber')
            if task_config.get('transcription_mode') == 'file' and os.path.exists(task_config.get('transcription_path', '')):
                self.logger.info(f"[{task_id}] Carregando transcrição do arquivo: {os.path.basename(task_config['transcription_path'])}")
                words = transcriber.load_srt_as_words(task_config['transcription_path'], task_id)
            else:
                self.logger.info(f"[{task_id}] Iniciando extração de áudio e transcrição...")
                audio_path = transcriber.extract_audio(video_path, task_id, stop_event)
                words = transcriber.transcribe_audio(audio_path, task_id, stop_event)
            
            if stop_event.is_set() or not words: raise InterruptedError("Falha na transcrição.")
            self.logger.info(f"[{task_id}] Transcrição concluída.") # SUCCESS é um nível customizado, INFO é mais padrão.
            
            # ETAPA 2: Análise de Conteúdo
            segments = self._get_module('content').create_speech_segments(words, pq, task_config, task_id, stop_event)
            if stop_event.is_set() or not segments: raise InterruptedError("Falha na análise de conteúdo.")
            self.logger.info(f"[{task_id}] Análise de conteúdo e definição de cortes concluída.")
            
            # ETAPA 3: Mapeamento da Linha do Tempo
            self.logger.info(f"[{task_id}] Criando mapa da nova linha do tempo...")
            remapper = TimelineRemapper(segments)

            # ETAPA 4: Geração do Roteiro para o Renderizador
            composer = self._get_module('composer')
            script_path = composer.generate_and_save_json(segments, video_path, task_id)
            if not script_path: raise RuntimeError("Falha ao salvar o roteiro de edição.")
            self.logger.info(f"[{task_id}] Roteiro de edição salvo em '{os.path.basename(script_path)}'.")

            # ETAPA 5: Geração das Legendas Sincronizadas
            self.logger.info(f"[{task_id}] Gerando legendas sincronizadas para o vídeo editado...")
            subtitle_generator = SubtitleGenerator(remapper, self.logger, task_id)
            srt_path = subtitle_generator.generate_srt(words, video_path)
            
            if srt_path:
                self.logger.info(f"[{task_id}] Legendas sincronizadas salvas em '{os.path.basename(srt_path)}'")
            else:
                self.logger.warning(f"[{task_id}] Não foi possível gerar o arquivo de legendas.")

            # ETAPA 6: Notificação para a UI
            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError as e:
            self.logger.warning(f"[{task_id}] Processo interrompido pelo usuário ou por uma falha: {e}")
            pq.put({'type': 'task_failed', 'task_id': task_id, 'reason': str(e)})
        except Exception as e:
            self.logger.critical(f"[{task_id}] Erro inesperado no orchestrator: {e}\n{traceback.format_exc()}")
            pq.put({'type': 'task_failed', 'task_id': task_id, 'reason': str(e)})