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

from contextlib import contextmanager
from typing import Dict, Any, Generator
from .exceptions import ResourceError, ProcessingError, ValidationError, StateError, InterruptedError

class Orchestrator:
    """
    Coordena a execução das tarefas, seja o pipeline completo do Sapiens
    ou apenas a renderização, utilizando os módulos de processamento.
    """
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.current_renderer: Optional[VideoRenderer] = None
        self._active_resources: Dict[str, Any] = {}
        self._validate_config()
        
    def _validate_config(self) -> None:
        """Valida a configuração inicial."""
        required_keys = ['whisper_model_size', 'whisper_device', 'whisper_compute_type']
        missing = [k for k in required_keys if not self.config.get(k)]
        if missing:
            raise ValidationError(f"Configuração incompleta. Faltam: {', '.join(missing)}")
            
    @contextmanager
    def _manage_resource(self, resource_id: str, resource: Any) -> Generator[Any, None, None]:
        """Gerencia o ciclo de vida de um recurso."""
        try:
            self._active_resources[resource_id] = resource
            yield resource
        finally:
            self._active_resources.pop(resource_id, None)
            if hasattr(resource, 'cleanup'):
                try:
                    resource.cleanup()
                except Exception as e:
                    self.logger.log(f"Erro ao limpar recurso {resource_id}: {e}", "WARNING")

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

    def _validate_task_config(self, task_config: dict) -> None:
        """Valida a configuração da tarefa."""
        required = ['id', 'video_path', 'operation_mode']
        if not all(k in task_config for k in required):
            raise ValidationError(f"Configuração de tarefa inválida. Campos obrigatórios: {required}")
            
        if not os.path.exists(task_config['video_path']):
            raise ResourceError(f"Arquivo de vídeo não encontrado: {task_config['video_path']}")
            
        valid_modes = ['whisper', 'file']
        if task_config.get('transcription_mode') not in valid_modes:
            raise ValidationError(f"Modo de transcrição inválido. Use: {valid_modes}")

    def run_sapiens_task(self, pq: queue.Queue, task_config: dict, stop_event: threading.Event):
        """Executa o pipeline completo de análise e geração de roteiro."""
        task_id = task_config['id']
        
        try:
            self._validate_task_config(task_config)
            self.logger.log(f"Iniciando pipeline 'Sapiens' para '{os.path.basename(task_config['video_path'])}'.", "INFO", task_id)
            if task_config.get('transcription_mode') == 'whisper':
                # Pipeline de transcrição com Whisper
                media_processor = self._get_module('media')
                transcriber = self._get_module('transcriber')
                
                with self._manage_resource('media_processor', media_processor):
                    audio_path = media_processor.extract_audio(task_config['video_path'], task_id)
                    if not audio_path:
                        raise ResourceError("Falha na extração de áudio")
                        
                    try:
                        with self._manage_resource('transcriber', transcriber):
                            words = transcriber.transcribe(audio_path, pq, task_id, stop_event)
                    finally:
                        # Garante limpeza do arquivo temporário mesmo em caso de erro
                        media_processor.cleanup(audio_path, task_id)
                        
                if not words:
                    raise ProcessingError("Transcrição não produziu resultados")
                    
            else:
                # Carrega transcrição de um arquivo JSON externo
                file_path = task_config.get('transcription_path', '')
                if not os.path.exists(file_path):
                    raise ResourceError(f"Arquivo de transcrição não encontrado: {file_path}")
                    
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        words = json.load(f)
                except json.JSONDecodeError as e:
                    raise ValidationError(f"Arquivo de transcrição inválido: {e}")
                except Exception as e:
                    raise ResourceError(f"Erro ao ler arquivo de transcrição: {e}")

            if stop_event.is_set():
                raise InterruptedError("Processo interrompido pelo usuário.")

            if not words:
                raise RuntimeError("A transcrição falhou - nenhuma palavra encontrada.")

            self.logger.log("Iniciando etapa de análise...", "INFO", task_id)

            # Gerenciamento da análise visual
            visual_data = []
            
            if task_config.get('use_visual_analysis'):
                self.logger.log("Iniciando análise visual...", "INFO", task_id)
                visual_analyzer = self._get_module('visual')
                
                with self._manage_resource('visual_analyzer', visual_analyzer):
                    try:
                        visual_data = visual_analyzer.analyze_video_in_single_pass(
                            task_config['video_path'], pq, task_id, stop_event)
                            
                        if not visual_data:
                            self.logger.log(
                                "Análise visual não produziu dados. Usando lista vazia.", 
                                "WARNING", task_id
                            )
                            visual_data = []
                            
                    except Exception as e:
                        self.logger.log(
                            f"Erro durante análise visual: {e}. Continuando sem dados visuais.", 
                            "WARNING", task_id, exc_info=True
                        )
                        visual_data = []
            else:
                self.logger.log(
                    "Análise visual desativada - usando lista vazia para dados visuais.", 
                    "INFO", task_id
                )
                pq.put({
                    'type': 'progress', 
                    'stage': 'Análise', 
                    'percentage': 75, 
                    'task_id': task_id
                })
                
            # Garante que visual_data seja sempre uma lista
            visual_data = list(visual_data or [])
            
            # Log de diagnóstico detalhado
            self.logger.log(
                f"Preparando análise de conteúdo com {len(visual_data)} pontos de dados visuais "
                f"(tipo: {type(visual_data).__name__})", 
                "DEBUG", task_id
            )

            if stop_event.is_set():
                raise InterruptedError("Processo interrompido pelo usuário.")

            segments = self._get_module('content').create_speech_segments(
                words, pq, task_config.get('use_visual_analysis'), visual_data, task_id, stop_event)
            
            if not segments:
                raise RuntimeError("Nenhum segmento de fala foi criado após a análise.")
            
            if stop_event.is_set():
                raise InterruptedError("Processo interrompido pelo usuário.")
                
            script_path = self._get_module('composer').generate_and_save_json(
                segments, task_config['video_path'], task_id)
                
            if not script_path:
                raise RuntimeError("Falha ao salvar o arquivo de roteiro.")
            
            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError as e:
            self.logger.log("Processamento interrompido pelo usuário", "WARNING", task_id)
            pq.put({
                'type': 'interrupted',
                'message': str(e),
                'task_id': task_id
            })
            
        except (ValidationError, ResourceError) as e:
            self.logger.log(f"Erro de validação/recurso: {e}", "ERROR", task_id, exc_info=True)
            pq.put({
                'type': 'error',
                'message': f"Erro de configuração: {str(e)}",
                'task_id': task_id
            })
            
        except ProcessingError as e:
            self.logger.log(f"Erro de processamento: {e}", "ERROR", task_id, exc_info=True)
            pq.put({
                'type': 'error',
                'message': f"Erro durante processamento: {str(e)}",
                'task_id': task_id
            })
            
        except Exception as e:
            self.logger.log(f"Erro fatal no pipeline Sapiens: {e}", "CRITICAL", task_id, exc_info=True)
            pq.put({
                'type': 'error',
                'message': f"Erro fatal: {str(e)}",
                'task_id': task_id
            })
            
        finally:
            # Garante limpeza de recursos pendentes
            for resource_id, resource in list(self._active_resources.items()):
                try:
                    if hasattr(resource, 'cleanup'):
                        resource.cleanup()
                    self._active_resources.pop(resource_id, None)
                except Exception as e:
                    self.logger.log(
                        f"Erro ao limpar recurso {resource_id}: {e}", 
                        "WARNING", task_id
                    )

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
