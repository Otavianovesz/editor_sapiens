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
        self._resources_lock = threading.Lock()  # Lock para sincronização de recursos
        self._validate_config()
        
    def _cleanup_resources(self, task_id: str):
        """Limpa recursos de forma thread-safe."""
        with self._resources_lock:
            resources_to_clean = [
                (resource_id, resource) 
                for resource_id, resource in self._active_resources.items()
                if task_id in resource_id
            ]
            
            for resource_id, resource in resources_to_clean:
                try:
                    if hasattr(resource, 'cleanup'):
                        resource.cleanup()
                    self._active_resources.pop(resource_id, None)
                    self.logger.log(f"Recurso {resource_id} liberado com sucesso", "DEBUG", task_id)
                except Exception as e:
                    self.logger.log(f"Erro ao limpar recurso {resource_id}: {e}", "WARNING", task_id)
        
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
        """
        Executa o pipeline completo de análise e geração de roteiro com gerenciamento
        seguro de recursos e threads.
        """
        task_id = task_config['id']
        thread_name = threading.current_thread().name
        self.logger.log(f"Iniciando pipeline Sapiens na thread {thread_name}", "DEBUG", task_id)
        
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

            # Gerenciamento da análise visual com tratamento robusto de erros
            visual_data = []
            
            try:
                if task_config.get('use_visual_analysis'):
                    self.logger.log("Iniciando análise visual...", "INFO", task_id)
                    visual_analyzer = self._get_module('visual')
                    
                    if not visual_analyzer:
                        raise RuntimeError("Falha ao inicializar analisador visual")
                    
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
                        "Análise visual desativada - continuando sem análise visual", 
                        "INFO", task_id
                    )
                
                # Garantir progresso mesmo sem análise visual
                pq.put({
                    'type': 'progress', 
                    'stage': 'Análise', 
                    'percentage': 75, 
                    'task_id': task_id
                })
                
                # Garantir que visual_data é sempre uma lista
                visual_data = list(visual_data or [])
                
                self.logger.log(
                    f"Preparando para próxima etapa com {len(visual_data)} pontos de dados visuais", 
                    "INFO", task_id
                )
                
            except Exception as e:
                self.logger.log(
                    f"Erro crítico durante análise visual: {e}", 
                    "ERROR", task_id, exc_info=True
                )
                visual_data = []  # Garante continuidade mesmo após erro
                
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

            try:
                content_analyzer = self._get_module('content')
                if not content_analyzer:
                    raise RuntimeError("Falha ao obter módulo de análise de conteúdo")

                self.logger.log(
                    f"Iniciando análise de conteúdo com análise visual {'habilitada' if task_config.get('use_visual_analysis') else 'desabilitada'}",
                    "INFO", task_id
                )
                
                segments = content_analyzer.create_speech_segments(
                    words=words,
                    pq=pq,
                    use_visual=task_config.get('use_visual_analysis', False),
                    visual_data=visual_data or [],  # Garante lista vazia se None
                    task_id=task_id,
                    stop_event=stop_event
                )
                
                if not segments:
                    raise RuntimeError("Nenhum segmento de fala foi criado após a análise.")
                    
                self.logger.log(f"Análise de conteúdo concluída com sucesso: {len(segments)} segmentos criados", "SUCCESS", task_id)
            except Exception as e:
                self.logger.log(f"Erro durante análise de conteúdo: {str(e)}", "ERROR", task_id, exc_info=True)
                raise
            
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
        """
        Executa a tarefa de renderização de vídeo a partir de um roteiro,
        com gerenciamento seguro de recursos e tratamento de erros.
        """
        task_id = task_config['id']
        thread_name = threading.current_thread().name
        renderer = None
        
        try:
            self.logger.log(f"Iniciando renderização na thread {thread_name}", "DEBUG", task_id)
            
            # Garante limpeza de recursos anteriores antes de iniciar
            self._cleanup_resources(task_id)
            
            # Validação do arquivo de roteiro
            script_path = task_config.get('render_script_path', '')
            if not script_path or not os.path.exists(script_path):
                raise ResourceError(f"Arquivo de roteiro não encontrado: {script_path}")
            
            # Preparação do caminho de saída
            output_path = os.path.splitext(task_config['video_path'])[0] + "_editado.mp4"
            
            # Criação do renderizador com proteção contra erros
            try:
                renderer = VideoRenderer(
                    source_path=task_config['video_path'], 
                    json_path=script_path, 
                    output_path=output_path, 
                    preset=self.config.get("render_preset"), 
                    logger=self.logger, 
                    pq=pq, 
                    task_id=task_id
                )
            except Exception as e:
                raise ProcessingError(f"Falha ao inicializar renderizador: {e}")
            
            # Atualização thread-safe do renderizador atual
            with self._resources_lock:
                self.current_renderer = renderer
            
            try:
                renderer.run(stop_event)
                if stop_event.is_set():
                    raise InterruptedError("Renderização interrompida pelo usuário")
            except Exception as e:
                raise ProcessingError(f"Erro durante renderização: {e}")
                
        except InterruptedError as e:
            self.logger.log(str(e), "WARNING", task_id)
            pq.put({
                'type': 'interrupted',
                'message': str(e),
                'task_id': task_id
            })
            
        except (ResourceError, ProcessingError) as e:
            self.logger.log(f"Erro na renderização: {e}", "ERROR", task_id, exc_info=True)
            pq.put({
                'type': 'error',
                'message': str(e),
                'task_id': task_id
            })
            
        except Exception as e:
            self.logger.log(f"Erro fatal na renderização: {e}", "CRITICAL", task_id, exc_info=True)
            pq.put({
                'type': 'error',
                'message': f"Erro fatal: {str(e)}",
                'task_id': task_id
            })
            
        finally:
            # Limpa o renderizador atual de forma thread-safe
            with self._resources_lock:
                self.current_renderer = None
            
            # Garante limpeza final dos recursos
            self._cleanup_resources(task_id)
            
            if not stop_event.is_set():
                pq.put({'type': 'done', 'task_id': task_id})
