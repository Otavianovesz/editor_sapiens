# -*- coding: utf-8 -*-

import os
import tempfile
import subprocess
import platform
import queue
import threading
import json
import dataclasses
from typing import List, Any, Optional, Dict

from .config import Config
from utils.logger import Logger

class MediaProcessor:
    """Responsável pela extração de áudio de arquivos de vídeo."""
    def __init__(self, logger: Logger):
        self.logger = logger

    def extract_audio(self, video_filepath: str, task_id: str) -> Optional[str]:
        self.logger.log(f"Iniciando extração de áudio de '{os.path.basename(video_filepath)}'.", "INFO", task_id)
        try:
            import ffmpeg
        except ImportError:
            self.logger.log("Módulo 'ffmpeg-python' não encontrado! Instale com 'pip install ffmpeg-python'", "CRITICAL", task_id)
            return None
        
        try:
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_f:
                output_audio_path = temp_f.name
            
            creation_flags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
            stream = ffmpeg.input(video_filepath).output(output_audio_path, acodec='pcm_s16le', ar='16000', ac=1)
            args = ffmpeg.compile(stream, overwrite_output=True)
            
            result = subprocess.run(args, capture_output=True, creationflags=creation_flags)
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)

            file_size = os.path.getsize(output_audio_path) / (1024 * 1024)
            self.logger.log(f"Áudio extraído com sucesso para '{output_audio_path}' (Tamanho: {file_size:.2f} MB).", "SUCCESS", task_id)
            return output_audio_path
        except ffmpeg.Error as e:
            self.logger.log(f"ERRO FFmpeg na extração: {e.stderr.decode() if e.stderr else e}", "ERROR", task_id)
            return None
        except Exception as e:
            self.logger.log(f"ERRO inesperado na extração de áudio: {e}", "CRITICAL", task_id, exc_info=True)
            return None

    def cleanup(self, path: Optional[str], task_id: str):
        if path and os.path.exists(path):
            try:
                os.remove(path)
                self.logger.log(f"Arquivo temporário '{path}' removido.", "DEBUG", task_id)
            except OSError as e:
                self.logger.log(f"Falha ao remover arquivo temporário '{path}': {e}", "WARNING", task_id)

class AudioTranscriber:
    """Responsável pela transcrição de áudio usando o modelo Whisper."""
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.model = None

    def _load_model(self, task_id):
        try:
            from faster_whisper import WhisperModel
        except ImportError:
            self.logger.log("Módulo 'faster_whisper' não encontrado! Instale com 'pip install faster-whisper'", "CRITICAL", task_id)
            raise

        model_name = self.config.get("whisper_model_size")
        device = self.config.get("whisper_device")
        compute_type = self.config.get("whisper_compute_type")
        download_root = "models"
        
        self.logger.log(f"Carregando modelo Whisper '{model_name}' (Dispositivo: {device}, Tipo: {compute_type})...", "INFO", task_id)
        try:
            try:
                self.logger.log(f"Verificando cache local para o modelo '{model_name}'...", "INFO", task_id)
                self.model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root=download_root, local_files_only=True)
                self.logger.log("Modelo encontrado no cache local. Carregamento rápido.", "SUCCESS", task_id)
            except Exception:
                self.logger.log(f"Modelo '{model_name}' não encontrado no cache local. Iniciando download...", "WARNING", task_id)
                self.logger.log("--> ESTE PROCESSO PODE LEVAR VÁRIOS MINUTOS E USAR GIGABYTES DE ESPAÇO. <--", "WARNING", task_id)
                self.logger.log("--> Por favor, aguarde. O aplicativo pode parecer travado durante o download. <--", "WARNING", task_id)
                self.model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root=download_root, local_files_only=False)
                self.logger.log("Download do modelo concluído com sucesso!", "SUCCESS", task_id)
        except Exception as e:
            self.logger.log(f"ERRO CRÍTICO ao baixar ou carregar modelo Whisper: {e}", "CRITICAL", task_id, exc_info=True)
            raise

    def transcribe(self, path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Any]:
        if self.model is None:
            self._load_model(task_id)

        lang = self.config.get("whisper_language") or None
        self.logger.log(f"Iniciando transcrição (Idioma: {'Automático' if lang is None else lang}).", "INFO", task_id)
        
        # O filtro VAD (Voice Activity Detection) está ativado aqui, conforme solicitado.
        segments_gen, info = self.model.transcribe(path, language=lang, word_timestamps=True, vad_filter=True)
        
        if info.duration <= 0:
            self.logger.log("Duração inválida do áudio detectada.", "ERROR", task_id)
            return []
            
        if not info.language or info.language_probability < 0.5:
            self.logger.log(f"Aviso: Baixa confiança na detecção do idioma ({info.language}, {info.language_probability:.2f})", "WARNING", task_id)
            
        self.logger.log(f"Idioma detectado: {info.language} (Probabilidade: {info.language_probability:.2f}), Duração: {info.duration:.2f}s", "INFO", task_id)
        
        # Otimização de memória: cria uma classe leve para armazenar apenas os dados
        # essenciais, evitando guardar os objetos originais do faster-whisper.
        Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
        all_words = []
        
        try:
            for segment in segments_gen:
                if stop_event.is_set():
                    self.logger.log("Transcrição interrompida pelo usuário.", "WARNING", task_id)
                    return []
                
                progress = 11 + (segment.end / info.duration) * 39 if info.duration > 0 else 50
                pq.put({'type': 'progress', 'stage': 'Transcrevendo', 'percentage': progress, 'task_id': task_id})
                
                if hasattr(segment, "words") and segment.words:
                    # Cria cópias leves dos objetos de palavra para economizar memória.
                    for word_obj in segment.words:
                        all_words.append(Word(start=word_obj.start, end=word_obj.end, word=word_obj.word))

            if len(all_words) == 0:
                self.logger.log("Nenhuma palavra foi transcrita. Possível problema com o áudio.", "ERROR", task_id)
                return []

            self.logger.log(f"Transcrição finalizada. Total de {len(all_words)} palavras encontradas.", "SUCCESS", task_id)
            return all_words
        except Exception as e:
            self.logger.log(f"Erro durante o processamento de segmentos: {e}", "ERROR", task_id, exc_info=True)
            return []

class VisualAnalyzer:
    """Responsável pela análise visual de gestos e olhar usando MediaPipe."""
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.pose_model = None

def _init_model(self, task_id):
    try:
        import mediapipe as mp
    except ImportError:
        self.logger.log("Módulo 'mediapipe' não encontrado! Instale com 'pip install mediapipe'", "CRITICAL", task_id)
        raise
    self.logger.log("Inicializando modelo MediaPipe Pose...", "INFO", task_id)
    # guardamos o módulo mp apenas; a instância do Pose será usada como context manager
    self._mp = mp

def analyze_video_in_single_pass(self, video_path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Dict]:
    """
    Percorre o vídeo em passos (aprox. visual_analysis_fps) e retorna uma lista com
    dados de timestamp + flags (looking_away, gesturing). Implementação simples, com
    proteção contra falhas de bibliotecas nativas.
    """
    try:
        import cv2
    except ImportError:
        self.logger.log("'opencv-python' não encontrado! Instale com 'pip install opencv-python'", "CRITICAL", task_id)
        return []

    # (re)inicializa o mp se necessário
    if not hasattr(self, "_mp") or self._mp is None:
        try:
            self._init_model(task_id)
        except Exception as e:
            self.logger.log(f"Falha ao inicializar MediaPipe: {e}", "CRITICAL", task_id, exc_info=True)
            return []

    mp = self._mp

    cap = None
    results = []
    try:
        self.logger.log(f"Tentando abrir o vídeo para análise visual: {video_path}", "DEBUG", task_id)
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS) or 0.0

        if fps <= 0 or total_frames <= 0:
            self.logger.log(f"Vídeo inválido para análise visual: {total_frames} frames, {fps:.2f} FPS.", "ERROR", task_id)
            return []

        target_fps = max(1, int(self.config.get("visual_analysis_fps", 5)))
        process_every = max(1, int(max(1, fps) / target_fps))
        self.logger.log(f"Análise visual iniciada. Vídeo: {total_frames} frames @ {fps:.2f} FPS. Processando a cada {process_every} frames para atingir ~{target_fps} FPS.", "INFO", task_id)

        # usamos context manager para garantir liberação de recursos do MediaPipe
        with mp.solutions.pose.Pose(min_detection_confidence=0.5, min_tracking_confidence=0.5, model_complexity=1) as pose:
            for frame_idx in range(0, total_frames, process_every):
                if stop_event.is_set():
                    self.logger.log("Análise visual interrompida pelo usuário.", "WARNING", task_id)
                    break

                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                ret, frame = cap.read()
                if not ret:
                    self.logger.log(f"Falha ao ler frame {frame_idx}. Encerrando análise visual.", "WARNING", task_id)
                    break

                # enviar progresso para UI
                try:
                    pq.put({'type': 'progress', 'stage': 'Análise Visual', 'percentage': 51 + (frame_idx / max(1, total_frames)) * 24, 'task_id': task_id})
                except Exception:
                    # não crítico, apenas continue
                    pass

                try:
                    # MediaPipe espera imagens RGB
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    pm_res = pose.process(rgb)
                except Exception as e:
                    # captura erros provenientes de bindings nativos (pelo menos logamos e continuamos)
                    self.logger.log(f"Erro ao processar frame {frame_idx} com MediaPipe: {e}", "ERROR", task_id, exc_info=True)
                    # para segurança, adicionamos um ponto padrão e continuamos
                    results.append({"timestamp": (frame_idx / fps), "looking_away": False, "gesturing": False})
                    continue

                # A lógica real de detecção deve ser implementada aqui.
                # Por ora, mantemos valores padrão (mas extraímos landmarkes se disponíveis).
                looking_away = False
                gesturing = False
                if pm_res.pose_landmarks:
                    # Exemplo simples: se nariz deslocado muito para a esquerda/direita, considerar olhar para fora
                    try:
                        lm = pm_res.pose_landmarks.landmark
                        # índice 0: nariz no model do MediaPipe Pose
                        nose = lm[0]
                        # yaw simplificado (x perto de 0.5 centro): ajustamos thresholds a partir da config
                        if nose.x < 0.2 or nose.x > 0.8:
                            looking_away = True
                    except Exception:
                        pass

                results.append({"timestamp": (frame_idx / fps), "looking_away": looking_away, "gesturing": gesturing})

    except Exception as e:
        # captura qualquer erro inesperado e loga; retornamos o que foi coletado até então
        self.logger.log(f"Erro FATAL na análise visual: {e}", "CRITICAL", task_id, exc_info=True)
    finally:
        if cap is not None:
            try:
                cap.release()
            except Exception:
                pass

    self.logger.log(f"Análise visual concluída. {len(results)} pontos de dados gerados.", "SUCCESS", task_id)
    return results

class ContentAnalyzer:
    """Analisa a transcrição e os dados visuais para criar segmentos de fala."""
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def create_speech_segments(self, words, pq, use_visual, visual_data, task_id, stop_event):
        if not words:
            self.logger.log("Nenhuma palavra fornecida para análise de conteúdo.", "WARNING", task_id)
            return []
            
        self.logger.log(f"Analisando {len(words)} palavras para criar segmentos de corte...", "INFO", task_id)
        
        segs, start, n = [], words[0].start, len(words)
        fillers = self.config.get('filler_words', [])
        ctx_pause = self.config.get('filler_word_context_pause')
        
        for i in range(n - 1):
            if stop_event.is_set(): return []
            
            pq.put({'type': 'progress', 'stage': 'Analisando Conteúdo', 'percentage': 76 + (i / n) * 20, 'task_id': task_id})
            
            cur, nxt = words[i], words[i+1]
            pause = nxt.start - cur.end
            score = 0
            cut_reason = None
            
            if pause >= self.config.get('pause_threshold_s'):
                score += self.config.get('scores')['pause_long'] if pause > 0.8 else self.config.get('scores')['pause_medium']
                if score <= self.config.get('cut_threshold'):
                    cut_reason = f"pausa longa ({pause:.2f}s)"
            
            is_filler = cur.word.strip('.,?!- ').lower() in fillers
            if is_filler and (pause > ctx_pause or (i > 0 and cur.start - words[i-1].end > ctx_pause)):
                cut_reason = f"palavra de preenchimento ('{cur.word.strip()}')"

            if cut_reason:
                self.logger.log(f"Corte em {cur.end:.2f}s. Motivo: {cut_reason}.", "DEBUG", task_id, to_ui=False)
                self._add_seg(segs, start, cur.end)
                start = nxt.start
                
        self._add_seg(segs, start, words[-1].end)
        self.logger.log(f"Análise de conteúdo finalizada. {len(segs)} segmentos de fala mantidos.", "SUCCESS", task_id)
        return segs

    def _add_seg(self, segs, start, end):
        if (end - start) >= self.config.get("min_segment_duration_s"):
            segs.append({'start': start, 'end': end})

class ScriptComposer:
    """Gera e salva o arquivo de roteiro em formato JSON."""
    def __init__(self, logger: Logger):
        self.logger = logger

    def generate_and_save_json(self, segments, video_path, task_id):
        data = {"segments": [{"start_time": f"{s['start']:.3f}", "end_time": f"{s['end']:.3f}"} for s in segments]}
        out_path = os.path.splitext(video_path)[0] + ".json"
        
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            self.logger.log(f"Roteiro com {len(segments)} segmentos salvo em '{out_path}'", "SUCCESS", task_id)
            return out_path
        except IOError as e:
            self.logger.log(f"ERRO ao salvar roteiro JSON em '{out_path}': {e}", "ERROR", task_id)
            return None
