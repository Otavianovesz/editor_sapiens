# -*- coding: utf-8 -*-

import os
import tempfile
import subprocess
import platform
import queue
import threading
import json
import dataclasses
import re
from datetime import datetime, timedelta
from typing import List, Any, Optional, Dict, Union

from .config import Config
from utils.logger import Logger

# --- Módulo de Análise de Legendas (SRT/VTT) ---

class SubtitleParser:
    """
    Responsável por ler arquivos de legenda (.srt, .vtt) e convertê-los
    para o formato de lista de palavras compatível com o ContentAnalyzer.
    """
    def __init__(self, logger: Logger):
        self.logger = logger
        self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])

    def _time_str_to_seconds(self, time_str: str) -> float:
        """Converte uma string de tempo (HH:MM:SS,ms) para segundos."""
        time_str = time_str.replace(',', '.')
        try:
            dt = datetime.strptime(time_str, '%H:%M:%S.%f')
        except ValueError:
            dt = datetime.strptime(time_str, '%H:%M:%S')
        return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond).total_seconds()

    def parse(self, file_path: str, task_id: str) -> List[Any]:
        """Lê o arquivo de legenda e retorna uma lista de 'palavras'."""
        self.logger.log(f"Iniciando análise do arquivo de legenda: {os.path.basename(file_path)}", "INFO", task_id)
        words = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            pattern = re.compile(r'(\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d{3})[\s\S]*?\n([\s\S]+?)(?=\n\n|\Z)', re.MULTILINE)
            
            for match in pattern.finditer(content):
                start_time_s = self._time_str_to_seconds(match.group(1))
                end_time_s = self._time_str_to_seconds(match.group(2))
                text = match.group(3).strip().replace('\n', ' ')

                sub_words = [w for w in text.split(' ') if w]
                if not sub_words: continue

                duration_per_word = (end_time_s - start_time_s) / len(sub_words)
                current_time = start_time_s

                for i, word_text in enumerate(sub_words):
                    word_start = current_time
                    word_end = current_time + duration_per_word
                    words.append(self.Word(start=word_start, end=word_end, word=word_text))
                    current_time = word_end

            self.logger.log(f"Análise da legenda concluída. {len(words)} palavras extraídas.", "SUCCESS", task_id)
            return words

        except FileNotFoundError:
            self.logger.log(f"Arquivo de legenda não encontrado: {file_path}", "ERROR", task_id)
            return []
        except Exception as e:
            self.logger.log(f"Erro inesperado ao analisar arquivo de legenda: {e}", "CRITICAL", task_id, exc_info=True)
            return []


# --- Módulos de Processamento de Mídia e IA ---

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

            result = subprocess.run(args, capture_output=True, creationflags=creation_flags, timeout=600)
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)

            file_size = os.path.getsize(output_audio_path) / (1024 * 1024)
            self.logger.log(f"Áudio extraído com sucesso para '{output_audio_path}' (Tamanho: {file_size:.2f} MB).", "SUCCESS", task_id)
            return output_audio_path
        except subprocess.TimeoutExpired:
            self.logger.log("Timeout: A extração de áudio demorou mais de 10 minutos e foi cancelada.", "ERROR", task_id)
            return None
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
    """Implementação Singleton para o transcritor de áudio."""
    _instance = None
    _model = None
    _model_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AudioTranscriber, cls).__new__(cls)
        return cls._instance

    def __init__(self, logger: Logger, config: Config):
        if not hasattr(self, 'logger'):
            self.logger = logger
            self.config = config

    def _load_model(self, pq: queue.Queue, task_id: str):
        with self._model_lock:
            if self._model is not None: return
            try:
                from faster_whisper import WhisperModel
            except ImportError:
                self.logger.log("Módulo 'faster_whisper' não encontrado! Instale com 'pip install faster-whisper'", "CRITICAL", task_id)
                raise

            model_name, device, compute_type, download_root = self.config.get("whisper_model_size"), self.config.get("whisper_device"), self.config.get("whisper_compute_type"), "models"
            self.logger.log(f"Carregando modelo Whisper '{model_name}' pela primeira vez...", "INFO", task_id)
            pq.put({'type': 'progress', 'stage': f'Carregando Modelo {model_name}', 'percentage': 1, 'task_id': task_id})

            try:
                self.logger.log(f"Verificando cache local para o modelo...", "INFO", task_id)
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root=download_root, local_files_only=True)
                self.logger.log("Modelo encontrado no cache local.", "SUCCESS", task_id)
            except (ValueError, FileNotFoundError):
                self.logger.log(f"Modelo não encontrado no cache. Iniciando download...", "WARNING", task_id)
                pq.put({'type': 'progress', 'stage': f'Baixando Modelo...', 'percentage': 2, 'task_id': task_id})
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root=download_root, local_files_only=False)
                self.logger.log("Download do modelo concluído.", "SUCCESS", task_id)
            except Exception as e:
                self.logger.log(f"ERRO CRÍTICO ao carregar modelo Whisper: {e}", "CRITICAL", task_id, exc_info=True); self._model = None; raise
    
    def release_gpu_memory(self):
        if self.config.get("whisper_device") == "cuda":
            try:
                import torch; torch.cuda.empty_cache(); self.logger.log("Cache de memória da GPU limpo.", "DEBUG")
            except Exception as e: self.logger.log(f"Falha ao limpar cache da GPU: {e}", "ERROR")

    def transcribe(self, path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Any]:
        if self._model is None:
            self._load_model(pq, task_id)
            if self._model is None: raise RuntimeError("Modelo de transcrição não pôde ser carregado.")
        lang = self.config.get("whisper_language") or None
        self.logger.log(f"Iniciando transcrição (Idioma: {'Automático' if lang is None else lang}).", "INFO", task_id)
        segments_gen, info = self._model.transcribe(path, language=lang, word_timestamps=True, vad_filter=True)
        self.logger.log(f"Idioma detectado: {info.language} (Prob: {info.language_probability:.2f}), Duração: {info.duration:.2f}s", "INFO", task_id)
        Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
        all_words = [Word(start=w.start, end=w.end, word=w.word) for s in segments_gen for w in s.words or [] if not stop_event.is_set()]
        if stop_event.is_set(): self.logger.log("Transcrição interrompida.", "WARNING", task_id); return []
        self.logger.log(f"Transcrição finalizada. {len(all_words)} palavras.", "SUCCESS", task_id)
        return all_words

class VisualAnalyzer:
    """Responsável pela análise visual (placeholder)."""
    def __init__(self, logger: Logger, config: Config):
        self.logger, self.config, self.pose_model = logger, config, None
    def _init_model(self, task_id):
        try: import mediapipe as mp; self.pose_model = mp.solutions.pose.Pose(min_detection_confidence=0.5, min_tracking_confidence=0.5, model_complexity=1)
        except ImportError: self.logger.log("Módulo 'mediapipe' não encontrado.", "CRITICAL", task_id); raise
    def analyze_video_in_single_pass(self, video_path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Dict]:
        try: import cv2
        except ImportError: self.logger.log("'opencv-python' não encontrado.", "CRITICAL", task_id); return []
        if self.pose_model is None: self._init_model(task_id)
        cap = cv2.VideoCapture(video_path)
        total_frames, fps = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0 or total_frames <= 0: self.logger.log("Vídeo inválido.", "ERROR", task_id); cap.release(); return []
        process_every = max(1, int(fps / self.config.get("visual_analysis_fps")))
        results = []
        for frame_idx in range(0, total_frames, process_every):
            if stop_event.is_set(): break
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx); ret, _ = cap.read()
            if not ret: break
            pq.put({'type': 'progress', 'stage': 'Análise Visual', 'percentage': 51 + (frame_idx / total_frames) * 24, 'task_id': task_id})
            results.append({"timestamp": (frame_idx / fps), "looking_away": False, "gesturing": False})
        cap.release(); self.logger.log("Análise visual (placeholder) concluída.", "SUCCESS", task_id)
        return results

class ContentAnalyzer:
    """Analisa a transcrição e aplica a lógica de corte com margens de segurança."""
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def create_speech_segments(self, words: List[Any], pq: queue.Queue, use_visual: bool, visual_data: Any, task_id: str, stop_event: threading.Event) -> List[Dict[str, float]]:
        if not words: self.logger.log("Nenhuma palavra para análise.", "WARNING", task_id); return []
        self.logger.log(f"Analisando {len(words)} palavras para criar segmentos...", "INFO", task_id)

        segs = []
        start_time = max(0, words[0].start - self.config.get("segment_padding_start_s", 0.1))
        n = len(words)
        
        # Obtém as configurações de padding
        padding_start = self.config.get("segment_padding_start_s", 0.1)
        padding_end = self.config.get("segment_padding_end_s", 0.1)
        fillers = self.config.get('filler_words', [])
        ctx_pause = self.config.get('filler_word_context_pause')

        for i in range(n - 1):
            if stop_event.is_set(): return []
            if i % 100 == 0: pq.put({'type': 'progress', 'stage': 'Analisando Conteúdo', 'percentage': 76 + (i / n) * 20, 'task_id': task_id})

            cur, nxt = words[i], words[i+1]
            pause = nxt.start - cur.end
            cut_reason = None

            if pause >= self.config.get('pause_threshold_s'):
                score = self.config.get('scores')['pause_long'] if pause > 0.8 else self.config.get('scores')['pause_medium']
                if score <= self.config.get('cut_threshold'): cut_reason = f"pausa longa ({pause:.2f}s)"
            
            is_filler = cur.word.strip('.,?!- ').lower() in fillers
            if is_filler and (pause > ctx_pause or (i > 0 and cur.start - words[i-1].end > ctx_pause)):
                cut_reason = f"palavra de preenchimento ('{cur.word.strip()}')"

            if cut_reason:
                self.logger.log(f"Corte em {cur.end:.2f}s. Motivo: {cut_reason}.", "DEBUG", task_id, to_ui=False)
                
                # --- LÓGICA DE PADDING COM PREVENÇÃO DE SOBREPOSIÇÃO ---
                segment_end = cur.end + padding_end
                next_segment_start = nxt.start - padding_start
                
                # Se as margens se sobrepõem, encontre o ponto médio da pausa
                if segment_end > next_segment_start:
                    midpoint = cur.end + pause / 2
                    segment_end = midpoint
                    next_segment_start = midpoint
                
                self._add_seg(segs, start_time, segment_end)
                start_time = next_segment_start
        
        # Adiciona o último segmento
        final_end_time = words[-1].end + padding_end
        self._add_seg(segs, start_time, final_end_time)
        
        self.logger.log(f"Análise finalizada. {len(segs)} segmentos mantidos.", "SUCCESS", task_id)
        return segs

    def _add_seg(self, segs: List[Dict[str, float]], start: float, end: float):
        """Adiciona um segmento à lista se tiver a duração mínima."""
        # Garante que o início nunca seja negativo
        start = max(0, start)
        if (end - start) >= self.config.get("min_segment_duration_s"):
            segs.append({'start': start, 'end': end})

class ScriptComposer:
    """Gera e salva o arquivo de roteiro JSON."""
    def __init__(self, logger: Logger): self.logger = logger
    def generate_and_save_json(self, segments: List[Dict[str, float]], video_path: str, task_id: str) -> Optional[str]:
        data = { "segments": [{"start": round(s['start'], 3), "end": round(s['end'], 3)} for s in segments] }
        out_path = os.path.splitext(video_path)[0] + ".json"
        try:
            with open(out_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
            self.logger.log(f"Roteiro com {len(segments)} segmentos salvo.", "SUCCESS", task_id)
            return out_path
        except IOError as e: self.logger.log(f"ERRO ao salvar roteiro JSON: {e}", "ERROR", task_id); return None

class SubtitleGenerator:
    """Gera um arquivo de legenda .srt sincronizado com o vídeo editado."""
    def __init__(self, logger: Logger): self.logger = logger; self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
    def _seconds_to_srt_time(self, seconds: float) -> str:
        millis = int((seconds - int(seconds)) * 1000); td = timedelta(seconds=int(seconds)); return f"{td},{millis:03d}"
    def _remap_words_to_new_timeline(self, words: List[Any], segments: List[Dict[str, float]]) -> List[Any]:
        remapped_words = []; time_removed = 0.0; last_seg_end = 0.0
        for segment in segments:
            time_removed += (segment['start'] - last_seg_end)
            for word in words:
                if segment['start'] <= word.start < segment['end']:
                    remapped_words.append(self.Word(start=word.start - time_removed, end=word.end - time_removed, word=word.word))
            last_seg_end = segment['end']
        return remapped_words
    def generate_srt(self, words: List[Any], segments: List[Dict[str, float]], output_path: str, task_id: str):
        self.logger.log("Gerando legenda .srt sincronizada...", "INFO", task_id)
        if not words or not segments: self.logger.log("Dados insuficientes para gerar legenda.", "WARNING", task_id); return
        remapped_words = self._remap_words_to_new_timeline(words, segments)
        if not remapped_words: self.logger.log("Nenhuma palavra nos segmentos mantidos.", "WARNING", task_id); return
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                subtitle_index, current_line, line_start_time = 1, "", remapped_words[0].start
                for i, word in enumerate(remapped_words):
                    next_word_start = remapped_words[i+1].start if i + 1 < len(remapped_words) else word.end + 5
                    current_line += word.word + " "
                    if len(current_line) > 42 or (next_word_start - word.end > 1.0) or (word.end - line_start_time > 5.0):
                        f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(word.end)}\n{current_line.strip()}\n\n")
                        subtitle_index += 1; current_line = ""
                        if i + 1 < len(remapped_words): line_start_time = remapped_words[i+1].start
                if current_line.strip():
                    f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(remapped_words[-1].end)}\n{current_line.strip()}\n\n")
            self.logger.log(f"Legenda .srt salva com sucesso.", "SUCCESS", task_id)
        except Exception as e: self.logger.log(f"Erro ao gerar legenda .srt: {e}", "CRITICAL", task_id, exc_info=True)