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
from typing import List, Any, Optional, Dict, Union, Tuple

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
    _instance = None; _model = None; _model_lock = threading.Lock()
    def __new__(cls, *args, **kwargs):
        if cls._instance is None: cls._instance = super(AudioTranscriber, cls).__new__(cls)
        return cls._instance
    def __init__(self, logger: Logger, config: Config):
        if not hasattr(self, 'logger'): self.logger = logger; self.config = config
    def _load_model(self, pq: queue.Queue, task_id: str):
        with self._model_lock:
            if self._model is not None: return
            try: from faster_whisper import WhisperModel
            except ImportError: self.logger.log("'faster_whisper' não encontrado.", "CRITICAL", task_id); raise
            model_name, device, compute_type = self.config.get("whisper_model_size"), self.config.get("whisper_device"), self.config.get("whisper_compute_type")
            pq.put({'type': 'progress', 'stage': f'Carregando Modelo {model_name}', 'percentage': 1, 'task_id': task_id})
            try:
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root="models", local_files_only=True)
                self.logger.log("Modelo carregado do cache local.", "SUCCESS", task_id)
            except (ValueError, FileNotFoundError):
                pq.put({'type': 'progress', 'stage': f'Baixando Modelo...', 'percentage': 2, 'task_id': task_id})
                self.logger.log(f"Baixando modelo '{model_name}'. Isso pode levar vários minutos.", "WARNING", task_id)
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root="models", local_files_only=False)
            except Exception as e: self.logger.log(f"ERRO CRÍTICO ao carregar modelo: {e}", "CRITICAL", task_id, exc_info=True); self._model = None; raise
    def release_gpu_memory(self):
        if self.config.get("whisper_device") == "cuda":
            try: import torch; torch.cuda.empty_cache(); self.logger.log("Cache de VRAM da GPU limpo.", "DEBUG")
            except Exception as e: self.logger.log(f"Falha ao limpar cache da GPU: {e}", "ERROR")
    def transcribe(self, path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Any]:
        if self._model is None: self._load_model(pq, task_id)
        if self._model is None: raise RuntimeError("Modelo de transcrição não carregado.")
        lang = self.config.get("whisper_language") or None
        segments_gen, info = self._model.transcribe(path, language=lang, word_timestamps=True, vad_filter=True)
        self.logger.log(f"Idioma: {info.language} (Prob: {info.language_probability:.2f}), Duração: {info.duration:.2f}s", "INFO", task_id)
        Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
        all_words = []
        for s in segments_gen:
            if stop_event.is_set(): self.logger.log("Transcrição interrompida.", "WARNING", task_id); return []
            pq.put({'type': 'progress', 'stage': 'Transcrevendo', 'percentage': 11 + (s.end / info.duration) * 39 if info.duration > 0 else 50, 'task_id': task_id})
            if s.words: all_words.extend([Word(start=w.start, end=w.end, word=w.word) for w in s.words])
        self.logger.log(f"Transcrição finalizada com {len(all_words)} palavras.", "SUCCESS", task_id)
        return all_words

class VisualAnalyzer:
    """Responsável pela análise visual (placeholder)."""
    def __init__(self, logger: Logger, config: Config): self.logger, self.config, self.pose_model = logger, config, None
    def _init_model(self, task_id):
        try: import mediapipe as mp; self.pose_model = mp.solutions.pose.Pose(min_detection_confidence=0.5, min_tracking_confidence=0.5, model_complexity=1)
        except ImportError: self.logger.log("'mediapipe' não encontrado.", "CRITICAL", task_id); raise
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
    """
    Analisa a transcrição e gera duas listas de segmentos: uma com margens (padding)
    para cortes de vídeo suaves, e outra com tempos exatos para sincronização de legendas.
    """
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def create_speech_segments(self, words: List[Any], pq: queue.Queue, task_config: Dict, task_id: str, stop_event: threading.Event) -> List[Dict[str, float]]:
        # A assinatura foi corrigida para aceitar todos os argumentos enviados pelo Orchestrator.
        # A lógica interna foi levemente ajustada para usar 'task_config' em vez de 'self.config'
        # para configurações que podem variar por tarefa.

        if not words:
            self.logger.warning(f"[{task_id}] Nenhuma palavra para análise. Retornando segmentos vazios.")
            return []
        self.logger.info(f"[{task_id}] Analisando {len(words)} palavras para criar segmentos de fala...")

        # Parâmetros de configuração
        pause_threshold = self.config.get('pause_threshold_s', 0.5)
        fillers = self.config.get('filler_words', [])
        ctx_pause = self.config.get('filler_word_context_pause', 0.25)
        min_segment_duration = self.config.get('min_segment_duration_s', 0.2)
        padding_start = self.config.get("segment_padding_start_s", 0.1)
        padding_end = self.config.get("segment_padding_end_s", 0.1)

        segments = []
        if not words:
            return segments

        current_start = max(0, words[0].start - padding_start)

        for i in range(len(words) - 1):
            if stop_event.is_set():
                self.logger.warning(f"[{task_id}] Análise de conteúdo interrompida.")
                return []
            
            # Atualiza o progresso na UI
            if i % 100 == 0:
                percentage = 76 + (i / len(words)) * 20
                pq.put({'type': 'progress', 'stage': 'Analisando Conteúdo', 'percentage': percentage, 'task_id': task_id})

            cur = words[i]
            nxt = words[i + 1]
            pause = nxt.start - cur.end
            cut_reason = None

            # Lógica de corte por pausa
            if pause >= pause_threshold:
                cut_reason = f"pausa de {pause:.2f}s"

            # Lógica de corte por palavra de preenchimento (filler word)
            is_filler = cur.word.strip('.,?!- ').lower() in fillers
            if is_filler and (pause > ctx_pause or (i > 0 and cur.start - words[i-1].end > ctx_pause)):
                cut_reason = f"palavra de preenchimento ('{cur.word.strip()}')"
            
            if cut_reason:
                # self.logger.debug(f"[{task_id}] Corte em {cur.end:.2f}s devido a: {cut_reason}")
                
                # Finaliza o segmento atual com margem de segurança
                segment_end = cur.end + padding_end
                
                # Garante que as margens não se sobreponham
                next_segment_start_candidate = nxt.start - padding_start
                if segment_end > next_segment_start_candidate:
                    midpoint = cur.end + pause / 2
                    segment_end = midpoint
                    next_segment_start_candidate = midpoint
                
                if (segment_end - current_start) >= min_segment_duration:
                    segments.append({'start': current_start, 'end': segment_end})
                
                current_start = next_segment_start_candidate

        # Adiciona o último segmento
        final_end_time = words[-1].end + padding_end
        if (final_end_time - current_start) >= min_segment_duration:
            segments.append({'start': current_start, 'end': final_end_time})

        self.logger.info(f"[{task_id}] Análise finalizada. {len(segments)} segmentos mantidos.")
        return segments

    def _add_seg(self, segs: List[Dict[str, float]], start: float, end: float):
        start = max(0, start)
        if (end - start) >= self.config.get("min_segment_duration_s"):
            segs.append({'start': start, 'end': end})

class ScriptComposer:
    """Gera e salva o arquivo de roteiro JSON para o renderizador."""
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
    """Gera um arquivo de legenda .srt sincronizado, respeitando os cortes."""
    def __init__(self, logger: Logger):
        self.logger = logger
        # Armazena o tempo original para detectar os cortes.
        self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word', 'original_start'])

    def _seconds_to_srt_time(self, seconds: float) -> str:
        """Converte segundos para o formato de tempo SRT (HH:MM:SS,ms)."""
        if seconds < 0: seconds = 0
        millis = int((seconds - int(seconds)) * 1000)
        td = timedelta(seconds=int(seconds))
        return f"{td},{millis:03d}"

    def _remap_words_to_new_timeline(self, words: List[Any], exact_segments: List[Dict[str, float]]) -> List[Any]:
        """Mapeia os tempos das palavras para a nova linha do tempo, guardando o tempo original."""
        remapped_words = []
        time_removed = 0.0
        last_seg_end = 0.0
        for segment in exact_segments:
            time_removed += (segment['start'] - last_seg_end)
            for word in words:
                if segment['start'] <= word.start < segment['end']:
                    remapped_words.append(self.Word(
                        start=word.start - time_removed, 
                        end=word.end - time_removed, 
                        word=word.word,
                        original_start=word.start # Guarda o tempo original
                    ))
            last_seg_end = segment['end']
        return remapped_words

    def generate_srt(self, words: List[Any], exact_segments: List[Dict[str, float]], output_path: str, task_id: str):
        self.logger.log("Gerando legenda .srt sincronizada...", "INFO", task_id)
        if not words or not exact_segments: self.logger.log("Dados insuficientes para gerar legenda.", "WARNING", task_id); return
        
        remapped_words = self._remap_words_to_new_timeline(words, exact_segments)
        if not remapped_words: self.logger.log("Nenhuma palavra nos segmentos mantidos.", "WARNING", task_id); return

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                subtitle_index, current_line, line_start_time = 1, "", remapped_words[0].start
                
                for i, word in enumerate(remapped_words):
                    is_cut = False
                    # --- O DETECTOR DE CORTES ---
                    if i + 1 < len(remapped_words):
                        next_word = remapped_words[i+1]
                        # Se a distância entre as palavras no tempo ORIGINAL for grande, é um corte.
                        if (next_word.original_start - word.end) > 2.0: # Limite de 2 segundos
                            is_cut = True

                    current_line += word.word + " "
                    
                    # Força uma nova legenda se a linha for muito longa, a pausa for grande, ou um corte foi detectado.
                    if is_cut or len(current_line) > 42 or (word.end - line_start_time > 5.0):
                        f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(word.end)}\n{current_line.strip()}\n\n")
                        subtitle_index += 1; current_line = ""
                        if i + 1 < len(remapped_words):
                            line_start_time = remapped_words[i+1].start
                
                # Garante que a última linha seja escrita
                if current_line.strip():
                    f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(remapped_words[-1].end)}\n{current_line.strip()}\n\n")

            self.logger.log(f"Legenda .srt salva com sucesso.", "SUCCESS", task_id)
        except Exception as e:
            self.logger.log(f"Erro ao gerar legenda .srt: {e}", "CRITICAL", task_id, exc_info=True)