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
            self.model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root=download_root, local_files_only=True)
            self.logger.log("Modelo encontrado no cache local. Carregamento rápido.", "SUCCESS", task_id)
        except (ValueError, FileNotFoundError):
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

        segments_gen, info = self.model.transcribe(path, language=lang, word_timestamps=True, vad_filter=True)
        self.logger.log(f"Idioma detectado: {info.language} (Probabilidade: {info.language_probability:.2f}), Duração: {info.duration:.2f}s", "INFO", task_id)

        Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
        all_words = []

        for segment in segments_gen:
            if stop_event.is_set():
                self.logger.log("Transcrição interrompida pelo usuário.", "WARNING", task_id)
                return []

            progress = 11 + (segment.end / info.duration) * 39 if info.duration > 0 else 50
            pq.put({'type': 'progress', 'stage': 'Transcrevendo', 'percentage': progress, 'task_id': task_id})

            if hasattr(segment, "words") and segment.words:
                for word_obj in segment.words:
                    all_words.append(Word(start=word_obj.start, end=word_obj.end, word=word_obj.word))

        self.logger.log(f"Transcrição finalizada. Total de {len(all_words)} palavras encontradas.", "SUCCESS", task_id)
        return all_words

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
        self.pose_model = mp.solutions.pose.Pose(min_detection_confidence=0.5, min_tracking_confidence=0.5, model_complexity=1)

    def analyze_video_in_single_pass(self, video_path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Dict]:
        try:
            import cv2
        except ImportError:
            self.logger.log("'opencv-python' não encontrado! Instale com 'pip install opencv-python'", "CRITICAL", task_id)
            return []

        if self.pose_model is None:
            self._init_model(task_id)

        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)

        if fps <= 0 or total_frames <= 0:
            self.logger.log(f"Vídeo inválido ou corrompido: {total_frames} frames, {fps:.2f} FPS.", "ERROR", task_id)
            cap.release()
            return []

        target_fps = self.config.get("visual_analysis_fps")
        process_every = max(1, int(fps / target_fps))
        self.logger.log(f"Análise visual iniciada. Vídeo: {total_frames} frames @ {fps:.2f} FPS. Processando a cada {process_every} frames para atingir ~{target_fps} FPS.", "INFO", task_id)

        results = []
        for frame_idx in range(0, total_frames, process_every):
            if stop_event.is_set():
                self.logger.log("Análise visual interrompida.", "WARNING", task_id)
                break

            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if not ret: break

            pq.put({'type': 'progress', 'stage': 'Análise Visual', 'percentage': 51 + (frame_idx / total_frames) * 24, 'task_id': task_id})
            results.append({"timestamp": (frame_idx / fps), "looking_away": False, "gesturing": False})

        cap.release()
        self.logger.log(f"Análise visual (placeholder) concluída. {len(results)} pontos de dados gerados.", "SUCCESS", task_id)
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

            if i % 100 == 0:
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
    """Gera e salva o arquivo de roteiro em formato JSON padronizado."""
    def __init__(self, logger: Logger):
        self.logger = logger

    def generate_and_save_json(self, segments: List[Dict[str, float]], video_path: str, task_id: str) -> Optional[str]:
        data = { "segments": [{"start": round(s['start'], 3), "end": round(s['end'], 3)} for s in segments] }
        out_path = os.path.splitext(video_path)[0] + ".json"

        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            self.logger.log(f"Roteiro com {len(segments)} segmentos salvo em '{os.path.basename(out_path)}'", "SUCCESS", task_id)
            return out_path
        except IOError as e:
            self.logger.log(f"ERRO ao salvar roteiro JSON em '{out_path}': {e}", "ERROR", task_id)
            return None

class SubtitleGenerator:
    """
    Gera um arquivo de legenda .srt sincronizado com o vídeo editado final.
    """
    def __init__(self, logger: Logger):
        self.logger = logger
        self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])

    def _seconds_to_srt_time(self, seconds: float) -> str:
        """Converte segundos para o formato de tempo SRT (HH:MM:SS,ms)."""
        millis = int((seconds - int(seconds)) * 1000)
        td = timedelta(seconds=int(seconds))
        return f"{td},{millis:03d}"

    def _remap_words_to_new_timeline(self, words: List[Any], segments: List[Dict[str, float]]) -> List[Any]:
        """
        Mapeia os tempos das palavras originais para a nova linha do tempo do vídeo
        editado, subtraindo a duração de todos os trechos cortados.
        """
        remapped_words = []
        time_removed_before_current_segment = 0.0
        last_segment_end = 0.0

        for segment in segments:
            time_removed_before_current_segment += (segment['start'] - last_segment_end)
            
            for word in words:
                if segment['start'] <= word.start < segment['end']:
                    new_start = word.start - time_removed_before_current_segment
                    new_end = word.end - time_removed_before_current_segment
                    remapped_words.append(self.Word(start=new_start, end=new_end, word=word.word))
            
            last_segment_end = segment['end']
            
        return remapped_words

    def generate_srt(self, words: List[Any], segments: List[Dict[str, float]], output_path: str, task_id: str):
        self.logger.log("Iniciando geração de legenda .srt sincronizada...", "INFO", task_id)
        if not words or not segments:
            self.logger.log("Dados de palavras ou segmentos insuficientes para gerar legenda.", "WARNING", task_id)
            return

        remapped_words = self._remap_words_to_new_timeline(words, segments)
        if not remapped_words:
            self.logger.log("Nenhuma palavra encontrada nos segmentos mantidos. Abortando geração de legenda.", "WARNING", task_id)
            return

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                subtitle_index = 1
                current_line = ""
                line_start_time = remapped_words[0].start

                for i, word in enumerate(remapped_words):
                    next_word_start = remapped_words[i+1].start if i + 1 < len(remapped_words) else word.end + 5
                    
                    current_line += word.word + " "
                    
                    if len(current_line) > 42 or (next_word_start - word.end > 1.0) or (word.end - line_start_time > 5.0):
                        f.write(f"{subtitle_index}\n")
                        f.write(f"{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(word.end)}\n")
                        f.write(f"{current_line.strip()}\n\n")
                        
                        subtitle_index += 1
                        current_line = ""
                        if i + 1 < len(remapped_words):
                            line_start_time = remapped_words[i+1].start
                
                if current_line.strip():
                    f.write(f"{subtitle_index}\n")
                    f.write(f"{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(remapped_words[-1].end)}\n")
                    f.write(f"{current_line.strip()}\n\n")

            self.logger.log(f"Legenda .srt sincronizada salva com sucesso em '{os.path.basename(output_path)}'.", "SUCCESS", task_id)
        except IOError as e:
            self.logger.log(f"Erro ao escrever arquivo de legenda .srt: {e}", "ERROR", task_id)
        except Exception as e:
            self.logger.log(f"Erro inesperado na geração da legenda: {e}", "CRITICAL", task_id, exc_info=True)