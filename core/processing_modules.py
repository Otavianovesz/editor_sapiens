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

# --- Subtitle Analysis Module (SRT/VTT) ---

class SubtitleParser:
    """
    Responsible for reading subtitle files (.srt, .vtt) and converting them
    to the word list format compatible with ContentAnalyzer.
    """
    def __init__(self, logger: Logger):
        """Initializes the SubtitleParser.

        Args:
            logger (Logger): The logger instance.
        """
        self.logger = logger
        self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])

    def _time_str_to_seconds(self, time_str: str) -> float:
        """Converts a time string (HH:MM:SS,ms) to seconds.

        Args:
            time_str (str): The time string.

        Returns:
            float: The time in seconds.
        """
        time_str = time_str.replace(',', '.')
        try:
            dt = datetime.strptime(time_str, '%H:%M:%S.%f')
        except ValueError:
            dt = datetime.strptime(time_str, '%H:%M:%S')
        return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond).total_seconds()

    def parse(self, file_path: str, task_id: str) -> List[Any]:
        """Reads the subtitle file and returns a list of 'words'.

        Args:
            file_path (str): The path to the subtitle file.
            task_id (str): The ID of the task.

        Returns:
            List[Any]: A list of 'word' objects.
        """
        self.logger.log(f"Starting analysis of subtitle file: {os.path.basename(file_path)}", "INFO", task_id)
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

            self.logger.log(f"Subtitle analysis completed. {len(words)} words extracted.", "SUCCESS", task_id)
            return words

        except FileNotFoundError:
            self.logger.log(f"Subtitle file not found: {file_path}", "ERROR", task_id)
            return []
        except Exception as e:
            self.logger.log(f"Unexpected error when analyzing subtitle file: {e}", "CRITICAL", task_id, exc_info=True)
            return []


# --- Media and AI Processing Modules ---

class MediaProcessor:
    """Responsible for extracting audio from video files."""
    def __init__(self, logger: Logger):
        """Initializes the MediaProcessor.

        Args:
            logger (Logger): The logger instance.
        """
        self.logger = logger

    def extract_audio(self, video_filepath: str, task_id: str) -> Optional[str]:
        """Extracts audio from a video file.

        Args:
            video_filepath (str): The path to the video file.
            task_id (str): The ID of the task.

        Returns:
            Optional[str]: The path to the extracted audio file, or None if it fails.
        """
        self.logger.log(f"Starting audio extraction from '{os.path.basename(video_filepath)}'.", "INFO", task_id)
        try:
            import ffmpeg
        except ImportError:
            self.logger.log("Module 'ffmpeg-python' not found! Install with 'pip install ffmpeg-python'", "CRITICAL", task_id)
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
            self.logger.log(f"Audio successfully extracted to '{output_audio_path}' (Size: {file_size:.2f} MB).", "SUCCESS", task_id)
            return output_audio_path
        except subprocess.TimeoutExpired:
            self.logger.log("Timeout: Audio extraction took more than 10 minutes and was canceled.", "ERROR", task_id)
            return None
        except ffmpeg.Error as e:
            self.logger.log(f"FFmpeg ERROR during extraction: {e.stderr.decode() if e.stderr else e}", "ERROR", task_id)
            return None
        except Exception as e:
            self.logger.log(f"Unexpected ERROR during audio extraction: {e}", "CRITICAL", task_id, exc_info=True)
            return None

    def cleanup(self, path: Optional[str], task_id: str):
        """Cleans up temporary files.

        Args:
            path (Optional[str]): The path to the temporary file.
            task_id (str): The ID of the task.
        """
        if path and os.path.exists(path):
            try:
                os.remove(path)
                self.logger.log(f"Temporary file '{path}' removed.", "DEBUG", task_id)
            except OSError as e:
                self.logger.log(f"Failed to remove temporary file '{path}': {e}", "WARNING", task_id)

class AudioTranscriber:
    """Singleton implementation for the audio transcriber."""
    _instance = None; _model = None; _model_lock = threading.Lock()
    def __new__(cls, *args, **kwargs):
        """Creates a new instance of the AudioTranscriber.

        Returns:
            AudioTranscriber: The new instance.
        """
        if cls._instance is None: cls._instance = super(AudioTranscriber, cls).__new__(cls)
        return cls._instance
    def __init__(self, logger: Logger, config: Config):
        """Initializes the AudioTranscriber.

        Args:
            logger (Logger): The logger instance.
            config (Config): The configuration instance.
        """
        if not hasattr(self, 'logger'): self.logger = logger; self.config = config
    def _load_model(self, pq: queue.Queue, task_id: str):
        """Loads the transcription model.

        Args:
            pq (queue.Queue): The progress queue.
            task_id (str): The ID of the task.
        """
        with self._model_lock:
            if self._model is not None: return
            try: from faster_whisper import WhisperModel
            except ImportError: self.logger.log("'faster_whisper' not found.", "CRITICAL", task_id); raise
            model_name, device, compute_type = self.config.get("whisper_model_size"), self.config.get("whisper_device"), self.config.get("whisper_compute_type")
            pq.put({'type': 'progress', 'stage': f'Loading Model {model_name}', 'percentage': 1, 'task_id': task_id})
            try:
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root="models", local_files_only=True)
                self.logger.log("Model loaded from local cache.", "SUCCESS", task_id)
            except (ValueError, FileNotFoundError):
                pq.put({'type': 'progress', 'stage': f'Downloading Model...', 'percentage': 2, 'task_id': task_id})
                self.logger.log(f"Downloading model '{model_name}'. This may take several minutes.", "WARNING", task_id)
                self._model = WhisperModel(model_name, device=device, compute_type=compute_type, download_root="models", local_files_only=False)
            except Exception as e: self.logger.log(f"CRITICAL ERROR when loading model: {e}", "CRITICAL", task_id, exc_info=True); self._model = None; raise
    def release_gpu_memory(self):
        """Releases the GPU memory."""
        if self.config.get("whisper_device") == "cuda":
            try: import torch; torch.cuda.empty_cache(); self.logger.log("GPU VRAM cache cleared.", "DEBUG")
            except Exception as e: self.logger.log(f"Failed to clear GPU cache: {e}", "ERROR")
    def transcribe(self, path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Any]:
        """Transcribes an audio file.

        Args:
            path (str): The path to the audio file.
            pq (queue.Queue): The progress queue.
            task_id (str): The ID of the task.
            stop_event (threading.Event): The event to stop the transcription.

        Returns:
            List[Any]: A list of 'word' objects.
        """
        if self._model is None: self._load_model(pq, task_id)
        if self._model is None: raise RuntimeError("Transcription model not loaded.")
        lang = self.config.get("whisper_language") or None
        segments_gen, info = self._model.transcribe(path, language=lang, word_timestamps=True, vad_filter=True)
        self.logger.log(f"Language: {info.language} (Prob: {info.language_probability:.2f}), Duration: {info.duration:.2f}s", "INFO", task_id)
        Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word'])
        all_words = []
        for s in segments_gen:
            if stop_event.is_set(): self.logger.log("Transcription interrupted.", "WARNING", task_id); return []
            pq.put({'type': 'progress', 'stage': 'Transcribing', 'percentage': 11 + (s.end / info.duration) * 39 if info.duration > 0 else 50, 'task_id': task_id})
            if s.words: all_words.extend([Word(start=w.start, end=w.end, word=w.word) for w in s.words])
        self.logger.log(f"Transcription finished with {len(all_words)} words.", "SUCCESS", task_id)
        return all_words

class VisualAnalyzer:
    """Responsible for visual analysis (placeholder)."""
    def __init__(self, logger: Logger, config: Config):
        """Initializes the VisualAnalyzer.

        Args:
            logger (Logger): The logger instance.
            config (Config): The configuration instance.
        """
        self.logger, self.config, self.pose_model = logger, config, None
    def _init_model(self, task_id):
        """Initializes the visual analysis model.

        Args:
            task_id (str): The ID of the task.
        """
        try: import mediapipe as mp; self.pose_model = mp.solutions.pose.Pose(min_detection_confidence=0.5, min_tracking_confidence=0.5, model_complexity=1)
        except ImportError: self.logger.log("'mediapipe' not found.", "CRITICAL", task_id); raise
    def analyze_video_in_single_pass(self, video_path: str, pq: queue.Queue, task_id: str, stop_event: threading.Event) -> List[Dict]:
        """Analyzes a video in a single pass.

        Args:
            video_path (str): The path to the video file.
            pq (queue.Queue): The progress queue.
            task_id (str): The ID of the task.
            stop_event (threading.Event): The event to stop the analysis.

        Returns:
            List[Dict]: A list of dictionaries with the analysis results.
        """
        try: import cv2
        except ImportError: self.logger.log("'opencv-python' not found.", "CRITICAL", task_id); return []
        if self.pose_model is None: self._init_model(task_id)
        cap = cv2.VideoCapture(video_path)
        total_frames, fps = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0 or total_frames <= 0: self.logger.log("Invalid video.", "ERROR", task_id); cap.release(); return []
        process_every = max(1, int(fps / self.config.get("visual_analysis_fps")))
        results = []
        for frame_idx in range(0, total_frames, process_every):
            if stop_event.is_set(): break
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx); ret, _ = cap.read()
            if not ret: break
            pq.put({'type': 'progress', 'stage': 'Visual Analysis', 'percentage': 51 + (frame_idx / total_frames) * 24, 'task_id': task_id})
            results.append({"timestamp": (frame_idx / fps), "looking_away": False, "gesturing": False})
        cap.release(); self.logger.log("Visual analysis (placeholder) completed.", "SUCCESS", task_id)
        return results

class ContentAnalyzer:
    """
    Analyzes the transcription and generates two lists of segments: one with padding
    for smooth video cuts, and another with exact times for subtitle synchronization.
    """
    def __init__(self, logger: Logger, config: Config):
        """Initializes the ContentAnalyzer.

        Args:
            logger (Logger): The logger instance.
            config (Config): The configuration instance.
        """
        self.logger = logger
        self.config = config

    def create_speech_segments(self, words: List[Any], pq: queue.Queue, task_config: Dict, task_id: str, stop_event: threading.Event) -> List[Dict[str, float]]:
        """Creates speech segments from a list of words.

        Args:
            words (List[Any]): A list of 'word' objects.
            pq (queue.Queue): The progress queue.
            task_config (Dict): The configuration for the task.
            task_id (str): The ID of the task.
            stop_event (threading.Event): The event to stop the analysis.

        Returns:
            List[Dict[str, float]]: A list of speech segments.
        """
        # The signature has been corrected to accept all arguments sent by the Orchestrator.
        # The internal logic has been slightly adjusted to use 'task_config' instead of 'self.config'
        # for settings that may vary by task.

        if not words:
            self.logger.warning(f"[{task_id}] No words to analyze. Returning empty segments.")
            return []
        self.logger.info(f"[{task_id}] Analyzing {len(words)} words to create speech segments...")

        # Configuration parameters
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
                self.logger.warning(f"[{task_id}] Content analysis interrupted.")
                return []
            
            # Updates the progress in the UI
            if i % 100 == 0:
                percentage = 76 + (i / len(words)) * 20
                pq.put({'type': 'progress', 'stage': 'Analyzing Content', 'percentage': percentage, 'task_id': task_id})

            cur = words[i]
            nxt = words[i + 1]
            pause = nxt.start - cur.end
            cut_reason = None

            # Pause cutting logic
            if pause >= pause_threshold:
                cut_reason = f"pause of {pause:.2f}s"

            # Filler word cutting logic
            is_filler = cur.word.strip('.,?!- ').lower() in fillers
            if is_filler and (pause > ctx_pause or (i > 0 and cur.start - words[i-1].end > ctx_pause)):
                cut_reason = f"filler word ('{cur.word.strip()}')"
            
            if cut_reason:
                # self.logger.debug(f"[{task_id}] Cut at {cur.end:.2f}s due to: {cut_reason}")
                
                # Finishes the current segment with a safety margin
                segment_end = cur.end + padding_end
                
                # Ensures that the margins do not overlap
                next_segment_start_candidate = nxt.start - padding_start
                if segment_end > next_segment_start_candidate:
                    midpoint = cur.end + pause / 2
                    segment_end = midpoint
                    next_segment_start_candidate = midpoint
                
                if (segment_end - current_start) >= min_segment_duration:
                    segments.append({'start': current_start, 'end': segment_end})
                
                current_start = next_segment_start_candidate

        # Adds the last segment
        final_end_time = words[-1].end + padding_end
        if (final_end_time - current_start) >= min_segment_duration:
            segments.append({'start': current_start, 'end': final_end_time})

        self.logger.info(f"[{task_id}] Analysis finished. {len(segments)} segments kept.")
        return segments

    def _add_seg(self, segs: List[Dict[str, float]], start: float, end: float):
        """Adds a segment to the list of segments.

        Args:
            segs (List[Dict[str, float]]): The list of segments.
            start (float): The start time of the segment.
            end (float): The end time of the segment.
        """
        start = max(0, start)
        if (end - start) >= self.config.get("min_segment_duration_s"):
            segs.append({'start': start, 'end': end})

class ScriptComposer:
    """Generates and saves the JSON script file for the renderer."""
    def __init__(self, logger: Logger):
        """Initializes the ScriptComposer.

        Args:
            logger (Logger): The logger instance.
        """
        self.logger = logger
    def generate_and_save_json(self, segments: List[Dict[str, float]], video_path: str, task_id: str) -> Optional[str]:
        """Generates and saves the JSON script file.

        Args:
            segments (List[Dict[str, float]]): The list of segments.
            video_path (str): The path to the video file.
            task_id (str): The ID of the task.

        Returns:
            Optional[str]: The path to the JSON script file, or None if it fails.
        """
        data = { "segments": [{"start": round(s['start'], 3), "end": round(s['end'], 3)} for s in segments] }
        out_path = os.path.splitext(video_path)[0] + ".json"
        try:
            with open(out_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
            self.logger.log(f"Script with {len(segments)} segments saved.", "SUCCESS", task_id)
            return out_path
        except IOError as e: self.logger.log(f"ERROR when saving JSON script: {e}", "ERROR", task_id); return None

class SubtitleGenerator:
    """Generates a synchronized .srt subtitle file, respecting the cuts."""
    def __init__(self, logger: Logger):
        """Initializes the SubtitleGenerator.

        Args:
            logger (Logger): The logger instance.
        """
        self.logger = logger
        # Stores the original time to detect cuts.
        self.Word = dataclasses.make_dataclass('Word', ['start', 'end', 'word', 'original_start'])

    def _seconds_to_srt_time(self, seconds: float) -> str:
        """Converts seconds to the SRT time format (HH:MM:SS,ms).

        Args:
            seconds (float): The time in seconds.

        Returns:
            str: The time in SRT format.
        """
        if seconds < 0: seconds = 0
        millis = int((seconds - int(seconds)) * 1000)
        td = timedelta(seconds=int(seconds))
        return f"{td},{millis:03d}"

    def _remap_words_to_new_timeline(self, words: List[Any], exact_segments: List[Dict[str, float]]) -> List[Any]:
        """Remaps the word times to the new timeline, keeping the original time.

        Args:
            words (List[Any]): A list of 'word' objects.
            exact_segments (List[Dict[str, float]]): The list of exact segments.

        Returns:
            List[Any]: A list of remapped 'word' objects.
        """
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
                        original_start=word.start # Keeps the original time
                    ))
            last_seg_end = segment['end']
        return remapped_words

    def generate_srt(self, words: List[Any], exact_segments: List[Dict[str, float]], output_path: str, task_id: str):
        """Generates a synchronized .srt subtitle file.

        Args:
            words (List[Any]): A list of 'word' objects.
            exact_segments (List[Dict[str, float]]): The list of exact segments.
            output_path (str): The path to the output file.
            task_id (str): The ID of the task.
        """
        self.logger.log("Generating synchronized .srt subtitle...", "INFO", task_id)
        if not words or not exact_segments: self.logger.log("Insufficient data to generate subtitle.", "WARNING", task_id); return
        
        remapped_words = self._remap_words_to_new_timeline(words, exact_segments)
        if not remapped_words: self.logger.log("No words in the kept segments.", "WARNING", task_id); return

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                subtitle_index, current_line, line_start_time = 1, "", remapped_words[0].start
                
                for i, word in enumerate(remapped_words):
                    is_cut = False
                    # --- THE CUT DETECTOR ---
                    if i + 1 < len(remapped_words):
                        next_word = remapped_words[i+1]
                        # If the distance between the words in the ORIGINAL time is large, it is a cut.
                        if (next_word.original_start - word.end) > 2.0: # 2-second limit
                            is_cut = True

                    current_line += word.word + " "
                    
                    # Forces a new subtitle if the line is too long, the pause is large, or a cut has been detected.
                    if is_cut or len(current_line) > 42 or (word.end - line_start_time > 5.0):
                        f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(word.end)}\n{current_line.strip()}\n\n")
                        subtitle_index += 1; current_line = ""
                        if i + 1 < len(remapped_words):
                            line_start_time = remapped_words[i+1].start
                
                # Ensures that the last line is written
                if current_line.strip():
                    f.write(f"{subtitle_index}\n{self._seconds_to_srt_time(line_start_time)} --> {self._seconds_to_srt_time(remapped_words[-1].end)}\n{current_line.strip()}\n\n")

            self.logger.log(f".srt subtitle successfully saved.", "SUCCESS", task_id)
        except Exception as e:
            self.logger.log(f"Error when generating .srt subtitle: {e}", "CRITICAL", task_id, exc_info=True)
