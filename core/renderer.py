# -*- coding: utf-8 -*-

import os
import re
import json
import tempfile
import subprocess
import platform
from datetime import datetime, timedelta
from queue import Queue
from threading import Event
from typing import Union

from utils.logger import Logger

class VideoRenderer:
    """
    Executa a renderização de um vídeo com base em um arquivo de roteiro JSON,
    utilizando FFmpeg para cortar e concatenar os segmentos.
    """
    def __init__(self, source_path: str, json_path: str, output_path: str, preset: str, logger: Logger, pq: Queue, task_id: str):
        self.source_path = source_path
        self.json_path = json_path
        self.output_path = output_path
        self.preset = preset
        self.logger = logger
        self.pq = pq
        self.task_id = task_id
        self.process = None

    def interrupt(self):
        """Tenta interromper o processo FFmpeg em andamento."""
        if self.process and self.process.poll() is None:
            self.logger.log("Enviando sinal de interrupção para o FFmpeg...", "WARNING", self.task_id)
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def _time_to_seconds(self, time_val: Union[int, float, str]) -> float:
        """Converte diferentes formatos de tempo para segundos (float)."""
        if isinstance(time_val, (int, float)):
            return float(time_val)
        if isinstance(time_val, str):
            try:
                dt = datetime.strptime(time_val, '%H:%M:%S.%f')
            except ValueError:
                dt = datetime.strptime(time_val, '%H:%M:%S')
            return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond).total_seconds()
        raise TypeError(f"Formato de tempo '{time_val}' não suportado.")

    def run(self, stop_event: Event):
        """Inicia o processo de renderização."""
        try:
            self.logger.log(f"Iniciando renderização de '{os.path.basename(self.source_path)}' para '{os.path.basename(self.output_path)}' com preset '{self.preset}'.", "INFO", self.task_id)
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                script_data = json.load(f)
            
            segments = script_data.get("segments", [])
            if not segments: raise ValueError("Arquivo de roteiro não contém segmentos.")
            
            self.logger.log(f"Roteiro '{os.path.basename(self.json_path)}' carregado com {len(segments)} segmentos.", "INFO", self.task_id)
            
            valid_segs = [{'start': self._time_to_seconds(s['start_time']), 'end': self._time_to_seconds(s['end_time'])} for s in segments]
            total_dur = sum(s['end'] - s['start'] for s in valid_segs if s['end'] > s['start'])
            if total_dur <= 0: raise ValueError("Duração total do vídeo editado é zero.")
            
            self.logger.log(f"Duração total do vídeo final calculada: {total_dur:.2f}s.", "INFO", self.task_id)

            with tempfile.TemporaryDirectory() as temp_dir:
                clip_files = self._create_clips(valid_segs, temp_dir, stop_event)
                if not clip_files: raise ValueError("Nenhum clipe válido foi gerado.")
                
                manifest_path = os.path.join(temp_dir, "manifest.txt")
                with open(manifest_path, 'w', encoding='utf-8') as f:
                    for clip in clip_files:
                        f.write(f"file '{os.path.basename(clip)}'\n")
                        
                self._run_ffmpeg_concat(manifest_path, temp_dir, total_dur, stop_event)
                
                if not stop_event.is_set():
                    self.pq.put({'type': 'done', 'task_id': self.task_id})

        except InterruptedError:
            self.pq.put({'type': 'interrupted', 'task_id': self.task_id})
        except Exception as e:
            self.logger.log(f"Erro fatal na renderização: {e}", "CRITICAL", self.task_id, exc_info=True)
            self.pq.put({'type': 'error', 'message': str(e), 'task_id': self.task_id})

    def _create_clips(self, segments, temp_dir, stop_event):
        try:
            import ffmpeg
        except ImportError:
            self.logger.log("Módulo 'ffmpeg-python' não encontrado!", "CRITICAL", self.task_id)
            raise InterruptedError()
            
        clip_files = []
        self.logger.log(f"Criando {len(segments)} clipes temporários em '{temp_dir}'...", "DEBUG", self.task_id)
        creation_flags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
        
        for i, seg in enumerate(segments):
            if stop_event.is_set(): raise InterruptedError()
            if seg['end'] <= seg['start']: continue
            
            clip_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            stream = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end'])
            stream = ffmpeg.output(stream, clip_path,
                       vcodec='libx264',
                       preset='veryfast',
                       crf='23',
                       acodec='aac',
                       strict='experimental')
            args = ffmpeg.compile(stream, overwrite_output=True)
            result = subprocess.run(args, capture_output=True, creationflags=creation_flags)
            
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)
            clip_files.append(clip_path)
            
        return clip_files

    def _run_ffmpeg_concat(self, manifest, temp_dir, total_dur, stop_event):
        cmd = ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', manifest, '-y', '-c:v', 'libx264', '-preset', self.preset, '-c:a', 'aac', self.output_path]
        self.logger.log(f"Executando comando de concatenação FFmpeg: {' '.join(cmd)}", "INFO", self.task_id)
        creation_flags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
        
        self.process = subprocess.Popen(cmd, cwd=temp_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, creationflags=creation_flags, encoding='utf-8', errors='ignore')
        
        time_regex = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})")
        for line in self.process.stdout:
            if stop_event.is_set():
                self.interrupt()
                raise InterruptedError()
                
            if match := time_regex.search(line):
                h, m, s, cs = map(int, match.groups())
                current = h * 3600 + m * 60 + s + cs / 100
                if total_dur > 0:
                    self.pq.put({'type': 'progress', 'stage': 'Renderizando', 'percentage': min(25 + (current / total_dur) * 74, 99), 'task_id': self.task_id})
                    
        self.process.wait()
        
        if self.process.returncode != 0 and not stop_event.is_set():
            raise RuntimeError(f"FFmpeg falhou com código {self.process.returncode}.")
            
        self.logger.log(f"Renderização para '{self.output_path}' concluída com sucesso.", "SUCCESS", self.task_id)
