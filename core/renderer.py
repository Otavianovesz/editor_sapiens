# core/renderer.py

import ffmpeg
import subprocess
import os
from typing import List, Dict

class VideoRenderer:
    def __init__(self, source_path: str, output_path: str, preset: str, logger, task_id: str):
        self.source_path = source_path
        self.output_path = output_path
        self.preset = preset
        self.logger = logger
        self.task_id = task_id

    def _create_clips(self, segments: List[Dict], temp_dir: str):
        clip_paths = []
        total_segments = len(segments)
        for i, seg in enumerate(segments):
            clip_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            clip_paths.append(clip_path)
            # --- CHAMADA DE LOG CORRIGIDA ---
            self.logger.info(f"[{self.task_id}] Criando clipe {i+1}/{total_segments}...")

            try:
                stream = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end'])
                stream = ffmpeg.output(
                    stream, clip_path, vcodec='libx264', acodec='aac',
                    preset=self.preset, crf=23, shortest=None
                )
                args = ffmpeg.compile(stream, overwrite_output=True)
                
                result = subprocess.run(args, capture_output=True, text=True)
                if result.returncode != 0:
                    raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)

            except ffmpeg.Error as e:
                error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
                # --- CHAMADA DE LOG CORRIGIDA ---
                self.logger.error(f"[{self.task_id}] Erro do FFmpeg ao criar clipe {clip_path}: {error_message}")
                raise
        return clip_paths

    def _run_ffmpeg_concat(self, manifest_path: str, total_duration: float):
        # --- CHAMADA DE LOG CORRIGIDA ---
        self.logger.info(f"[{self.task_id}] Concatenando clipes para gerar o vídeo final...")
        
        cmd = [
            'ffmpeg', '-fflags', '+genpts', '-f', 'concat',
            '-safe', '0', '-i', manifest_path, '-y', '-c:v', 'libx264',
            '-preset', self.preset, '-c:a', 'aac', self.output_path
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)
                
        except ffmpeg.Error as e:
            error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
            # --- CHAMADA DE LOG CORRIGIDA ---
            self.logger.error(f"[{self.task_id}] Erro do FFmpeg durante a concatenação final: {error_message}")
            raise

    def render_video(self, segments: List[Dict], temp_dir: str):
        clip_paths = self._create_clips(segments, temp_dir)
        manifest_path = os.path.join(temp_dir, "manifest.txt")
        with open(manifest_path, 'w') as f:
            for clip_path in clip_paths:
                f.write(f"file '{os.path.basename(clip_path)}'\n")
        total_duration = sum(seg['end'] - seg['start'] for seg in segments)
        self._run_ffmpeg_concat(manifest_path, total_duration)

        # --- CHAMADA DE LOG CORRIGIDA ---
        self.logger.info(f"[{self.task_id}] Vídeo renderizado com sucesso em '{self.output_path}'")