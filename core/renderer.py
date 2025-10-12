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
        # Tenta detectar se a aceleração por hardware NVIDIA (NVENC) está disponível.
        # Isso pode ser aprimorado com uma verificação real do ffmpeg no futuro.
        self.hw_accel_enabled = True 

    def _create_clips(self, segments: List[Dict], temp_dir: str):
        """
        Cria clipes de vídeo individuais.
        MODIFICADO: Agora utiliza aceleração por hardware (NVENC) para velocidade
        e o preset do usuário para otimização, mantendo a precisão do corte.
        """
        clip_paths = []
        total_segments = len(segments)

        # Seleciona o codec de vídeo: h264_nvenc para GPU NVIDIA, com fallback para libx264 (CPU)
        vcodec = "h264_nvenc" if self.hw_accel_enabled else "libx264"
        self.logger.info(f"[{self.task_id}] Usando codec de vídeo: {vcodec} com preset: {self.preset}")

        for i, seg in enumerate(segments):
            clip_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            clip_paths.append(clip_path)
            self.logger.info(f"[{self.task_id}] Criando clipe {i+1}/{total_segments}...")

            try:
                stream = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end'])
                
                # --- LÓGICA DE CORTE OTIMIZADA ---
                stream = ffmpeg.output(
                    stream,
                    clip_path,
                    vcodec=vcodec,           # Usa o codec de hardware (GPU) ou CPU
                    acodec='aac',            # Codec de áudio consistente
                    preset=self.preset,      # Aplica o preset de velocidade do usuário
                    crf=23,                  # Fator de qualidade (ignor_if_nvenc)
                    shortest=None
                )
                args = ffmpeg.compile(stream, overwrite_output=True)
                
                result = subprocess.run(args, capture_output=True, text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                if result.returncode != 0:
                    # Se o codec de hardware falhar, tenta novamente com o codec de CPU (fallback)
                    if vcodec == "h264_nvenc":
                        self.logger.warning(f"[{self.task_id}] Falha ao usar {vcodec}, tentando novamente com libx264 (CPU)...")
                        self.hw_accel_enabled = False # Desativa para os próximos clipes
                        vcodec = "libx264"
                        
                        stream_fallback = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end'])
                        stream_fallback = ffmpeg.output(
                            stream_fallback, clip_path, vcodec=vcodec, acodec='aac',
                            preset=self.preset, crf=23, shortest=None
                        )
                        args_fallback = ffmpeg.compile(stream_fallback, overwrite_output=True)
                        result = subprocess.run(args_fallback, capture_output=True, text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                        
                        if result.returncode != 0:
                           raise ffmpeg.Error('ffmpeg_fallback', stdout=result.stdout, stderr=result.stderr)
                    else:
                        raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)

            except ffmpeg.Error as e:
                error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
                self.logger.error(f"[{self.task_id}] Erro do FFmpeg ao criar clipe {clip_path}: {error_message}")
                raise
        return clip_paths

    def _run_ffmpeg_concat(self, manifest_path: str, total_duration: float):
        self.logger.info(f"[{self.task_id}] Concatenando clipes para gerar o vídeo final...")
        
        # O codec de concatenação final também pode ser acelerado por hardware
        vcodec = "h264_nvenc" if self.hw_accel_enabled else "libx264"
        
        cmd = [
            'ffmpeg', '-fflags', '+genpts', '-f', 'concat',
            '-safe', '0', '-i', manifest_path, '-y', '-c:v', vcodec,
            '-preset', self.preset, '-c:a', 'aac', self.output_path
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)
                
        except ffmpeg.Error as e:
            error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
            self.logger.error(f"[{self.task_id}] Erro do FFmpeg durante a concatenação final: {error_message}")
            raise

    def render_video(self, segments: List[Dict], temp_dir: str):
        clip_paths = self._create_clips(segments, temp_dir)
        manifest_path = os.path.join(temp_dir, "manifest.txt")
        with open(manifest_path, 'w') as f:
            # Garante que os caminhos usem a barra correta para o FFmpeg
            for clip_path in clip_paths:
                f.write(f"file '{os.path.basename(clip_path)}'\n")
        total_duration = sum(seg['end'] - seg['start'] for seg in segments)
        self._run_ffmpeg_concat(manifest_path, total_duration)

        self.logger.info(f"[{self.task_id}] Vídeo renderizado com sucesso em '{self.output_path}'")