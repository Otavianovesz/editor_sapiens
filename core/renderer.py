# core/renderer.py

import ffmpeg
import subprocess
import os
import concurrent.futures
from typing import List, Dict

class VideoRenderer:
    def __init__(self, source_path: str, output_path: str, preset: str, logger, task_id: str):
        self.source_path = source_path
        self.output_path = output_path
        self.preset = preset
        self.logger = logger
        self.task_id = task_id
        self.hw_accel_enabled = True # Inicia tentando usar a GPU

    def _process_segment(self, segment_info: Dict) -> str:
        """
        Processa um único segmento de vídeo para criar um clipe.
        Esta função é projetada para ser executada em paralelo por múltiplos workers.
        """
        i, seg, temp_dir, vcodec = segment_info['i'], segment_info['seg'], segment_info['temp_dir'], segment_info['vcodec']
        
        clip_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
        
        try:
            stream = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end'])
            stream = ffmpeg.output(
                stream, clip_path, vcodec=vcodec, acodec='aac',
                preset=self.preset, crf=23, shortest=None
            )
            args = ffmpeg.compile(stream, overwrite_output=True)
            
            result = subprocess.run(args, capture_output=True, text=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            
            if result.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=result.stdout, stderr=result.stderr)

            return clip_path
        
        except ffmpeg.Error as e:
            # Em caso de erro, retorna o erro para ser logado na thread principal
            error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
            raise RuntimeError(f"Erro no clipe {i}: {error_message}")

    def _create_clips(self, segments: List[Dict], temp_dir: str):
        """
        Cria clipes de vídeo em paralelo usando ThreadPoolExecutor para máxima utilização da CPU.
        """
        total_segments = len(segments)
        vcodec = "h264_nvenc" if self.hw_accel_enabled else "libx264"
        self.logger.info(f"[{self.task_id}] Iniciando criação de {total_segments} clipes com codec: {vcodec} e preset: {self.preset}")

        tasks = [{'i': i, 'seg': seg, 'temp_dir': temp_dir, 'vcodec': vcodec} for i, seg in enumerate(segments)]
        clip_paths = [None] * total_segments
        
        # Usa até o número de núcleos da CPU, mas no máximo 16 para não sobrecarregar o sistema
        max_workers = min(os.cpu_count() or 1, 16)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(self._process_segment, task): task['i'] for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    clip_path = future.result()
                    clip_paths[index] = clip_path
                    self.logger.info(f"[{self.task_id}] Clipe {index + 1}/{total_segments} criado com sucesso.")
                except Exception as exc:
                    # Se houver uma falha com NVENC, faz o fallback para CPU e reinicia o processo
                    if vcodec == "h264_nvenc":
                        self.logger.warning(f"[{self.task_id}] Falha ao usar {vcodec}. Cancelando e reiniciando com libx264 (CPU)...")
                        executor.shutdown(wait=False, cancel_futures=True) # Cancela todas as outras tarefas
                        self.hw_accel_enabled = False # Desativa a GPU para a nova tentativa
                        return self._create_clips(segments, temp_dir) # Chama a si mesma novamente com a GPU desativada
                    else:
                        self.logger.error(f"[{self.task_id}] Erro fatal ao criar clipe {index + 1}: {exc}")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise # Interrompe todo o processo se a CPU também falhar

        # Verifica se todos os clipes foram criados com sucesso
        if any(p is None for p in clip_paths):
            raise RuntimeError("Nem todos os clipes foram criados com sucesso.")
            
        return clip_paths

    def _run_ffmpeg_concat(self, manifest_path: str):
        self.logger.info(f"[{self.task_id}] Concatenando clipes para gerar o vídeo final...")
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
            for i in range(len(clip_paths)):
                f.write(f"file 'clip_{i:04d}.mp4'\n")
        self._run_ffmpeg_concat(manifest_path)
        self.logger.info(f"[{self.task_id}] Vídeo renderizado com sucesso em '{self.output_path}'")