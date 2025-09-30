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
from typing import Union, List, Dict, Any

from utils.logger import Logger

class VideoRenderer:
    """
    Executa a renderização de um vídeo com base em um arquivo de roteiro JSON,
    utilizando FFmpeg para cortar e concatenar os segmentos.
    Esta versão foi otimizada para ser mais robusta e lidar com múltiplos
    formatos de tempo no roteiro.
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
        """Tenta interromper o processo FFmpeg em andamento de forma segura."""
        if self.process and self.process.poll() is None:
            self.logger.log("Enviando sinal de interrupção para o FFmpeg...", "WARNING", self.task_id)
            try:
                # Tenta terminar o processo graciosamente primeiro
                self.process.terminate()
                self.process.wait(timeout=5)
                self.logger.log("Processo FFmpeg terminado com sucesso.", "INFO", self.task_id)
            except subprocess.TimeoutExpired:
                # Força a finalização se o processo não responder
                self.logger.log("Processo FFmpeg não respondeu. Forçando finalização...", "WARNING", self.task_id)
                self.process.kill()
            except Exception as e:
                self.logger.log(f"Erro ao tentar interromper o processo FFmpeg: {e}", "ERROR", self.task_id)


    def _time_to_seconds(self, time_val: Union[int, float, str]) -> float:
        """
        Converte diferentes formatos de tempo para segundos (float).
        É robusto para lidar com números (int/float) ou strings no formato H:M:S.ms.
        """
        if isinstance(time_val, (int, float)):
            return float(time_val)
        if isinstance(time_val, str):
            try:
                # Tenta o formato com milissegundos primeiro
                dt = datetime.strptime(time_val, '%H:%M:%S.%f')
            except ValueError:
                # Fallback para o formato sem milissegundos
                try:
                    dt = datetime.strptime(time_val, '%H:%M:%S')
                except ValueError:
                     # Se falhar, tenta converter diretamente para float (caso seja uma string numérica)
                    return float(time_val)
            return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond).total_seconds()
        raise TypeError(f"Formato de tempo '{time_val}' (tipo: {type(time_val)}) não é suportado.")

    def _parse_segments(self, segments_data: List[Dict[str, Any]]) -> List[Dict[str, float]]:
        """
        Analisa a lista de segmentos do JSON, oferecendo compatibilidade com
        múltiplos formatos de chave ('start'/'end' vs 'start_time'/'end_time').
        Isso previne o erro crítico 'KeyError' e garante retrocompatibilidade.
        """
        parsed_segments = []
        if not segments_data:
            return []

        # Detecta o formato do primeiro segmento para aplicar a todos
        first_seg = segments_data[0]
        if 'start' in first_seg and 'end' in first_seg:
            start_key, end_key = 'start', 'end'
            self.logger.log("Detectado formato de roteiro padrão ('start'/'end').", "DEBUG", self.task_id)
        elif 'start_time' in first_seg and 'end_time' in first_seg:
            start_key, end_key = 'start_time', 'end_time'
            self.logger.log("Detectado formato de roteiro legado ('start_time'/'end_time'). Aplicando camada de compatibilidade.", "WARNING", self.task_id)
        else:
            raise KeyError("O roteiro JSON não contém as chaves de tempo esperadas ('start'/'end' ou 'start_time'/'end_time').")

        for seg in segments_data:
            try:
                start_time = self._time_to_seconds(seg[start_key])
                end_time = self._time_to_seconds(seg[end_key])
                if end_time > start_time:
                    parsed_segments.append({'start': start_time, 'end': end_time})
            except (KeyError, TypeError, ValueError) as e:
                self.logger.log(f"Segmento inválido ou corrompido ignorado: {seg}. Erro: {e}", "WARNING", self.task_id)
                continue
        return parsed_segments


    def run(self, stop_event: Event):
        """Inicia o processo de renderização."""
        try:
            self.logger.log(f"Iniciando renderização de '{os.path.basename(self.source_path)}' para '{os.path.basename(self.output_path)}' com preset '{self.preset}'.", "INFO", self.task_id)

            with open(self.json_path, 'r', encoding='utf-8') as f:
                script_data = json.load(f)

            segments = script_data.get("segments", [])
            if not segments:
                raise ValueError("Arquivo de roteiro não contém segmentos ou a chave 'segments' está ausente.")

            self.logger.log(f"Roteiro '{os.path.basename(self.json_path)}' carregado com {len(segments)} segmentos. Analisando e validando...", "INFO", self.task_id)

            valid_segs = self._parse_segments(segments)
            if not valid_segs:
                raise ValueError("Nenhum segmento válido encontrado no roteiro após análise.")

            total_dur = sum(s['end'] - s['start'] for s in valid_segs)
            if total_dur <= 0:
                raise ValueError(f"Duração total do vídeo editado é zero ou negativa ({total_dur:.2f}s).")

            self.logger.log(f"Duração total do vídeo final calculada: {total_dur:.2f}s.", "INFO", self.task_id)

            with tempfile.TemporaryDirectory() as temp_dir:
                clip_files = self._create_clips(valid_segs, temp_dir, stop_event)
                if stop_event.is_set(): raise InterruptedError()
                if not clip_files: raise ValueError("Nenhum clipe de vídeo válido foi gerado a partir dos segmentos.")

                manifest_path = os.path.join(temp_dir, "manifest.txt")
                with open(manifest_path, 'w', encoding='utf-8') as f:
                    for clip in clip_files:
                        # Garante que os caminhos sejam seguros para o ffmpeg
                        safe_path = os.path.basename(clip).replace("'", "'\\''")
                        f.write(f"file '{safe_path}'\n")

                self._run_ffmpeg_concat(manifest_path, temp_dir, total_dur, stop_event)

                if not stop_event.is_set():
                    self.pq.put({'type': 'done', 'task_id': self.task_id})

        except InterruptedError:
            self.logger.log("Renderização foi interrompida pelo usuário.", "WARNING", self.task_id)
            self.pq.put({'type': 'interrupted', 'task_id': self.task_id})
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            self.logger.log(f"Erro de configuração ou de roteiro na renderização: {e}", "ERROR", self.task_id)
            self.pq.put({'type': 'error', 'message': f"Erro de Roteiro/Config: {e}", 'task_id': self.task_id})
        except Exception as e:
            self.logger.log(f"Erro fatal e inesperado na renderização: {e}", "CRITICAL", self.task_id, exc_info=True)
            self.pq.put({'type': 'error', 'message': str(e), 'task_id': self.task_id})

    def _create_clips(self, segments, temp_dir, stop_event):
        """Cria os clipes de vídeo individuais a partir do vídeo original."""
        try:
            import ffmpeg
        except ImportError:
            self.logger.log("Módulo 'ffmpeg-python' não encontrado! Instale com 'pip install ffmpeg-python'", "CRITICAL", self.task_id)
            raise InterruptedError() # Interrompe se a dependência chave faltar

        clip_files = []
        self.logger.log(f"Criando {len(segments)} clipes temporários em '{temp_dir}'...", "INFO", self.task_id)
        creation_flags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0

        for i, seg in enumerate(segments):
            if stop_event.is_set(): raise InterruptedError()

            clip_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            try:
                # Usar -c copy é rápido, mas pode causar problemas se os cortes não forem em keyframes.
                # Uma abordagem mais segura seria re-encodar, mas é muito mais lenta.
                # Mantendo -c copy por eficiência, que é o objetivo principal.
                stream = ffmpeg.input(self.source_path, ss=seg['start'], to=seg['end']).output(clip_path, c='copy', ignore_chapters=1)
                args = ffmpeg.compile(stream, overwrite_output=True)
                result = subprocess.run(args, capture_output=True, creationflags=creation_flags, timeout=300) # Timeout de 5min por clipe

                if result.returncode != 0:
                    stderr_output = result.stderr.decode('utf-8', errors='ignore')
                    self.logger.log(f"FFmpeg falhou ao criar clipe #{i} (segmento {seg['start']:.2f}s-{seg['end']:.2f}s). Erro: {stderr_output}", "ERROR", self.task_id)
                    # Não lança exceção, permite que o processo continue com os clipes que funcionaram.
                    continue

                clip_files.append(clip_path)

            except subprocess.TimeoutExpired:
                 self.logger.log(f"Timeout ao criar clipe #{i}. O segmento pode ser muito longo ou o vídeo está corrompido.", "ERROR", self.task_id)
                 continue
            except ffmpeg.Error as e:
                self.logger.log(f"Erro FFmpeg ao criar clipe #{i}: {e.stderr.decode() if e.stderr else e}", "ERROR", self.task_id)
                continue

        return clip_files

    def _run_ffmpeg_concat(self, manifest, temp_dir, total_dur, stop_event):
        """Executa o comando final do FFmpeg para concatenar todos os clipes."""
        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0', '-i', manifest,
            '-y',
            '-c:v', 'libx264', '-preset', self.preset,
            '-c:a', 'aac', '-b:a', '192k',
            self.output_path
        ]
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
                    # O progresso da renderização começa em 25% do total, assumindo que a criação de clipes levou tempo.
                    progress = min(25 + (current / total_dur) * 74, 99.9)
                    self.pq.put({'type': 'progress', 'stage': 'Renderizando', 'percentage': progress, 'task_id': self.task_id})

        self.process.wait()

        if self.process.returncode != 0 and not stop_event.is_set():
            # O erro já foi logado linha a linha, aqui apenas lançamos a exceção final.
            raise RuntimeError(f"FFmpeg falhou na concatenação final com código de saída {self.process.returncode}.")

        self.logger.log(f"Renderização para '{self.output_path}' concluída com sucesso.", "SUCCESS", self.task_id)