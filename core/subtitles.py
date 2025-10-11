# core/subtitles.py

import os
from bisect import bisect_right
from datetime import timedelta
from typing import List, Dict, Optional, Tuple, Any

class TimelineRemapper:
    # (O código desta classe permanece inalterado, pois não faz logging)
    def __init__(self, segments_to_keep: List[Dict[str, float]]):
        self._segments = sorted(segments_to_keep, key=lambda s: s['start'])
        self._original_starts: List[float] = []
        self._offsets: List[float] = []
        last_original_end = 0.0
        cumulative_offset = 0.0
        for segment in self._segments:
            original_start = segment['start']
            time_removed_before_this = original_start - last_original_end
            cumulative_offset += time_removed_before_this
            self._original_starts.append(original_start)
            self._offsets.append(cumulative_offset)
            last_original_end = segment['end']

    def remap_timestamp(self, original_timestamp: float) -> Optional[float]:
        if not self._original_starts: return None
        segment_index = bisect_right(self._original_starts, original_timestamp) - 1
        if segment_index < 0: return None
        segment = self._segments[segment_index]
        if original_timestamp > segment['end']: return None
        offset = self._offsets[segment_index]
        return original_timestamp - offset

    def remap_event(self, start_time: float, end_time: float) -> Optional[Tuple[float, float]]:
        new_start = self.remap_timestamp(start_time)
        new_end = self.remap_timestamp(end_time)
        if new_start is not None and new_end is not None and new_end > new_start:
            return new_start, new_end
        return None

class SubtitleGenerator:
    def __init__(self, remapper: TimelineRemapper, logger, task_id: str):
        self.remapper = remapper
        self.logger = logger
        self.task_id = task_id
        self.MAX_CHARS_PER_LINE = 42
        self.MAX_PAUSE_BETWEEN_WORDS = 0.7

    def _format_time(self, seconds: float) -> str:
        if seconds < 0: seconds = 0.0
        delta = timedelta(seconds=seconds)
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = delta.microseconds // 1000
        return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03d}"

    def generate_srt(self, words: List[Any], video_path: str) -> Optional[str]:
        remapped_words = []
        for word in words:
            remap_result = self.remapper.remap_event(word.start, word.end)
            if remap_result:
                new_start, new_end = remap_result
                remapped_words.append({'text': word.word.strip(), 'start': new_start, 'end': new_end})

        if not remapped_words:
            # --- CHAMADA DE LOG CORRIGIDA ---
            self.logger.warning(f"[{self.task_id}] Nenhuma palavra da transcrição foi mantida. Legendas não geradas.")
            return None

        srt_entries = []
        current_line_words = []
        line_start_time = 0
        for i, word in enumerate(remapped_words):
            if not current_line_words:
                current_line_words.append(word['text'])
                line_start_time = word['start']
            else:
                line_text = " ".join(current_line_words)
                pause = word['start'] - remapped_words[i - 1]['end']
                if (len(line_text) + len(word['text']) + 1 > self.MAX_CHARS_PER_LINE) or (pause > self.MAX_PAUSE_BETWEEN_WORDS):
                    line_end_time = remapped_words[i - 1]['end']
                    srt_entries.append({'start': line_start_time, 'end': line_end_time, 'text': line_text})
                    current_line_words = [word['text']]
                    line_start_time = word['start']
                else:
                    current_line_words.append(word['text'])
        
        if current_line_words:
            srt_entries.append({
                'start': line_start_time, 'end': remapped_words[-1]['end'],
                'text': " ".join(current_line_words)
            })

        srt_content = []
        for i, entry in enumerate(srt_entries):
            start_str = self._format_time(entry['start'])
            end_str = self._format_time(entry['end'])
            srt_content.append(f"{i + 1}\n{start_str} --> {end_str}\n{entry['text']}\n")

        output_path = os.path.splitext(video_path)[0] + "_editado.srt"
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(srt_content))
            return output_path
        except IOError as e:
            # --- CHAMADA DE LOG CORRIGIDA ---
            self.logger.error(f"[{self.task_id}] Falha ao salvar o arquivo de legenda SRT: {e}")
            return None