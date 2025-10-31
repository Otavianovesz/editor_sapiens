# core/renderer.py

import ffmpeg
import subprocess
import os
import concurrent.futures
from typing import List, Dict

class VideoRenderer:
    """Renders the final video from the processed segments."""
    def __init__(self, source_path: str, output_path: str, preset: str, logger, task_id: str):
        """Initializes the VideoRenderer.

        Args:
            source_path (str): The path to the source video file.
            output_path (str): The path to the output video file.
            preset (str): The rendering preset.
            logger: The logger instance.
            task_id (str): The ID of the task.
        """
        self.source_path = source_path
        self.output_path = output_path
        self.preset = preset
        self.logger = logger
        self.task_id = task_id
        self.hw_accel_enabled = True # Starts trying to use the GPU

    def _process_segment(self, segment_info: Dict) -> str:
        """
        Processes a single video segment to create a clip.
        This function is designed to be executed in parallel by multiple workers.

        Args:
            segment_info (Dict): A dictionary with the segment information.

        Returns:
            str: The path to the created clip.
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
            # In case of an error, returns the error to be logged in the main thread
            error_message = e.stderr.decode('utf-8', 'ignore') if isinstance(e.stderr, bytes) else e.stderr
            raise RuntimeError(f"Error in clip {i}: {error_message}")

    def _create_clips(self, segments: List[Dict], temp_dir: str):
        """
        Creates video clips in parallel using ThreadPoolExecutor for maximum CPU utilization.

        Args:
            segments (List[Dict]): A list of dictionaries with the segment information.
            temp_dir (str): The path to the temporary directory.

        Returns:
            list: A list with the paths to the created clips.
        """
        total_segments = len(segments)
        vcodec = "h264_nvenc" if self.hw_accel_enabled else "libx264"
        self.logger.info(f"[{self.task_id}] Starting creation of {total_segments} clips with codec: {vcodec} and preset: {self.preset}")

        tasks = [{'i': i, 'seg': seg, 'temp_dir': temp_dir, 'vcodec': vcodec} for i, seg in enumerate(segments)]
        clip_paths = [None] * total_segments
        
        # Uses up to the number of CPU cores, but at most 16 so as not to overload the system
        max_workers = min(os.cpu_count() or 1, 16)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(self._process_segment, task): task['i'] for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    clip_path = future.result()
                    clip_paths[index] = clip_path
                    self.logger.info(f"[{self.task_id}] Clip {index + 1}/{total_segments} created successfully.")
                except Exception as exc:
                    # If there is a failure with NVENC, it falls back to the CPU and restarts the process
                    if vcodec == "h264_nvenc":
                        self.logger.warning(f"[{self.task_id}] Failed to use {vcodec}. Canceling and restarting with libx264 (CPU)...")
                        executor.shutdown(wait=False, cancel_futures=True) # Cancels all other tasks
                        self.hw_accel_enabled = False # Disables the GPU for the new attempt
                        return self._create_clips(segments, temp_dir) # Calls itself again with the GPU disabled
                    else:
                        self.logger.error(f"[{self.task_id}] Fatal error when creating clip {index + 1}: {exc}")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise # Stops the entire process if the CPU also fails

        # Checks if all clips were created successfully
        if any(p is None for p in clip_paths):
            raise RuntimeError("Not all clips were created successfully.")
            
        return clip_paths

    def _run_ffmpeg_concat(self, manifest_path: str):
        """
        Concatenates the video clips to generate the final video.

        Args:
            manifest_path (str): The path to the manifest file.
        """
        self.logger.info(f"[{self.task_id}] Concatenating clips to generate the final video...")
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
            self.logger.error(f"[{self.task_id}] FFmpeg error during final concatenation: {error_message}")
            raise

    def render_video(self, segments: List[Dict], temp_dir: str):
        """
        Renders the final video.

        Args:
            segments (List[Dict]): A list of dictionaries with the segment information.
            temp_dir (str): The path to the temporary directory.
        """
        clip_paths = self._create_clips(segments, temp_dir)
        manifest_path = os.path.join(temp_dir, "manifest.txt")
        with open(manifest_path, 'w') as f:
            for i in range(len(clip_paths)):
                f.write(f"file 'clip_{i:04d}.mp4'\n")
        self._run_ffmpeg_concat(manifest_path)
        self.logger.info(f"[{self.task_id}] Video successfully rendered in '{self.output_path}'")
