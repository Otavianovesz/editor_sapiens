# core/orchestrator.py

import os
import traceback
import logging
import json
import tempfile
from typing import Dict
from .subtitles import TimelineRemapper, SubtitleGenerator
from .renderer import VideoRenderer
from core.exceptions import InterruptedError

class Orchestrator:
    """Orchestrates the entire video processing pipeline."""
    def __init__(self, logger, config, modules):
        """Initializes the Orchestrator.

        Args:
            logger: The logger instance.
            config: The configuration instance.
            modules: A dictionary of processing modules.
        """
        self._modules = modules
        self.config = config
        self.logger = logger
        self.current_renderer = None

    def _get_module(self, name: str):
        """Gets a processing module by name.

        Args:
            name (str): The name of the module.

        Returns:
            The processing module.
        """
        return self._modules.get(name)

    def interrupt_current_task(self):
        """Interrupts the current task."""
        # This function can be used for more complex interruptions in the future
        pass

    def run_sapiens_task(self, pq, task_config: Dict, stop_event):
        """Runs the main video processing task.

        Args:
            pq: The progress queue.
            task_config (Dict): The configuration for the task.
            stop_event: The event to stop the task.
        """
        task_id = task_config['id']
        video_path = task_config['video_path']
        
        try:
            self.logger.info(f"[{task_id}] Starting Sapiens process...")

            # STEP 1: Transcription
            if task_config.get('transcription_mode') == 'file' and os.path.exists(task_config.get('transcription_path', '')):
                self.logger.info(f"[{task_id}] Loading transcription from file: {os.path.basename(task_config['transcription_path'])}")
                words = self._get_module('parser').parse(task_config['transcription_path'], task_id)
            else:
                self.logger.info(f"[{task_id}] Starting audio extraction and transcription...")
                media_processor = self._get_module('media_processor')
                transcriber = self._get_module('transcriber')
                audio_path = media_processor.extract_audio(task_config['video_path'], task_id)
                words = transcriber.transcribe(audio_path, pq, task_id, stop_event)
                media_processor.cleanup(audio_path, task_id)
            
            if stop_event.is_set() or not words: raise InterruptedError("Transcription failed.")
            self.logger.info(f"[{task_id}] Transcription completed.")
            
            # STEP 2: Content Analysis
            content_analyzer = self._get_module('content')
            segments = content_analyzer.create_speech_segments(words, pq, task_config, task_id, stop_event)
            if stop_event.is_set() or not segments: raise InterruptedError("Content analysis failed.")
            self.logger.info(f"[{task_id}] Content analysis and cut definition completed.")
            
            # STEP 3: Timeline and Script Mapping
            remapper = TimelineRemapper(segments)
            composer = self._get_module('composer')
            script_path = composer.generate_and_save_json(segments, video_path, task_id)
            if not script_path: raise RuntimeError("Failed to save the editing script.")
            self.logger.info(f"[{task_id}] Editing script saved in '{os.path.basename(script_path)}'.")

            # STEP 4: Generation of Synchronized Subtitles
            self.logger.info(f"[{task_id}] Generating synchronized subtitles for the edited video...")
            subtitle_generator = SubtitleGenerator(remapper, self.logger, task_id)
            srt_path = subtitle_generator.generate_srt(words, video_path)
            
            if srt_path:
                self.logger.info(f"[{task_id}] Synchronized subtitles saved in '{os.path.basename(srt_path)}'")
            else:
                self.logger.warning(f"[{task_id}] Could not generate the subtitle file.")

            # STEP 5: Notification to the UI
            pq.put({'type': 'sapiens_done', 'script_path': script_path, 'task_id': task_id})

        except InterruptedError as e:
            self.logger.warning(f"[{task_id}] Process interrupted: {e}")
            pq.put({'type': 'interrupted', 'message': str(e), 'task_id': task_id})
        except Exception as e:
            self.logger.critical(f"[{task_id}] Unexpected error in the orchestrator: {e}\n{traceback.format_exc()}")
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})

    # --- ADDED METHOD ---
    def run_render_task(self, pq, task_config: Dict, stop_event):
        """
        Executes only the video rendering part, using a JSON script.

        Args:
            pq: The progress queue.
            task_config (Dict): The configuration for the task.
            stop_event: The event to stop the task.
        """
        task_id = task_config['id']
        script_path = task_config.get('render_script_path')
        video_path = task_config.get('video_path')

        try:
            self.logger.info(f"[{task_id}] Starting rendering task...")
            if not script_path or not os.path.exists(script_path):
                raise FileNotFoundError(f"Script file '{script_path}' not found.")
            
            with open(script_path, 'r', encoding='utf-8') as f:
                script_data = json.load(f)
            segments = script_data.get("segments")
            if not segments:
                raise ValueError("Script does not contain the 'segments' key or it is empty.")

            output_path = os.path.splitext(video_path)[0] + "_edited.mp4"
            render_preset = self.config.get("render_preset", "medium")

            # Creates a temporary directory for the clips
            with tempfile.TemporaryDirectory(prefix="sapiens_") as temp_dir:
                self.logger.info(f"[{task_id}] Using temporary directory: {temp_dir}")

                renderer = VideoRenderer(
                    source_path=video_path,
                    output_path=output_path,
                    preset=render_preset,
                    logger=self.logger,
                    task_id=task_id
                )
                
                # Rendering is a long-running process that needs to be monitored
                renderer.render_video(segments, temp_dir)

            if stop_event.is_set():
                raise InterruptedError("Rendering interrupted by the user.")

            self.logger.info(f"[{task_id}] Rendering completed successfully.")
            pq.put({'type': 'done', 'task_id': task_id})

        except InterruptedError as e:
            self.logger.warning(f"[{task_id}] Rendering process interrupted: {e}")
            pq.put({'type': 'interrupted', 'message': str(e), 'task_id': task_id})
        except Exception as e:
            self.logger.critical(f"[{task_id}] Unexpected error in rendering: {e}\n{traceback.format_exc()}")
            pq.put({'type': 'error', 'message': str(e), 'task_id': task_id})
