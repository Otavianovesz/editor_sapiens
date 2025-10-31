# -*- coding: utf-8 -*-

import sys
import logging
import faulthandler
import threading
import queue
import gc
import os
from typing import NoReturn, Optional
from tkinter import messagebox
from types import FrameType
from typing import Union, Type, Optional

class LoggerAdapter(logging.LoggerAdapter):
    """Adapter to ensure that task_id is always present in the log."""
    def process(self, msg, kwargs):
        """Processes the log message and keyword arguments.

        Args:
            msg (str): The log message.
            kwargs (dict): The keyword arguments.

        Returns:
            tuple: A tuple containing the processed log message and keyword arguments.
        """
        kwargs.setdefault('extra', {}).setdefault('task_id', 'Global')
        return msg, kwargs

def setup_logging() -> None:
    """Configures the global logger with file rotation and appropriate levels."""
    try:
        log_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] - %(threadName)s: [%(task_id)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Log file configuration with rotation
        from logging.handlers import RotatingFileHandler
        log_file_handler = RotatingFileHandler(
            'sapiens.log',
            mode='a',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        log_file_handler.setFormatter(log_formatter)
        
        # Root logger configuration
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Clears existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            
        root_logger.addHandler(log_file_handler)
        
        # Configures the global adapter
        global logger
        logger = LoggerAdapter(root_logger, {'task_id': 'Global'})
        
        logger.info("================ STARTING APPLICATION ================")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to configure logging: {e}", file=sys.stderr)
        raise

def cleanup_threads() -> None:
    """Performs a systematic cleanup of system resources."""
    def safe_thread_stop(thread: threading.Thread) -> None:
        try:
            if hasattr(thread, '_stop'):
                thread._stop()
        except Exception as e:
            logger.warning(f"Failed to stop thread {thread.name}: {e}")

    try:
        # 1. Forced garbage collection
        gc.collect()
        
        # 2. Progressbar cleanup
        try:
            from tqdm import tqdm
            tqdm._instances.clear()
        except Exception as e:
            logger.debug(f"Ignoring error in tqdm cleanup: {e}")

        # 3. Controlled thread interruption
        current = threading.current_thread()
        for thread in threading.enumerate():
            if thread is not current and not thread.daemon:
                safe_thread_stop(thread)
                
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)

def setup_thread_excepthook() -> None:
    """Sets up a global handler for unhandled exceptions in threads."""
    
    def custom_thread_excepthook(args: threading.ExceptHookArgs) -> None:
        error_msg = (
            f"Unhandled error in thread {args.thread.name}: "
            f"{args.exc_type.__name__}: {args.exc_value}"
        )
        
        logger.critical(error_msg, 
                        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
                        extra={'task_id': 'Global'})
        
        if isinstance(args.exc_value, (SystemExit, KeyboardInterrupt)):
            logger.info("Closing application due to interruption signal",
                        extra={'task_id': 'Global'})
            cleanup_threads()
            os._exit(1)

    threading.excepthook = custom_thread_excepthook

def signal_handler(signum: int, frame: Optional[FrameType]) -> NoReturn:
    """Unified handler for system signals.

    Args:
        signum (int): The signal number.
        frame (Optional[FrameType]): The current stack frame.
    """
    logger.info(f"Signal received: {signum}", extra={'task_id': 'Global'})
    cleanup_threads()
    sys.exit(0)

def main() -> int:
    """Main entry point with robust error management.

    Returns:
        int: The exit code.
    """
    try:
        # Initial settings
        faulthandler.enable()
        setup_logging()
        setup_thread_excepthook()
        
        # Signal configuration
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            from ui.app import App
        except ImportError as e:
            error_message = (
                f"Import error: {e}\n\n"
                "Essential dependencies not found.\n"
                "Run: pip install -r requirements.txt"
            )
            logger.critical(f"Import failed: {e}", exc_info=True)
            
            try:
                import tkinter as tk
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror("Critical Error", error_message)
            except Exception:
                print(error_message, file=sys.stderr)
            return 1

        try:
            app = App()
            app.mainloop()
        except Exception as e:
            error_msg = f"Fatal error in the application: {type(e).__name__}: {e}"
            logger.critical(error_msg, exc_info=True)
            
            try:
                messagebox.showerror("Fatal Error", error_msg)
            except Exception:
                print(error_msg, file=sys.stderr)
            return 1
            
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            cleanup_threads()
            logger.info("================ CLOSING APPLICATION ================")
        except Exception as e:
            print(f"Error on finalization: {e}", file=sys.stderr)
            
    return 0

if __name__ == "__main__":
    sys.exit(main())

class MediaProcessor:
    """A class to process media files."""
    def __init__(self, app_context):
        """Initializes the MediaProcessor.

        Args:
            app_context: The application context.
        """
        self.app_context = app_context
        self.temp_files = set()
    
    def register_temp_file(self, path: str) -> None:
        """Registers a temporary file.

        Args:
            path (str): The path to the temporary file.
        """
        self.temp_files.add(path)
        self.app_context.register_temp_file(path)
    
    def cleanup(self) -> None:
        """Cleans up the processor's resources."""
        for path in self.temp_files:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                logger.warning(f"Error when removing temporary file: {path}: {e}")
        self.temp_files.clear()
