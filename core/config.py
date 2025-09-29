# -*- coding: utf-8 -*-
"""Configuration management module for Sapiens Editor."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from threading import Lock


class ConfigurationError(Exception):
    """Raised when configuration operations fail."""
    pass


class Config:
    """
    Thread-safe configuration manager with validation and default fallbacks.
    
    Manages application settings through JSON persistence with atomic operations.
    """
    
    # Default configuration schema with types
    DEFAULT_SETTINGS: Dict[str, Any] = {
        # Whisper transcription settings
        "whisper_model_size": "large-v3",
        "whisper_language": "pt",
        "whisper_device": "cuda",
        "whisper_compute_type": "float16",
        
        # Visual analysis settings
        "visual_analysis_fps": 5,
        
        # Speech analysis settings
        "pause_threshold_s": 0.5,
        "min_segment_duration_s": 0.2,
        "cut_threshold": -7.0,
        "filler_word_context_pause": 0.25,
        
        # Scoring system
        "scores": {
            "pause_long": -10,
            "pause_medium": -7,
            "looking_away": -5,
            "gesturing": 8
        },
        
        # Filler words dictionary
        "filler_words": [
            "uhm", "hum", "ahn", "é", "hã", "bem", "tipo", "aí", "daí",
            "então", "assim", "meio que", "né", "tá", "viu", "sabe",
            "entende", "certo", "ok", "beleza", "fechou", "na verdade",
            "quer dizer", "ou seja", "basicamente", "literalmente",
            "simplesmente", "realmente", "praticamente", "cara", "meu",
            "véi", "mano", "bicho"
        ],
        
        # Sensitivity settings
        "gesture_sensitivity_velocity": 0.1,
        "gaze_sensitivity_yaw": 0.8,
        "gaze_sensitivity_pitch": 0.7,
        
        # Rendering settings
        "render_preset": "medium"
    }
    
    def __init__(self, config_path: str = 'config_sapiens.json'):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration JSON file
        """
        self.config_path = Path(config_path)
        self.settings: Dict[str, Any] = self.DEFAULT_SETTINGS.copy()
        self._lock = Lock()
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file with proper error handling."""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    
                # Merge loaded config with defaults (preserves new default keys)
                self._merge_configs(loaded_config)
                logging.info(f"Configuration loaded from {self.config_path}")
            else:
                logging.info("Configuration file not found, using defaults")
                self.save()
                
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}")
            self._create_backup_and_reset()
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            self._create_backup_and_reset()
    
    def _merge_configs(self, loaded: Dict[str, Any]) -> None:
        """
        Merge loaded configuration with defaults, preserving type safety.
        
        Args:
            loaded: Configuration dictionary from file
        """
        with self._lock:
            for key, default_value in self.DEFAULT_SETTINGS.items():
                if key in loaded:
                    loaded_value = loaded[key]
                    
                    # Type validation
                    if isinstance(default_value, dict) and isinstance(loaded_value, dict):
                        # Deep merge for nested dicts
                        self.settings[key] = {**default_value, **loaded_value}
                    elif type(loaded_value) == type(default_value):
                        self.settings[key] = loaded_value
                    else:
                        logging.warning(
                            f"Type mismatch for '{key}': expected {type(default_value).__name__}, "
                            f"got {type(loaded_value).__name__}. Using default."
                        )
                        self.settings[key] = default_value
                else:
                    self.settings[key] = default_value
    
    def _create_backup_and_reset(self) -> None:
        """Create backup of corrupted config and reset to defaults."""
        try:
            if self.config_path.exists():
                backup_path = self.config_path.with_suffix('.json.backup')
                self.config_path.rename(backup_path)
                logging.info(f"Corrupted config backed up to {backup_path}")
        except Exception as e:
            logging.error(f"Failed to create backup: {e}")
        
        self.settings = self.DEFAULT_SETTINGS.copy()
        self.save()
    
    def save(self) -> bool:
        """
        Atomically save current configuration to file.
        
        Returns:
            True if save successful, False otherwise
        """
        temp_path = self.config_path.with_suffix('.tmp')
        
        try:
            with self._lock:
                # Write to temporary file first
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.settings, f, indent=4, ensure_ascii=False)
                
                # Atomic replace
                temp_path.replace(self.config_path)
                
            logging.info(f"Configuration saved to {self.config_path}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to save configuration: {e}", exc_info=True)
            if temp_path.exists():
                temp_path.unlink()
            return False
    
    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Thread-safe get operation with fallback.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        with self._lock:
            return self.settings.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Thread-safe set operation with validation.
        
        Args:
            key: Configuration key
            value: New value
            
        Raises:
            ConfigurationError: If key not in defaults or type mismatch
        """
        with self._lock:
            if key not in self.DEFAULT_SETTINGS:
                raise ConfigurationError(f"Unknown configuration key: {key}")
            
            default_value = self.DEFAULT_SETTINGS[key]
            if not isinstance(value, type(default_value)):
                raise ConfigurationError(
                    f"Type mismatch for '{key}': expected {type(default_value).__name__}, "
                    f"got {type(value).__name__}"
                )
            
            self.settings[key] = value
    
    def reset_to_defaults(self) -> None:
        """Reset all settings to default values."""
        with self._lock:
            self.settings = self.DEFAULT_SETTINGS.copy()
        logging.info("Configuration reset to defaults")
    
    def validate(self) -> bool:
        """
        Validate current configuration.
        
        Returns:
            True if configuration is valid
        """
        try:
            with self._lock:
                for key, value in self.settings.items():
                    if key not in self.DEFAULT_SETTINGS:
                        logging.warning(f"Unknown configuration key: {key}")
                        continue
                    
                    default_type = type(self.DEFAULT_SETTINGS[key])
                    if not isinstance(value, default_type):
                        logging.error(
                            f"Invalid type for '{key}': expected {default_type.__name__}, "
                            f"got {type(value).__name__}"
                        )
                        return False
            
            return True
            
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            return False