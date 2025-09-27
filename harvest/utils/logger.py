"""
Enhanced logging utilities for the harvest package.
"""

import logging
import json
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


class ColoredFormatter(logging.Formatter):
    """Colored console formatter for logs."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        # Get color for the log level
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        # Create colored output
        level_colored = f"{color}{record.levelname:<8}{reset}"
        logger_name = f"[{record.name}]" if record.name != 'root' else ""
        
        # Base message
        message = f"{timestamp} {level_colored} {logger_name} {record.getMessage()}"
        
        # Add extra fields if they exist
        if hasattr(record, 'extra_fields') and record.extra_fields:
            extra_str = " | ".join([f"{k}={v}" for k, v in record.extra_fields.items()])
            message += f" | {extra_str}"
        
        # Add exception info if present
        if record.exc_info:
            message += f"\n{self.formatException(record.exc_info)}"
        
        return message


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for logs."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "time": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        
        # Add extra fields if they exist
        if hasattr(record, 'extra_fields') and record.extra_fields:
            log_entry.update(record.extra_fields)
        
        # Add standard fields if they exist and are not default
        if record.name != 'root':
            log_entry['logger'] = record.name
        
        if record.filename:
            log_entry['filename'] = record.filename
            log_entry['lineno'] = record.lineno
        
        if record.funcName:
            log_entry['function'] = record.funcName
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logger(name: str = "harvest", 
                level: str = "INFO",
                enable_console: bool = True,
                enable_file_logging: bool = True,
                log_dir: str = "logs") -> logging.Logger:
    """
    Set up enhanced logger with colored console output and separate log files.
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_console: Whether to enable colored console output
        enable_file_logging: Whether to enable file logging
        log_dir: Directory for log files
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler with colored output
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColoredFormatter())
        logger.addHandler(console_handler)
    
    # File handlers for different log levels
    if enable_file_logging:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # Info log file (INFO and above)
        info_handler = logging.FileHandler(
            log_path / "info.log", 
            encoding='utf-8'
        )
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(JSONFormatter())
        logger.addHandler(info_handler)
        
        # Error log file (ERROR and above)
        error_handler = logging.FileHandler(
            log_path / "error.log", 
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(JSONFormatter())
        logger.addHandler(error_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_logger(name: str = "harvest") -> logging.Logger:
    """Get existing logger or create a new one with enhanced settings."""
    logger = logging.getLogger(name)
    
    # If logger doesn't have handlers, set it up with enhanced defaults
    if not logger.handlers:
        return setup_logger(
            name=name,
            level="INFO",
            enable_console=True,
            enable_file_logging=True,
            log_dir="logs"
        )
    
    return logger


def log_with_extra(logger: logging.Logger, level: str, message: str, **extra_fields) -> None:
    """
    Log a message with extra fields.
    
    Args:
        logger: Logger instance
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        message: Log message
        **extra_fields: Additional fields to include in the log
    """
    # Create a log record with extra fields
    record = logger.makeRecord(
        logger.name, 
        getattr(logging, level.upper()), 
        "", 0, message, (), None
    )
    
    # Add extra fields to the record
    record.extra_fields = extra_fields
    
    # Handle the record
    logger.handle(record)


# Convenience functions for common logging scenarios
def log_download(logger: logging.Logger, url: str, filename: str, status: str, **extra):
    """Log download activity."""
    log_with_extra(
        logger, "INFO", f"Download {status}: {filename}",
        url=url, filename=filename, status=status, **extra
    )


def log_database(logger: logging.Logger, operation: str, table: str, **extra):
    """Log database operations."""
    log_with_extra(
        logger, "DEBUG", f"Database {operation} on {table}",
        operation=operation, table=table, **extra
    )


def log_error(logger: logging.Logger, message: str, error: Exception, **extra):
    """Log errors with exception details."""
    log_with_extra(
        logger, "ERROR", message,
        error_type=type(error).__name__, error_message=str(error), **extra
    )
