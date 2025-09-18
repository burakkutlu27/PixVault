"""
Logging utilities for the harvest package.
"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


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
                log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up logger with JSON formatting for both console and file output.
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create JSON formatter
    json_formatter = JSONFormatter()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(json_formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_logger(name: str = "harvest") -> logging.Logger:
    """Get existing logger or create a new one with default settings."""
    logger = logging.getLogger(name)
    
    # If logger doesn't have handlers, set it up with defaults
    if not logger.handlers:
        return setup_logger(name)
    
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
