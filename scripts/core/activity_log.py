"""
Activity logging for DSAR processing.

Provides centralized logging of all DSAR processing activities
for audit trail and GDPR accountability compliance.

Log Format: JSON Lines (one JSON object per line)
Location: ./output/dsar_activity.jsonl
"""

import logging
import json
import os
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path


# Module-level logger cache and lock for thread safety
_loggers: Dict[str, logging.Logger] = {}
_logger_lock = threading.Lock()


def _get_logger(output_dir: str = './output') -> logging.Logger:
    """
    Get or create the activity logger for a specific output directory.

    Uses Python's logging module for thread-safe file writes.

    Args:
        output_dir: Directory for the log file

    Returns:
        Configured logger instance
    """
    log_path = os.path.abspath(os.path.join(output_dir, 'dsar_activity.jsonl'))

    # Fast path: return existing logger if already created
    if log_path in _loggers:
        return _loggers[log_path]

    # Use lock to prevent race conditions when creating logger
    with _logger_lock:
        # Double-check after acquiring lock
        if log_path in _loggers:
            return _loggers[log_path]

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Create new logger with unique name based on path hash
        logger_name = f'dsar_activity_{hash(log_path)}'
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        # Create file handler (append mode for audit trail)
        handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)

        # Prevent propagation to root logger
        logger.propagate = False

        _loggers[log_path] = logger
        return logger


def log_event(event_type: str, output_dir: str = './output', **kwargs) -> None:
    """
    Log a DSAR activity event.

    Args:
        event_type: Type of event (e.g., 'processing_started', 'processing_complete')
        output_dir: Output directory for the log file
        **kwargs: Additional event data

    Event Types:
        - processing_started: Vendor script begins
        - data_subject_found: Data subject identified in export
        - processing_complete: Vendor script finishes successfully
        - processing_failed: Vendor script encounters error
        - package_compilation_started: compile_package begins
        - package_compilation_complete: Package created
    """
    try:
        entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_type': event_type,
            **kwargs
        }

        logger = _get_logger(output_dir)
        logger.info(json.dumps(entry, default=str))
    except Exception:
        # Never let logging failures crash the main process
        # Silently continue - the DSAR processing is more important
        pass


def get_activity_log_path(output_dir: str = './output') -> str:
    """
    Get the path to the activity log file.

    Args:
        output_dir: Output directory

    Returns:
        Full path to the activity log file
    """
    return os.path.join(output_dir, 'dsar_activity.jsonl')


def read_activity_log(output_dir: str = './output') -> List[Dict]:
    """
    Read all entries from the activity log.

    Args:
        output_dir: Output directory containing the log

    Returns:
        List of log entry dictionaries
    """
    log_path = get_activity_log_path(output_dir)
    entries = []

    if not os.path.exists(log_path):
        return entries

    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except IOError:
        pass

    return entries


def get_activity_summary(
    data_subject_name: str,
    output_dir: str = './output'
) -> Dict[str, Any]:
    """
    Get summary of processing activity for a data subject.

    This summary is designed to be included in the DSAR package manifest.

    Args:
        data_subject_name: Name of the data subject
        output_dir: Output directory containing the log

    Returns:
        Summary dictionary with processing statistics
    """
    entries = read_activity_log(output_dir)

    # Filter to this data subject
    subject_entries = [
        e for e in entries
        if e.get('data_subject_name') == data_subject_name
    ]

    # Collect completion events
    completions = [
        e for e in subject_entries
        if e.get('event_type') == 'processing_complete'
    ]

    failures = [
        e for e in subject_entries
        if e.get('event_type') == 'processing_failed'
    ]

    # Calculate totals
    vendors_processed = []
    total_records = 0
    total_execution_time = 0.0

    for entry in completions:
        vendor = entry.get('vendor')
        if vendor and vendor not in vendors_processed:
            vendors_processed.append(vendor)

        records = entry.get('records_processed', 0)
        if isinstance(records, int):
            total_records += records

        exec_time = entry.get('execution_time_seconds', 0)
        if isinstance(exec_time, (int, float)):
            total_execution_time += exec_time

    # Get processing date from first entry
    processing_date = None
    if subject_entries:
        first_ts = subject_entries[0].get('timestamp', '')
        if first_ts:
            processing_date = first_ts[:10]  # YYYY-MM-DD

    return {
        'processing_date': processing_date,
        'vendors_processed': sorted(vendors_processed),
        'vendors_failed': [e.get('vendor') for e in failures if e.get('vendor')],
        'total_records': total_records,
        'total_execution_time_seconds': round(total_execution_time, 2),
        'all_successful': len(failures) == 0,
    }


def clear_activity_log(output_dir: str = './output') -> None:
    """
    Clear the activity log file.

    Use with caution - this removes the audit trail.

    Args:
        output_dir: Output directory containing the log
    """
    log_path = os.path.abspath(os.path.join(output_dir, 'dsar_activity.jsonl'))

    with _logger_lock:
        # Close existing handlers and remove logger from cache
        if log_path in _loggers:
            logger = _loggers[log_path]
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
            del _loggers[log_path]

    # Remove the file
    if os.path.exists(log_path):
        os.remove(log_path)
