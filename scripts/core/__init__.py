"""
DSAR Toolkit Core Modules

Shared functionality for all DSAR vendor processors.
"""

from .redaction import RedactionEngine
from .docgen import create_vendor_report, create_cover_letter, create_redaction_key
from .utils import (
    setup_argparser,
    parse_extra_redactions,
    load_json,
    load_csv,
    extract_zip,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    truncate,
    strip_html,
)
from .activity_log import (
    log_event,
    read_activity_log,
    get_activity_summary,
    get_activity_log_path,
    clear_activity_log,
)

__all__ = [
    'RedactionEngine',
    'create_vendor_report',
    'create_cover_letter',
    'create_redaction_key',
    'setup_argparser',
    'parse_extra_redactions',
    'load_json',
    'load_csv',
    'extract_zip',
    'save_json',
    'ensure_output_dir',
    'safe_filename',
    'format_date',
    'truncate',
    'strip_html',
    'log_event',
    'read_activity_log',
    'get_activity_summary',
    'get_activity_log_path',
    'clear_activity_log',
]
