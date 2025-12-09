"""
Common utilities for DSAR processors.

This module provides shared functionality for:
- Command-line argument parsing
- File I/O operations (JSON, CSV, ZIP)
- Date parsing and formatting
- Text processing utilities
"""

import os
import sys
import json
import zipfile
import csv
import re
import html
import argparse
import chardet
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime


def setup_argparser(vendor_name: str) -> argparse.ArgumentParser:
    """
    Create a standard argument parser for DSAR scripts.

    Args:
        vendor_name: Name of the vendor (used in help text)

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description=f'Process {vendor_name} export for DSAR response',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python {vendor_name.lower()}_dsar.py export.json "John Smith" -e john@company.com
  python {vendor_name.lower()}_dsar.py export.zip "Jane Doe" -e jane@company.com -o ./output
  python {vendor_name.lower()}_dsar.py export.csv "Bob Wilson" --redact "Alice Brown, Charlie Davis"
        """
    )
    parser.add_argument(
        'export_path',
        help='Path to export file (JSON, CSV, or ZIP)'
    )
    parser.add_argument(
        'data_subject_name',
        help='Full name of the data subject'
    )
    parser.add_argument(
        '--email', '-e',
        help='Email address of the data subject (required if multiple name matches)'
    )
    parser.add_argument(
        '--redact', '-r',
        help='Additional names to redact (comma-separated)'
    )
    parser.add_argument(
        '--output', '-o',
        default='./output',
        help='Output directory (default: ./output)'
    )
    return parser


def parse_extra_redactions(redact_arg: str) -> List[str]:
    """
    Parse comma-separated redaction list from CLI argument.

    Args:
        redact_arg: Comma-separated string of names to redact

    Returns:
        List of names to add to redaction
    """
    if not redact_arg:
        return []
    return [name.strip() for name in redact_arg.split(',') if name.strip()]


def load_json(path: str) -> Any:
    """
    Load a JSON file with automatic encoding detection.

    Args:
        path: Path to the JSON file

    Returns:
        Parsed JSON content

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If JSON is invalid
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Export file not found: {path}")

    # Try UTF-8 first (most common)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except UnicodeDecodeError:
        pass

    # Detect encoding
    with open(path, 'rb') as f:
        raw = f.read()
        detected = chardet.detect(raw)
        encoding = detected.get('encoding', 'utf-8')

    with open(path, 'r', encoding=encoding) as f:
        return json.load(f)


def load_csv(path: str) -> List[Dict]:
    """
    Load a CSV file as a list of dictionaries.

    Args:
        path: Path to the CSV file

    Returns:
        List of dictionaries (one per row)

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Export file not found: {path}")

    # Try common encodings
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']

    for encoding in encodings:
        try:
            with open(path, 'r', encoding=encoding, newline='') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except UnicodeDecodeError:
            continue

    # Fall back to chardet
    with open(path, 'rb') as f:
        raw = f.read()
        detected = chardet.detect(raw)
        encoding = detected.get('encoding', 'utf-8')

    with open(path, 'r', encoding=encoding, newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)


def extract_zip(zip_path: str, extract_to: str = None) -> str:
    """
    Extract a ZIP file and return the extraction directory.

    Args:
        zip_path: Path to the ZIP file
        extract_to: Destination directory (default: same name as ZIP without extension)

    Returns:
        Path to the extraction directory

    Raises:
        FileNotFoundError: If ZIP file doesn't exist
        zipfile.BadZipFile: If file is not a valid ZIP
        ValueError: If ZIP contains unsafe paths
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    if extract_to is None:
        extract_to = os.path.splitext(zip_path)[0]

    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Security check: prevent path traversal attacks
        for name in zf.namelist():
            if name.startswith('/') or '..' in name:
                raise ValueError(f"Unsafe path in ZIP: {name}")

        zf.extractall(extract_to)

    return extract_to


def read_from_zip(zip_path: str, filename: str) -> bytes:
    """
    Read a file directly from a ZIP without full extraction.

    Args:
        zip_path: Path to the ZIP file
        filename: Name of the file within the ZIP

    Returns:
        Raw bytes of the file content
    """
    with zipfile.ZipFile(zip_path, 'r') as zf:
        return zf.read(filename)


def load_json_from_zip(zip_path: str, json_filename: str) -> Any:
    """
    Load a JSON file directly from a ZIP.

    Args:
        zip_path: Path to the ZIP file
        json_filename: Name of the JSON file within the ZIP

    Returns:
        Parsed JSON content
    """
    content = read_from_zip(zip_path, json_filename)
    return json.loads(content.decode('utf-8'))


def save_json(data: Any, path: str) -> None:
    """
    Save data as a formatted JSON file.

    Args:
        data: Data to serialize
        path: Output file path
    """
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)


def ensure_output_dir(output_dir: str) -> str:
    """
    Ensure an output directory exists, creating it if necessary.

    Args:
        output_dir: Path to the output directory

    Returns:
        The same path (for chaining)
    """
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def safe_filename(name: str) -> str:
    """
    Convert a name to a safe filename.

    Removes special characters and replaces spaces with underscores.

    Args:
        name: The name to convert

    Returns:
        A filesystem-safe filename
    """
    # Keep alphanumeric, spaces, hyphens, underscores
    safe = "".join(
        c if c.isalnum() or c in (' ', '-', '_') else '_'
        for c in name
    )
    # Replace spaces with underscores and collapse multiple underscores
    safe = safe.strip().replace(' ', '_')
    safe = re.sub(r'_+', '_', safe)
    return safe


def format_date(date_str: str) -> str:
    """
    Parse and format a date string from various formats.

    Args:
        date_str: Date string in various possible formats

    Returns:
        Formatted date string (YYYY-MM-DD HH:MM) or 'N/A' if unparseable
    """
    if not date_str:
        return 'N/A'

    # Common date formats to try
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',      # ISO 8601 with microseconds and Z
        '%Y-%m-%dT%H:%M:%S.%f',        # ISO 8601 with microseconds
        '%Y-%m-%dT%H:%M:%SZ',          # ISO 8601 with Z
        '%Y-%m-%dT%H:%M:%S',           # ISO 8601
        '%Y-%m-%d %H:%M:%S.%f',        # SQL with microseconds
        '%Y-%m-%d %H:%M:%S',           # SQL format
        '%Y-%m-%d',                     # Date only
        '%d/%m/%Y %H:%M:%S',           # European with time
        '%d/%m/%Y',                     # European date
        '%m/%d/%Y %H:%M:%S',           # American with time
        '%m/%d/%Y',                     # American date
        '%d %b %Y',                     # 15 Jan 2025
        '%d %B %Y',                     # 15 January 2025
        '%b %d, %Y',                    # Jan 15, 2025
        '%B %d, %Y',                    # January 15, 2025
    ]

    date_str = str(date_str).strip()

    # Handle timezone suffix variations
    if date_str.endswith('+00:00'):
        date_str = date_str[:-6]
    elif date_str.endswith('Z'):
        date_str = date_str[:-1]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str[:26], fmt)
            return dt.strftime('%Y-%m-%d %H:%M')
        except ValueError:
            continue

    # Try python-dateutil if available
    try:
        from dateutil import parser
        dt = parser.parse(date_str)
        return dt.strftime('%Y-%m-%d %H:%M')
    except (ImportError, ValueError):
        pass

    # Return truncated original if all else fails
    return str(date_str)[:20]


def truncate(text: str, max_length: int = 500) -> str:
    """
    Truncate text to a maximum length with ellipsis.

    Args:
        text: Text to truncate
        max_length: Maximum length (default 500)

    Returns:
        Truncated text with ellipsis if necessary
    """
    if not text:
        return ''
    text = str(text)
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + '...'


def strip_html(html_content: str) -> str:
    """
    Remove HTML tags and decode entities from text.

    Args:
        html_content: HTML string to clean

    Returns:
        Plain text with HTML removed
    """
    if not html_content:
        return ''

    # Decode HTML entities
    text = html.unescape(str(html_content))

    # Remove HTML tags
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    return text


def print_progress(current: int, total: int, prefix: str = 'Processing') -> None:
    """
    Print a simple progress indicator.

    Args:
        current: Current item number
        total: Total number of items
        prefix: Text prefix for the progress message
    """
    if total == 0:
        return

    if current % 1000 == 0 or current == total:
        percent = (current / total) * 100
        print(f"\r{prefix}: {current:,}/{total:,} ({percent:.1f}%)", end='', flush=True)

    if current == total:
        print()  # Final newline


def find_files_in_dir(
    directory: str,
    extensions: List[str] = None,
    recursive: bool = True
) -> List[str]:
    """
    Find files in a directory, optionally filtered by extension.

    Args:
        directory: Directory to search
        extensions: List of extensions to match (e.g., ['.json', '.csv'])
        recursive: Whether to search subdirectories

    Returns:
        List of file paths
    """
    if not os.path.isdir(directory):
        return []

    files = []
    pattern = '**/*' if recursive else '*'

    for path in Path(directory).glob(pattern):
        if path.is_file():
            if extensions is None or path.suffix.lower() in extensions:
                files.append(str(path))

    return sorted(files)


def get_timestamp() -> str:
    """
    Get current timestamp in filename-safe format.

    Returns:
        Timestamp string (YYYYMMDD_HHMMSS)
    """
    return datetime.now().strftime('%Y%m%d_%H%M%S')


class AmbiguousMatchError(Exception):
    """
    Raised when multiple users match the data subject criteria.

    Attributes:
        matches: List of matching users
    """
    def __init__(self, message: str, matches: List[Dict]):
        super().__init__(message)
        self.matches = matches


def validate_data_subject_match(
    matches: List[Dict],
    data_subject_name: str,
    data_subject_email: str = None
) -> Dict:
    """
    Validate that we have exactly one data subject match.

    If multiple matches and no email provided, raises AmbiguousMatchError
    with instructions to provide an email.

    Args:
        matches: List of potential matches from the export
        data_subject_name: Name of the data subject
        data_subject_email: Email of the data subject (optional)

    Returns:
        The single matched user

    Raises:
        ValueError: If no matches found
        AmbiguousMatchError: If multiple matches and no email to disambiguate
    """
    if not matches:
        raise ValueError(f"Data subject '{data_subject_name}' not found in export")

    if len(matches) == 1:
        return matches[0]

    # Multiple matches - try to disambiguate by email
    if data_subject_email:
        email_lower = data_subject_email.lower()
        email_matches = [
            m for m in matches
            if (m.get('email') or '').lower() == email_lower
        ]
        if len(email_matches) == 1:
            return email_matches[0]

    # Cannot disambiguate
    match_info = "\n".join([
        f"  - {m.get('name', 'Unknown')} ({m.get('email', 'no email')})"
        for m in matches
    ])
    raise AmbiguousMatchError(
        f"Multiple users match '{data_subject_name}':\n{match_info}\n\n"
        f"Please provide --email to disambiguate.",
        matches
    )
