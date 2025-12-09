#!/usr/bin/env python3
"""
Generic CSV DSAR Processor

Fallback processor for CSV exports from unlisted vendors.
Auto-detects columns and attempts to find data subject.

Usage:
    python generic_csv_dsar.py export.csv "John Smith" --email john@company.com
    python generic_csv_dsar.py export.csv "John Smith" --email john@company.com --name-col "Full Name" --email-col "Email Address"
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redaction import RedactionEngine
from core.docgen import create_vendor_report
from core.utils import (
    load_csv,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    strip_html,
)

VENDOR_NAME = "Generic_CSV"

# Common column name variations for name fields
NAME_COLUMNS = [
    'name', 'full_name', 'fullname', 'display_name', 'displayname',
    'user_name', 'username', 'customer_name', 'contact_name',
    'first_name', 'firstname', 'fname', 'given_name',
    'person_name', 'member_name', 'employee_name',
]

# Common column name variations for email fields
EMAIL_COLUMNS = [
    'email', 'email_address', 'emailaddress', 'e_mail', 'e-mail',
    'user_email', 'work_email', 'primary_email', 'contact_email',
    'mail', 'email_id',
]

# Common column name variations for ID fields
ID_COLUMNS = [
    'id', 'user_id', 'userid', 'customer_id', 'contact_id',
    'member_id', 'employee_id', 'record_id', '_id', 'uuid',
]

# Common column name variations for date fields
DATE_COLUMNS = [
    'date', 'created', 'created_at', 'createdat', 'timestamp',
    'modified', 'updated', 'updated_at', 'time', 'datetime',
]


def detect_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    """Detect a column from a list of candidates."""
    columns_lower = {c.lower().strip(): c for c in columns}

    for candidate in candidates:
        if candidate in columns_lower:
            return columns_lower[candidate]

    # Fuzzy match - check if candidate is contained in column name
    for candidate in candidates:
        for col_lower, col_original in columns_lower.items():
            if candidate in col_lower:
                return col_original

    return None


def find_data_subject(
    rows: List[Dict],
    name: str,
    email: str = None,
    name_col: str = None,
    email_col: str = None
) -> Optional[Dict]:
    """Find the data subject in CSV rows."""
    if not rows:
        raise ValueError("CSV file is empty")

    columns = list(rows[0].keys())

    # Detect columns if not specified
    if not name_col:
        name_col = detect_column(columns, NAME_COLUMNS)
    if not email_col:
        email_col = detect_column(columns, EMAIL_COLUMNS)

    id_col = detect_column(columns, ID_COLUMNS)

    if not name_col and not email_col:
        raise ValueError(
            f"Could not auto-detect name or email columns. "
            f"Available columns: {', '.join(columns)}. "
            f"Use --name-col and --email-col to specify manually."
        )

    matches = []
    name_lower = name.lower()

    for i, row in enumerate(rows):
        row_email = (row.get(email_col, '') or '').lower().strip() if email_col else ''
        row_name = (row.get(name_col, '') or '').lower().strip() if name_col else ''

        is_match = False
        if email and email_col and row_email == email.lower():
            is_match = True
        elif name_col and (name_lower in row_name or row_name in name_lower):
            is_match = True

        if is_match:
            matches.append({
                'id': row.get(id_col, str(i)) if id_col else str(i),
                'name': row.get(name_col, '') if name_col else '',
                'email': row.get(email_col, '') if email_col else '',
                'raw': row,
                'row_index': i,
            })

    if not matches:
        raise ValueError(
            f"Data subject '{name}' not found in CSV. "
            f"Searched in columns: name={name_col}, email={email_col}"
        )

    if len(matches) > 1 and not email:
        match_info = "\n".join([
            f"  - Row {m['row_index']}: {m['name']} ({m.get('email', 'no email')})"
            for m in matches[:10]
        ])
        raise ValueError(
            f"Multiple matches found:\n{match_info}\n"
            f"Provide --email to disambiguate"
        )

    return matches[0]


def extract_users(
    rows: List[Dict],
    name_col: str = None,
    email_col: str = None
) -> Dict[str, Dict]:
    """Extract all users from CSV for redaction mapping."""
    users = {}

    if not rows:
        return users

    columns = list(rows[0].keys())

    if not name_col:
        name_col = detect_column(columns, NAME_COLUMNS)
    if not email_col:
        email_col = detect_column(columns, EMAIL_COLUMNS)

    id_col = detect_column(columns, ID_COLUMNS)

    for i, row in enumerate(rows):
        user_id = str(row.get(id_col, i)) if id_col else str(i)
        name = row.get(name_col, '') if name_col else ''
        email_val = row.get(email_col, '') if email_col else ''

        if name or email_val:
            users[user_id] = {'name': name, 'email': email_val}

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile from the matched row."""
    raw = data_subject.get('raw', {})
    profile = {}

    for key, value in raw.items():
        if value is not None and str(value).strip():
            # Clean up column names for display
            display_key = key.replace('_', ' ').title()
            profile[display_key] = value

    return profile


def extract_records(
    rows: List[Dict],
    data_subject: Dict,
    name_col: str = None,
    email_col: str = None
) -> List[Dict]:
    """Extract records for the data subject."""
    records = []

    if not rows:
        return records

    columns = list(rows[0].keys())

    if not name_col:
        name_col = detect_column(columns, NAME_COLUMNS)
    if not email_col:
        email_col = detect_column(columns, EMAIL_COLUMNS)

    date_col = detect_column(columns, DATE_COLUMNS)
    id_col = detect_column(columns, ID_COLUMNS)

    ds_name = (data_subject.get('name') or '').lower()
    ds_email = (data_subject.get('email') or '').lower()
    ds_id = str(data_subject.get('id', ''))

    for i, row in enumerate(rows):
        # Check if this row belongs to the data subject
        row_name = (row.get(name_col, '') or '').lower().strip() if name_col else ''
        row_email = (row.get(email_col, '') or '').lower().strip() if email_col else ''
        row_id = str(row.get(id_col, '')) if id_col else ''

        is_match = False
        if ds_email and row_email == ds_email:
            is_match = True
        elif ds_name and (ds_name in row_name or row_name in ds_name):
            is_match = True
        elif ds_id and row_id == ds_id:
            is_match = True

        if is_match:
            # Build content from all columns
            content_parts = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    content_parts.append(f"{key}: {strip_html(str(value))[:200]}")

            date_val = row.get(date_col, '') if date_col else ''

            records.append({
                'date': format_date(date_val) if date_val else f"Row {i}",
                'type': 'record',
                'category': 'Data',
                'content': '\n'.join(content_parts[:20]),  # Limit fields
            })

    return records


def setup_csv_argparser(vendor_name: str) -> argparse.ArgumentParser:
    """Set up argument parser with CSV-specific options."""
    parser = argparse.ArgumentParser(
        description=f'Process {vendor_name} export for DSAR response'
    )
    parser.add_argument('export_path', help='Path to the export file')
    parser.add_argument('data_subject_name', help='Name of the data subject')
    parser.add_argument('--email', '-e', help='Email of the data subject')
    parser.add_argument('--redact', '-r', nargs='*', help='Additional names to redact')
    parser.add_argument('--output', '-o', default='./output', help='Output directory')
    parser.add_argument('--name-col', help='Column name containing person names')
    parser.add_argument('--email-col', help='Column name containing email addresses')
    return parser


def process(
    export_path: str,
    data_subject_name: str,
    data_subject_email: str = None,
    extra_redactions: List[str] = None,
    output_dir: str = './output',
    name_col: str = None,
    email_col: str = None
) -> tuple:
    """Process a CSV export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading CSV export from {export_path}...")
    rows = load_csv(export_path)
    print(f"  Loaded {len(rows)} rows")

    if rows:
        columns = list(rows[0].keys())
        print(f"  Columns: {', '.join(columns[:10])}{'...' if len(columns) > 10 else ''}")

        # Auto-detect columns
        detected_name = name_col or detect_column(columns, NAME_COLUMNS)
        detected_email = email_col or detect_column(columns, EMAIL_COLUMNS)
        print(f"  Detected name column: {detected_name or 'None'}")
        print(f"  Detected email column: {detected_email or 'None'}")

    print(f"\nSearching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(rows, data_subject_name, data_subject_email, name_col, email_col)
    ds_id = data_subject['id']
    print(f"  Found: {data_subject['name']} ({data_subject.get('email', 'no email')})")

    print("Building redaction map...")
    engine = RedactionEngine(data_subject_name, data_subject_email)

    users = extract_users(rows, name_col, email_col)
    for user_id, user_info in users.items():
        engine.add_user(user_id, user_info.get('name'), user_info.get('email'))
    print(f"  Mapped {engine.get_total_redactions()} entities for redaction")

    for name in (extra_redactions or []):
        engine.add_external(name)

    print("Extracting profile data...")
    profile = extract_profile(data_subject)

    print("Extracting records...")
    records = extract_records(rows, data_subject, name_col, email_col)
    print(f"  Found {len(records)} records for data subject")

    print("Applying redactions...")
    redacted_records = []
    for record in records:
        redacted = record.copy()
        if 'content' in redacted:
            redacted['content'] = engine.redact(str(redacted['content']))
        redacted_records.append(redacted)

    safe_name = safe_filename(data_subject_name)
    timestamp = get_timestamp()

    # Derive vendor name from filename
    vendor_name = os.path.splitext(os.path.basename(export_path))[0]
    vendor_name = vendor_name.replace('_export', '').replace('-export', '')
    vendor_name = vendor_name.replace('_', ' ').replace('-', ' ').title()
    if not vendor_name or vendor_name.lower() == 'export':
        vendor_name = VENDOR_NAME

    print("Generating Word report...")
    doc = create_vendor_report(
        vendor_name=vendor_name,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        profile_data=profile,
        records=redacted_records,
        redaction_stats=engine.get_stats(),
        export_filename=os.path.basename(export_path)
    )
    docx_path = os.path.join(output_dir, f"{vendor_name.replace(' ', '_')}_DSAR_{safe_name}_{timestamp}.docx")
    doc.save(docx_path)

    print("Generating JSON export...")
    json_data = {
        'vendor': vendor_name,
        'data_subject': data_subject_name,
        'email': data_subject_email,
        'generated': datetime.now().isoformat(),
        'profile': profile,
        'records': redacted_records,
        'record_count': len(redacted_records),
    }
    json_path = os.path.join(output_dir, f"{vendor_name.replace(' ', '_')}_DSAR_{safe_name}_{timestamp}.json")
    save_json(json_data, json_path)

    key_path = os.path.join(output_dir, 'internal', f"{vendor_name.replace(' ', '_')}_REDACTION_KEY_{safe_name}_{timestamp}.json")
    save_json(engine.get_redaction_key(), key_path)

    stats = engine.get_stats()
    print(f"\n✓ {vendor_name}: {len(redacted_records)} records processed")
    print(f"  Redacted: {stats['user']} users, {stats['external']} external")
    print(f"  → {docx_path}")
    print(f"  → {json_path}")

    return docx_path, json_path


if __name__ == '__main__':
    parser = setup_csv_argparser(VENDOR_NAME)
    args = parser.parse_args()

    extra_redactions = args.redact if args.redact else []

    try:
        process(
            export_path=args.export_path,
            data_subject_name=args.data_subject_name,
            data_subject_email=args.email,
            extra_redactions=extra_redactions,
            output_dir=args.output,
            name_col=args.name_col,
            email_col=args.email_col
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
