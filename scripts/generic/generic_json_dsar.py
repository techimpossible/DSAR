#!/usr/bin/env python3
"""
Generic JSON DSAR Processor

Fallback processor for JSON exports from unlisted vendors.
Auto-detects structure and attempts to find data subject.

Usage:
    python generic_json_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redaction import RedactionEngine
from core.docgen import create_vendor_report
from core.utils import (
    setup_argparser, parse_extra_redactions, load_json, save_json,
    ensure_output_dir, safe_filename, format_date, get_timestamp, strip_html,
)

VENDOR_NAME = "Generic_JSON"


def find_users_in_data(data: Any, users: List[Dict] = None, path: str = "") -> List[Dict]:
    """Recursively find objects that look like user records."""
    if users is None:
        users = []

    if isinstance(data, dict):
        # Check if this dict looks like a user
        has_name = any(k.lower() in ['name', 'fullname', 'full_name', 'displayname', 'display_name',
                                      'username', 'user_name'] for k in data.keys())
        has_email = any(k.lower() in ['email', 'emailaddress', 'email_address', 'mail'] for k in data.keys())

        if has_name or has_email:
            users.append({'data': data, 'path': path})

        for key, value in data.items():
            find_users_in_data(value, users, f"{path}.{key}" if path else key)

    elif isinstance(data, list):
        for i, item in enumerate(data):
            find_users_in_data(item, users, f"{path}[{i}]")

    return users


def extract_name_email(obj: Dict) -> tuple:
    """Extract name and email from a user-like object."""
    name = None
    email = None

    for key, value in obj.items():
        key_lower = key.lower()
        if not isinstance(value, str):
            continue

        if key_lower in ['email', 'emailaddress', 'email_address', 'mail']:
            email = value
        elif key_lower in ['name', 'fullname', 'full_name', 'displayname', 'display_name']:
            name = value
        elif key_lower in ['firstname', 'first_name']:
            first = value
            last = obj.get('lastName', obj.get('last_name', obj.get('lastname', '')))
            name = f"{first} {last}".strip()

    return name, email


def find_data_subject(data: Any, name: str, email: str = None) -> Optional[Dict]:
    """Find data subject in generic JSON structure."""
    users = find_users_in_data(data)
    matches = []
    name_lower = name.lower()

    for user_entry in users:
        user_data = user_entry['data']
        user_name, user_email = extract_name_email(user_data)

        if not user_name and not user_email:
            continue

        is_match = False
        if email and user_email and user_email.lower() == email.lower():
            is_match = True
        elif user_name and (name_lower in user_name.lower() or user_name.lower() in name_lower):
            is_match = True

        if is_match:
            matches.append({
                'id': user_data.get('id', user_data.get('Id', user_data.get('_id', user_entry['path']))),
                'name': user_name or 'Unknown',
                'email': user_email,
                'raw': user_data,
                'path': user_entry['path'],
            })

    if not matches:
        raise ValueError(f"Data subject '{name}' not found in export")
    if len(matches) > 1 and not email:
        match_info = "\n".join([f"  - {m['name']} ({m.get('email', 'no email')})" for m in matches])
        raise ValueError(f"Multiple matches found:\n{match_info}\nProvide --email to disambiguate")

    return matches[0]


def extract_users(data: Any) -> Dict[str, Dict]:
    """Extract all user-like objects for redaction."""
    users = {}
    for user_entry in find_users_in_data(data):
        user_data = user_entry['data']
        name, email = extract_name_email(user_data)
        user_id = str(user_data.get('id', user_data.get('Id', user_data.get('_id', user_entry['path']))))
        if name or email:
            users[user_id] = {'name': name, 'email': email}
    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile from detected user object."""
    raw = data_subject.get('raw', {})
    profile = {}

    # Common field mappings
    field_mappings = [
        ('id', ['id', 'Id', '_id', 'userId', 'user_id']),
        ('Name', ['name', 'fullName', 'full_name', 'displayName', 'display_name']),
        ('Email', ['email', 'emailAddress', 'email_address', 'mail']),
        ('Phone', ['phone', 'phoneNumber', 'phone_number', 'mobile']),
        ('Title', ['title', 'jobTitle', 'job_title', 'role']),
        ('Company', ['company', 'organization', 'org']),
        ('Created', ['created', 'createdAt', 'created_at', 'dateCreated']),
        ('Updated', ['updated', 'updatedAt', 'updated_at', 'lastModified']),
    ]

    for display_name, keys in field_mappings:
        for key in keys:
            if key in raw:
                value = raw[key]
                if display_name in ['Created', 'Updated']:
                    value = format_date(value)
                profile[display_name] = value
                break

    # Add any remaining string fields
    for key, value in raw.items():
        if isinstance(value, (str, int, float, bool)) and key not in profile:
            profile[key] = value

    return profile


def find_records_for_user(data: Any, data_subject_id: str, records: List[Dict] = None, path: str = "") -> List[Dict]:
    """Find records associated with the data subject."""
    if records is None:
        records = []

    if isinstance(data, dict):
        # Check if this object is associated with the data subject
        obj_id = str(data.get('userId', data.get('user_id', data.get('authorId', data.get('author_id', '')))))
        obj_email = data.get('email', data.get('userEmail', ''))

        is_associated = obj_id == data_subject_id or obj_email == data_subject_id

        if is_associated:
            # Extract record info
            date_val = data.get('date', data.get('created', data.get('createdAt', data.get('timestamp', ''))))
            type_val = data.get('type', data.get('action', data.get('event', path.split('.')[-1] if path else 'record')))
            content = data.get('content', data.get('text', data.get('body', data.get('message', str(data)))))

            records.append({
                'date': format_date(date_val) if date_val else 'N/A',
                'type': str(type_val)[:30],
                'category': path.split('.')[0] if path else 'general',
                'content': strip_html(str(content)[:1000]),
            })

        for key, value in data.items():
            find_records_for_user(value, data_subject_id, records, f"{path}.{key}" if path else key)

    elif isinstance(data, list):
        for i, item in enumerate(data):
            find_records_for_user(item, data_subject_id, records, f"{path}[{i}]")

    return records


def process(export_path: str, data_subject_name: str, data_subject_email: str = None,
            extra_redactions: List[str] = None, output_dir: str = './output') -> tuple:
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading JSON export from {export_path}...")
    data = load_json(export_path)

    print(f"Searching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(data, data_subject_name, data_subject_email)
    ds_id = str(data_subject['id'])
    print(f"  Found: {data_subject['name']} (path: {data_subject.get('path', 'root')})")

    print("Building redaction map...")
    engine = RedactionEngine(data_subject_name, data_subject_email)
    for user_id, user_info in extract_users(data).items():
        engine.add_user(user_id, user_info.get('name'), user_info.get('email'))
    for name in (extra_redactions or []):
        engine.add_external(name)
    print(f"  Mapped {engine.get_total_redactions()} entities for redaction")

    print("Extracting profile data...")
    profile = extract_profile(data_subject)

    print("Searching for associated records...")
    records = find_records_for_user(data, ds_id)
    print(f"  Found {len(records)} records")

    print("Applying redactions...")
    redacted_records = [{**r, 'content': engine.redact(str(r.get('content', '')))} for r in records]

    safe_name = safe_filename(data_subject_name)
    timestamp = get_timestamp()
    vendor_name = os.path.splitext(os.path.basename(export_path))[0].replace('_export', '').replace('_', ' ').title()
    if vendor_name == 'Export':
        vendor_name = VENDOR_NAME

    print("Generating reports...")
    doc = create_vendor_report(vendor_name, data_subject_name, data_subject_email,
                               profile, redacted_records, redaction_stats=engine.get_stats(),
                               export_filename=os.path.basename(export_path))
    docx_path = os.path.join(output_dir, f"{vendor_name}_DSAR_{safe_name}_{timestamp}.docx")
    doc.save(docx_path)

    json_data = {'vendor': vendor_name, 'data_subject': data_subject_name, 'email': data_subject_email,
                 'generated': datetime.now().isoformat(), 'profile': profile, 'records': redacted_records,
                 'record_count': len(redacted_records)}
    json_path = os.path.join(output_dir, f"{vendor_name}_DSAR_{safe_name}_{timestamp}.json")
    save_json(json_data, json_path)

    key_path = os.path.join(output_dir, 'internal', f"{vendor_name}_REDACTION_KEY_{safe_name}_{timestamp}.json")
    save_json(engine.get_redaction_key(), key_path)

    print(f"\n✓ {vendor_name}: {len(redacted_records)} records processed")
    print(f"  → {docx_path}")
    return docx_path, json_path


if __name__ == '__main__':
    parser = setup_argparser(VENDOR_NAME)
    args = parser.parse_args()
    try:
        process(args.export_path, args.data_subject_name, args.email,
                parse_extra_redactions(args.redact), args.output)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
