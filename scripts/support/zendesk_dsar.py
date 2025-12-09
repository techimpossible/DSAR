#!/usr/bin/env python3
"""
Zendesk DSAR Processor

Export source: Zendesk Admin > Channels > API > Data Export
               OR Zendesk Support > Admin > Manage > Reports > Export
Format: JSON with users, tickets, comments, and organizations

Usage:
    python zendesk_dsar.py export.json "John Smith" --email john@company.com
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
    setup_argparser,
    parse_extra_redactions,
    load_json,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    validate_data_subject_match,
    strip_html,
)

VENDOR_NAME = "Zendesk"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Zendesk users."""
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or '').lower()
        user_name = (user.get('name') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': user.get('name'),
                'email': user.get('email'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}
    for user in data.get('users', []):
        user_id = str(user.get('id', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('name'),
                'email': user.get('email'),
            }
    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    return {
        'User ID': data_subject.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Phone': raw.get('phone'),
        'Role': raw.get('role'),
        'Organization ID': raw.get('organization_id'),
        'Locale': raw.get('locale'),
        'Timezone': raw.get('time_zone'),
        'Created At': format_date(raw.get('created_at')),
        'Updated At': format_date(raw.get('updated_at')),
        'Last Login': format_date(raw.get('last_login_at')),
        'Verified': raw.get('verified'),
        'Active': raw.get('active'),
        'Suspended': raw.get('suspended'),
        'Tags': ', '.join(raw.get('tags', [])),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all tickets and comments for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Build ticket lookup
    tickets = {str(t.get('id')): t for t in data.get('tickets', [])}

    # Find tickets where user is requester or submitter
    for ticket in data.get('tickets', []):
        requester_id = str(ticket.get('requester_id', ''))
        submitter_id = str(ticket.get('submitter_id', ''))

        if requester_id == ds_id or submitter_id == ds_id:
            records.append({
                'date': format_date(ticket.get('created_at')),
                'type': 'ticket_created',
                'category': f"Ticket #{ticket.get('id')}",
                'content': f"Subject: {ticket.get('subject')}\nStatus: {ticket.get('status')}\nPriority: {ticket.get('priority')}\nDescription: {strip_html(ticket.get('description', ''))}",
            })

    # Extract comments by the data subject
    for comment in data.get('comments', data.get('ticket_comments', [])):
        author_id = str(comment.get('author_id', ''))
        if author_id == ds_id:
            ticket_id = comment.get('ticket_id')
            ticket = tickets.get(str(ticket_id), {})
            records.append({
                'date': format_date(comment.get('created_at')),
                'type': 'comment',
                'category': f"Ticket #{ticket_id} - {ticket.get('subject', 'Unknown')}",
                'content': strip_html(comment.get('body', comment.get('plain_body', ''))),
            })

    # Extract ticket events
    for event in data.get('ticket_events', []):
        if str(event.get('updater_id', '')) == ds_id:
            records.append({
                'date': format_date(event.get('timestamp') or event.get('created_at')),
                'type': 'ticket_update',
                'category': f"Ticket #{event.get('ticket_id')}",
                'content': f"Event: {event.get('event_type', 'update')}",
            })

    # Sort by date
    records.sort(key=lambda r: r.get('date', ''), reverse=True)
    return records


def process(
    export_path: str,
    data_subject_name: str,
    data_subject_email: str = None,
    extra_redactions: List[str] = None,
    output_dir: str = './output'
) -> tuple:
    """Process a Zendesk export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Zendesk export from {export_path}...")
    data = load_json(export_path)

    print(f"Searching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(data, data_subject_name, data_subject_email)
    ds_id = data_subject['id']
    print(f"  Found: {data_subject['name']} ({data_subject.get('email', 'no email')})")

    print("Building redaction map...")
    engine = RedactionEngine(data_subject_name, data_subject_email)

    users = extract_users(data)
    for user_id, user_info in users.items():
        engine.add_user(user_id, user_info.get('name'), user_info.get('email'))
    print(f"  Mapped {engine.get_total_redactions()} users for redaction")

    for name in (extra_redactions or []):
        engine.add_external(name)

    print("Extracting profile data...")
    profile = extract_profile(data_subject)

    print("Extracting activity records...")
    records = extract_records(data, ds_id)
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

    print("Generating Word report...")
    doc = create_vendor_report(
        vendor_name=VENDOR_NAME,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        profile_data=profile,
        records=redacted_records,
        redaction_stats=engine.get_stats(),
        export_filename=os.path.basename(export_path)
    )
    docx_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.docx")
    doc.save(docx_path)

    print("Generating JSON export...")
    json_data = {
        'vendor': VENDOR_NAME,
        'data_subject': data_subject_name,
        'email': data_subject_email,
        'generated': datetime.now().isoformat(),
        'profile': profile,
        'records': redacted_records,
        'record_count': len(redacted_records),
    }
    json_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.json")
    save_json(json_data, json_path)

    key_path = os.path.join(output_dir, 'internal', f"{VENDOR_NAME}_REDACTION_KEY_{safe_name}_{timestamp}.json")
    save_json(engine.get_redaction_key(), key_path)

    stats = engine.get_stats()
    print(f"\n✓ {VENDOR_NAME}: {len(redacted_records)} records processed")
    print(f"  Redacted: {stats['user']} users, {stats['external']} external")
    print(f"  → {docx_path}")
    print(f"  → {json_path}")

    return docx_path, json_path


if __name__ == '__main__':
    parser = setup_argparser(VENDOR_NAME)
    args = parser.parse_args()

    try:
        process(
            export_path=args.export_path,
            data_subject_name=args.data_subject_name,
            data_subject_email=args.email,
            extra_redactions=parse_extra_redactions(args.redact),
            output_dir=args.output
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
