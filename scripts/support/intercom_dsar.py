#!/usr/bin/env python3
"""
Intercom DSAR Processor

Export source: Intercom Settings > General > Data Management > Export Data
               OR Intercom API export
Format: JSON with users, leads, conversations, messages, and events

Usage:
    python intercom_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
import time
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
from core.activity_log import log_event

VENDOR_NAME = "Intercom"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Intercom users/leads/contacts."""
    contacts = (
        data.get('users', []) +
        data.get('leads', []) +
        data.get('contacts', [])
    )
    matches = []
    name_lower = name.lower()

    for contact in contacts:
        contact_email = (contact.get('email') or '').lower()
        contact_name = (contact.get('name') or '').lower()

        is_match = False
        if email and contact_email == email.lower():
            is_match = True
        elif name_lower in contact_name or contact_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': contact.get('id') or contact.get('user_id'),
                'name': contact.get('name'),
                'email': contact.get('email'),
                'raw': contact,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # Contacts (users + leads)
    for contact in data.get('users', []) + data.get('leads', []) + data.get('contacts', []):
        user_id = str(contact.get('id') or contact.get('user_id', ''))
        if user_id:
            users[user_id] = {
                'name': contact.get('name'),
                'email': contact.get('email'),
            }

    # Admins/team members
    for admin in data.get('admins', data.get('team_members', [])):
        admin_id = str(admin.get('id', ''))
        if admin_id:
            users[admin_id] = {
                'name': admin.get('name'),
                'email': admin.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    custom_attributes = raw.get('custom_attributes', {})
    custom_str = ', '.join([f"{k}: {v}" for k, v in custom_attributes.items()]) if custom_attributes else 'N/A'

    tags = raw.get('tags', {}).get('tags', [])
    tag_names = ', '.join([t.get('name', '') for t in tags]) if tags else 'N/A'

    companies = raw.get('companies', {}).get('companies', [])
    company_names = ', '.join([c.get('name', '') for c in companies]) if companies else 'N/A'

    return {
        'User ID': raw.get('id') or raw.get('user_id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Phone': raw.get('phone'),
        'Type': raw.get('type', 'user'),
        'Role': raw.get('role'),
        'Companies': company_names,
        'Tags': tag_names,
        'Location': f"{raw.get('location_data', {}).get('city', '')}, {raw.get('location_data', {}).get('country', '')}".strip(', ') or 'N/A',
        'Browser': raw.get('user_agent_data', raw.get('browser', '')),
        'OS': raw.get('os', ''),
        'Created At': format_date(raw.get('created_at')),
        'Updated At': format_date(raw.get('updated_at')),
        'Last Seen': format_date(raw.get('last_seen_at') or raw.get('last_request_at')),
        'Signed Up': format_date(raw.get('signed_up_at')),
        'Sessions': raw.get('session_count'),
        'Unsubscribed': raw.get('unsubscribed_from_emails'),
        'Custom Attributes': custom_str,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all conversations, messages, and events for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Build conversation lookup for contact's conversations
    user_conversations = set()
    for conv in data.get('conversations', []):
        contacts = conv.get('contacts', {}).get('contacts', [])
        contact_ids = [str(c.get('id', '')) for c in contacts]

        # Also check user field
        user = conv.get('user', {})
        if user:
            contact_ids.append(str(user.get('id', '')))

        if ds_id in contact_ids:
            user_conversations.add(str(conv.get('id', '')))

            # Add conversation record
            records.append({
                'date': format_date(conv.get('created_at')),
                'type': 'conversation',
                'category': f"Conversation #{conv.get('id')}",
                'content': f"State: {conv.get('state', 'unknown')}\nSource: {conv.get('source', {}).get('type', 'unknown')}\nSubject: {conv.get('source', {}).get('subject', 'No subject')}",
            })

    # Extract conversation parts (messages)
    for conv in data.get('conversations', []):
        conv_id = str(conv.get('id', ''))
        if conv_id not in user_conversations:
            continue

        # Get conversation parts
        parts = conv.get('conversation_parts', {}).get('conversation_parts', [])
        for part in parts:
            author = part.get('author', {})
            author_id = str(author.get('id', ''))

            # Include all messages in user's conversations
            records.append({
                'date': format_date(part.get('created_at')),
                'type': part.get('part_type', 'message'),
                'category': f"Conversation #{conv_id}",
                'content': strip_html(part.get('body', '')),
            })

        # Also get source body (initial message)
        source = conv.get('source', {})
        if source.get('body'):
            records.append({
                'date': format_date(conv.get('created_at')),
                'type': 'initial_message',
                'category': f"Conversation #{conv_id}",
                'content': strip_html(source.get('body', '')),
            })

    # Extract events
    for event in data.get('events', data.get('data_events', [])):
        user_id = str(event.get('user_id') or event.get('intercom_user_id', ''))
        if user_id == ds_id:
            metadata = event.get('metadata', {})
            meta_str = ', '.join([f"{k}: {v}" for k, v in metadata.items()]) if metadata else ''

            records.append({
                'date': format_date(event.get('created_at')),
                'type': 'event',
                'category': 'Events',
                'content': f"Event: {event.get('event_name')}\nMetadata: {meta_str}",
            })

    # Extract notes
    for note in data.get('notes', []):
        user = note.get('user', {})
        if str(user.get('id', '')) == ds_id:
            records.append({
                'date': format_date(note.get('created_at')),
                'type': 'note',
                'category': 'Notes',
                'content': strip_html(note.get('body', '')),
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
    """Process an Intercom export for DSAR response."""
    start_time = time.time()

    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    log_event(
        'processing_started',
        output_dir=output_dir,
        vendor=VENDOR_NAME,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        export_file=os.path.basename(export_path),
    )

    try:
        print(f"Loading Intercom export from {export_path}...")
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
        records = extract_records(data, ds_id, data_subject_email)
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

        elapsed = time.time() - start_time
        log_event(
            'processing_complete',
            output_dir=output_dir,
            vendor=VENDOR_NAME,
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            status='success',
            records_found=len(records),
            records_processed=len(redacted_records),
            redaction_stats=stats,
            files_generated=[os.path.basename(docx_path), os.path.basename(json_path)],
            execution_time_seconds=round(elapsed, 2),
        )

        return docx_path, json_path

    except Exception as e:
        elapsed = time.time() - start_time
        log_event(
            'processing_failed',
            output_dir=output_dir,
            vendor=VENDOR_NAME,
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            status='failure',
            error=str(e),
            execution_time_seconds=round(elapsed, 2),
        )
        raise


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
