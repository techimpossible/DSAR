#!/usr/bin/env python3
"""
Freshdesk DSAR Processor

Export source: Freshdesk Admin > Account > Data Export
               OR Freshdesk API export
Format: JSON with contacts, tickets, conversations, and notes

Usage:
    python freshdesk_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
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

VENDOR_NAME = "Freshdesk"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Freshdesk contacts."""
    contacts = data.get('contacts', data.get('requesters', []))
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
                'id': contact.get('id'),
                'name': contact.get('name'),
                'email': contact.get('email'),
                'raw': contact,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # Contacts
    for contact in data.get('contacts', data.get('requesters', [])):
        contact_id = str(contact.get('id', ''))
        if contact_id:
            users[contact_id] = {
                'name': contact.get('name'),
                'email': contact.get('email'),
            }

    # Agents
    for agent in data.get('agents', []):
        agent_id = str(agent.get('id', ''))
        if agent_id:
            contact_info = agent.get('contact', {})
            users[agent_id] = {
                'name': contact_info.get('name', agent.get('name')),
                'email': contact_info.get('email', agent.get('email')),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    # Extract custom fields
    custom_fields = raw.get('custom_fields', {})
    custom_str = ', '.join([f"{k}: {v}" for k, v in custom_fields.items() if v]) if custom_fields else 'N/A'

    # Extract company
    company = raw.get('company', {})
    company_name = company.get('name', 'N/A') if isinstance(company, dict) else 'N/A'

    return {
        'Contact ID': raw.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Phone': raw.get('phone'),
        'Mobile': raw.get('mobile'),
        'Company': company_name,
        'Job Title': raw.get('job_title'),
        'Language': raw.get('language'),
        'Time Zone': raw.get('time_zone'),
        'Description': raw.get('description'),
        'Address': raw.get('address'),
        'Twitter ID': raw.get('twitter_id'),
        'Facebook ID': raw.get('facebook_id'),
        'Unique External ID': raw.get('unique_external_id'),
        'Active': raw.get('active'),
        'Deleted': raw.get('deleted'),
        'View All Tickets': raw.get('view_all_tickets'),
        'Tags': ', '.join(raw.get('tags', [])) if raw.get('tags') else 'N/A',
        'Custom Fields': custom_str,
        'Created At': format_date(raw.get('created_at')),
        'Updated At': format_date(raw.get('updated_at')),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all tickets and conversations for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Build ticket lookup
    tickets = {}
    for ticket in data.get('tickets', []):
        tickets[str(ticket.get('id', ''))] = ticket

    # Tickets where user is requester
    for ticket in data.get('tickets', []):
        requester_id = str(ticket.get('requester_id', ''))
        requester_email = (ticket.get('requester', {}).get('email', '') if isinstance(ticket.get('requester'), dict) else '').lower()

        if requester_id == ds_id or requester_email == ds_email_lower:
            # Get agent info
            responder = ticket.get('responder_id')
            group = ticket.get('group_id')

            # Status mapping
            status_map = {2: 'Open', 3: 'Pending', 4: 'Resolved', 5: 'Closed'}
            status = status_map.get(ticket.get('status'), str(ticket.get('status', 'Unknown')))

            # Priority mapping
            priority_map = {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Urgent'}
            priority = priority_map.get(ticket.get('priority'), str(ticket.get('priority', 'Unknown')))

            records.append({
                'date': format_date(ticket.get('created_at')),
                'type': 'ticket',
                'category': f"Ticket #{ticket.get('id')}",
                'content': f"Subject: {ticket.get('subject')}\nStatus: {status}\nPriority: {priority}\nType: {ticket.get('type', 'N/A')}\nSource: {ticket.get('source', 'N/A')}\nDescription: {strip_html(ticket.get('description_text', ticket.get('description', '')) or '')[:500]}",
            })

    # Conversations/replies
    for conv in data.get('conversations', data.get('replies', [])):
        ticket_id = str(conv.get('ticket_id', ''))
        ticket = tickets.get(ticket_id, {})

        # Check if user is involved in this ticket
        requester_id = str(ticket.get('requester_id', ''))
        user_id = str(conv.get('user_id', ''))

        if requester_id == ds_id or user_id == ds_id:
            records.append({
                'date': format_date(conv.get('created_at')),
                'type': 'conversation',
                'category': f"Ticket #{ticket_id} - {ticket.get('subject', 'Unknown')}",
                'content': f"From: {conv.get('from_email', 'N/A')}\nTo: {', '.join(conv.get('to_emails', [])) if conv.get('to_emails') else 'N/A'}\nBody: {strip_html(conv.get('body_text', conv.get('body', '')) or '')}",
            })

    # Notes on tickets
    for note in data.get('notes', []):
        ticket_id = str(note.get('ticket_id', ''))
        ticket = tickets.get(ticket_id, {})

        if str(ticket.get('requester_id', '')) == ds_id:
            records.append({
                'date': format_date(note.get('created_at')),
                'type': 'note',
                'category': f"Ticket #{ticket_id}",
                'content': f"Private Note: {note.get('private', True)}\nBody: {strip_html(note.get('body_text', note.get('body', '')) or '')}",
            })

    # Satisfaction ratings
    for rating in data.get('satisfaction_ratings', []):
        ticket_id = str(rating.get('ticket_id', ''))
        ticket = tickets.get(ticket_id, {})

        if str(ticket.get('requester_id', '')) == ds_id or str(rating.get('user_id', '')) == ds_id:
            records.append({
                'date': format_date(rating.get('created_at')),
                'type': 'satisfaction_rating',
                'category': f"Ticket #{ticket_id}",
                'content': f"Rating: {rating.get('rating')}\nFeedback: {rating.get('feedback', 'N/A')}",
            })

    # Time entries
    for entry in data.get('time_entries', []):
        ticket_id = str(entry.get('ticket_id', ''))
        ticket = tickets.get(ticket_id, {})

        if str(ticket.get('requester_id', '')) == ds_id:
            records.append({
                'date': format_date(entry.get('created_at') or entry.get('executed_at')),
                'type': 'time_entry',
                'category': f"Ticket #{ticket_id}",
                'content': f"Time Spent: {entry.get('time_spent', 'N/A')}\nBillable: {entry.get('billable', False)}\nNote: {entry.get('note', 'N/A')}",
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
    """Process a Freshdesk export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Freshdesk export from {export_path}...")
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
