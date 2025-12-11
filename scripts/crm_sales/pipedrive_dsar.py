#!/usr/bin/env python3
"""
Pipedrive DSAR Processor

Export source: Pipedrive Settings > Data management > Export data
               OR Pipedrive API export
Format: JSON/CSV with persons, deals, activities, and notes

Usage:
    python pipedrive_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Pipedrive"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Pipedrive persons."""
    persons = data.get('persons', data.get('data', []))
    matches = []
    name_lower = name.lower()

    for person in persons:
        # Pipedrive stores emails as list of objects
        emails = person.get('email', [])
        person_emails = []
        for e in emails:
            if isinstance(e, dict):
                person_emails.append((e.get('value') or '').lower())
            elif isinstance(e, str):
                person_emails.append(e.lower())

        person_name = (person.get('name') or '').lower()

        is_match = False
        if email and email.lower() in person_emails:
            is_match = True
        elif name_lower in person_name or person_name in name_lower:
            is_match = True

        if is_match:
            primary_email = person_emails[0] if person_emails else None
            matches.append({
                'id': person.get('id'),
                'name': person.get('name'),
                'email': primary_email,
                'raw': person,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # Persons (contacts)
    for person in data.get('persons', data.get('data', [])):
        person_id = str(person.get('id', ''))
        if person_id:
            emails = person.get('email', [])
            primary_email = None
            if emails:
                first_email = emails[0]
                if isinstance(first_email, dict):
                    primary_email = first_email.get('value')
                elif isinstance(first_email, str):
                    primary_email = first_email

            users[person_id] = {
                'name': person.get('name'),
                'email': primary_email,
            }

    # Pipedrive users (staff)
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

    # Extract emails
    emails = raw.get('email', [])
    email_list = []
    for e in emails:
        if isinstance(e, dict):
            email_list.append(f"{e.get('value', '')} ({e.get('label', 'other')})")
        elif isinstance(e, str):
            email_list.append(e)

    # Extract phones
    phones = raw.get('phone', [])
    phone_list = []
    for p in phones:
        if isinstance(p, dict):
            phone_list.append(f"{p.get('value', '')} ({p.get('label', 'other')})")
        elif isinstance(p, str):
            phone_list.append(p)

    # Organization
    org = raw.get('org_id', {}) or raw.get('organization', {})
    org_name = org.get('name', 'N/A') if isinstance(org, dict) else 'N/A'

    # Owner
    owner = raw.get('owner_id', {})
    owner_name = owner.get('name', 'N/A') if isinstance(owner, dict) else 'N/A'

    return {
        'Person ID': raw.get('id'),
        'Name': raw.get('name'),
        'First Name': raw.get('first_name'),
        'Last Name': raw.get('last_name'),
        'Emails': ', '.join(email_list) if email_list else 'N/A',
        'Phones': ', '.join(phone_list) if phone_list else 'N/A',
        'Organization': org_name,
        'Job Title': raw.get('job_title'),
        'Owner': owner_name,
        'Label': raw.get('label'),
        'Visible To': raw.get('visible_to'),
        'Active': raw.get('active_flag'),
        'Deleted': raw.get('delete_time') is not None,
        'Marketing Status': raw.get('marketing_status'),
        'Open Deals': raw.get('open_deals_count'),
        'Closed Deals': raw.get('closed_deals_count'),
        'Won Deals': raw.get('won_deals_count'),
        'Lost Deals': raw.get('lost_deals_count'),
        'Activities Count': raw.get('activities_count'),
        'Done Activities': raw.get('done_activities_count'),
        'Next Activity Date': format_date(raw.get('next_activity_date')),
        'Last Activity Date': format_date(raw.get('last_activity_date')),
        'Add Time': format_date(raw.get('add_time')),
        'Update Time': format_date(raw.get('update_time')),
        'Picture URL': raw.get('picture_id', {}).get('pictures', {}).get('128') if isinstance(raw.get('picture_id'), dict) else None,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all deals, activities, and notes for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Deals involving the person
    for deal in data.get('deals', []):
        person = deal.get('person_id', {})
        person_id = str(person.get('id', '') if isinstance(person, dict) else person or '')

        if person_id == ds_id:
            org = deal.get('org_id', {})
            org_name = org.get('name', 'N/A') if isinstance(org, dict) else 'N/A'

            records.append({
                'date': format_date(deal.get('add_time')),
                'type': 'deal',
                'category': 'Deals',
                'content': f"Deal: {deal.get('title')}\nValue: {deal.get('currency', '')} {deal.get('value', 'N/A')}\nStage: {deal.get('stage_id')}\nStatus: {deal.get('status')}\nOrganization: {org_name}\nWon: {format_date(deal.get('won_time')) or 'N/A'}\nLost: {format_date(deal.get('lost_time')) or 'N/A'}",
            })

    # Activities
    for activity in data.get('activities', []):
        # Check person or participants
        person = activity.get('person_id', {})
        person_id = str(person.get('id', '') if isinstance(person, dict) else person or '')

        participants = activity.get('participants', [])
        participant_ids = [str(p.get('person_id', '')) for p in participants]

        if person_id == ds_id or ds_id in participant_ids:
            records.append({
                'date': format_date(activity.get('due_date') or activity.get('add_time')),
                'type': activity.get('type', 'activity'),
                'category': 'Activities',
                'content': f"Activity: {activity.get('subject')}\nType: {activity.get('type')}\nDone: {activity.get('done')}\nNote: {strip_html(activity.get('note', '') or '')[:500]}",
            })

    # Notes
    for note in data.get('notes', []):
        person = note.get('person_id', {})
        person_id = str(person.get('id', '') if isinstance(person, dict) else person or '')

        if person_id == ds_id:
            records.append({
                'date': format_date(note.get('add_time')),
                'type': 'note',
                'category': 'Notes',
                'content': strip_html(note.get('content', '')),
            })

    # Emails (if exported)
    for email_record in data.get('mail_messages', data.get('emails', [])):
        # Check if person is in to/from/cc
        from_addr = (email_record.get('from', {}).get('email_address') or '').lower()
        to_addrs = [t.get('email_address', '').lower() for t in email_record.get('to', [])]
        cc_addrs = [c.get('email_address', '').lower() for c in email_record.get('cc', [])]

        all_addrs = [from_addr] + to_addrs + cc_addrs

        if ds_email_lower and ds_email_lower in all_addrs:
            records.append({
                'date': format_date(email_record.get('message_time') or email_record.get('add_time')),
                'type': 'email',
                'category': 'Emails',
                'content': f"Subject: {email_record.get('subject')}\nFrom: {from_addr}\nTo: {', '.join(to_addrs)}\nSnippet: {email_record.get('snippet', '')[:300]}",
            })

    # Files/attachments
    for file in data.get('files', []):
        person = file.get('person_id', {})
        person_id = str(person.get('id', '') if isinstance(person, dict) else person or '')

        if person_id == ds_id:
            records.append({
                'date': format_date(file.get('add_time')),
                'type': 'file',
                'category': 'Files',
                'content': f"File: {file.get('name') or file.get('file_name')}\nType: {file.get('file_type', 'unknown')}\nSize: {file.get('file_size', 'N/A')} bytes",
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
    """Process a Pipedrive export for DSAR response."""
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
        print(f"Loading Pipedrive export from {export_path}...")
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
        print(f"  Mapped {engine.get_total_redactions()} persons for redaction")

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
