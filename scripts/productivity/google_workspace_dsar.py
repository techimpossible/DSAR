#!/usr/bin/env python3
"""
Google Workspace DSAR Processor

Export source: Google Admin Console > Data & personalization > Download your data
               OR Google Takeout (takeout.google.com)
Format: ZIP containing various JSON/MBOX/HTML files from Google services

Usage:
    python google_workspace_dsar.py takeout.zip "John Smith" --email john@company.com
    python google_workspace_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
import zipfile
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

VENDOR_NAME = "Google_Workspace"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load Google Workspace export from ZIP or JSON."""
    if export_path.endswith('.zip'):
        return load_takeout_zip(export_path)
    else:
        return load_json(export_path)


def load_takeout_zip(zip_path: str) -> Dict[str, Any]:
    """Load and parse Google Takeout ZIP export."""
    data = {
        'profile': {},
        'users': [],
        'drive_files': [],
        'calendar_events': [],
        'contacts': [],
        'emails': [],
        'chat_messages': [],
        'activity': [],
    }

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            try:
                if name.endswith('.json'):
                    content = json.loads(zf.read(name).decode('utf-8'))

                    # Profile info
                    if 'Profile' in name or 'profile' in name:
                        if isinstance(content, dict):
                            data['profile'].update(content)

                    # Drive/Docs
                    elif 'Drive' in name or 'Docs' in name:
                        if isinstance(content, list):
                            data['drive_files'].extend(content)
                        elif isinstance(content, dict) and content.get('items'):
                            data['drive_files'].extend(content['items'])

                    # Calendar
                    elif 'Calendar' in name:
                        if isinstance(content, list):
                            data['calendar_events'].extend(content)
                        elif isinstance(content, dict) and content.get('items'):
                            data['calendar_events'].extend(content['items'])

                    # Contacts
                    elif 'Contacts' in name:
                        if isinstance(content, list):
                            data['contacts'].extend(content)
                        elif isinstance(content, dict) and content.get('connections'):
                            data['contacts'].extend(content['connections'])

                    # Chat/Hangouts
                    elif 'Chat' in name or 'Hangouts' in name:
                        if isinstance(content, list):
                            data['chat_messages'].extend(content)
                        elif isinstance(content, dict) and content.get('messages'):
                            data['chat_messages'].extend(content['messages'])

                    # My Activity
                    elif 'My Activity' in name or 'activity' in name.lower():
                        if isinstance(content, list):
                            data['activity'].extend(content)

            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

    # Create user from profile
    if data['profile']:
        data['users'].append(data['profile'])

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Google Workspace data."""
    # For personal exports, the profile IS the data subject
    profile = data.get('profile', {})

    if profile:
        profile_email = (profile.get('email') or profile.get('emailAddress') or '').lower()
        profile_name = (profile.get('displayName') or profile.get('name', {}).get('fullName', '') or '').lower()

        name_lower = name.lower()

        is_match = False
        if email and profile_email == email.lower():
            is_match = True
        elif name_lower in profile_name or profile_name in name_lower:
            is_match = True

        if is_match:
            return {
                'id': profile.get('id') or profile.get('resourceName') or email,
                'name': profile.get('displayName') or profile.get('name', {}).get('fullName', '') or name,
                'email': profile.get('email') or profile.get('emailAddress') or email,
                'raw': profile,
            }

    # If no profile match, check users list
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or user.get('primaryEmail') or '').lower()
        user_name = (user.get('name', {}).get('fullName', '') or user.get('displayName') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': user.get('name', {}).get('fullName', '') or user.get('displayName'),
                'email': user.get('email') or user.get('primaryEmail'),
                'raw': user,
            })

    if matches:
        return validate_data_subject_match(matches, name, email)

    # If still no match, create from provided info
    return {
        'id': email or name,
        'name': name,
        'email': email,
        'raw': data.get('profile', {}),
    }


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # From contacts
    for contact in data.get('contacts', []):
        contact_id = contact.get('resourceName') or contact.get('id', '')
        names = contact.get('names', [])
        emails = contact.get('emailAddresses', [])

        name = names[0].get('displayName', '') if names else ''
        email_addr = emails[0].get('value', '') if emails else ''

        if contact_id:
            users[str(contact_id)] = {'name': name, 'email': email_addr}

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    name_obj = raw.get('name', {})

    # Extract organizations
    orgs = raw.get('organizations', [])
    org_info = orgs[0] if orgs else {}

    # Extract addresses
    addresses = raw.get('addresses', [])
    address_info = addresses[0] if addresses else {}

    # Extract phones
    phones = raw.get('phoneNumbers', raw.get('phones', []))
    phone_list = [p.get('value', p.get('number', '')) for p in phones[:3]] if phones else []

    return {
        'User ID': raw.get('id') or data_subject.get('id'),
        'Display Name': raw.get('displayName'),
        'Given Name': name_obj.get('givenName') or raw.get('givenName'),
        'Family Name': name_obj.get('familyName') or raw.get('familyName'),
        'Email': raw.get('email') or raw.get('primaryEmail') or raw.get('emailAddress'),
        'Phones': ', '.join(phone_list) if phone_list else 'N/A',
        'Organization': org_info.get('name', org_info.get('title', 'N/A')),
        'Job Title': org_info.get('title', raw.get('title', 'N/A')),
        'Department': org_info.get('department', 'N/A'),
        'Address': address_info.get('formattedValue', address_info.get('streetAddress', 'N/A')),
        'Profile Photo': raw.get('thumbnailPhotoUrl') or raw.get('photos', [{}])[0].get('url') if raw.get('photos') else None,
        'Language': raw.get('language') or raw.get('languages', [{}])[0].get('value') if raw.get('languages') else 'N/A',
        'Is Admin': raw.get('isAdmin'),
        'Suspended': raw.get('suspended'),
        'Archived': raw.get('archived'),
        'Created': format_date(raw.get('creationTime')),
        'Last Login': format_date(raw.get('lastLoginTime')),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all activity records for the data subject."""
    records = []
    ds_email_lower = (data_subject_email or '').lower()

    # Drive files
    for file in data.get('drive_files', []):
        # In personal export, all files belong to the user
        owners = file.get('owners', [])
        owner_emails = [o.get('emailAddress', '').lower() for o in owners]

        if not owners or ds_email_lower in owner_emails:
            records.append({
                'date': format_date(file.get('createdTime') or file.get('modifiedTime')),
                'type': 'drive_file',
                'category': 'Google Drive',
                'content': f"File: {file.get('name', file.get('title', 'Untitled'))}\nType: {file.get('mimeType', 'unknown')}\nShared: {file.get('shared', False)}\nTrashed: {file.get('trashed', False)}",
            })

    # Calendar events
    for event in data.get('calendar_events', []):
        creator = event.get('creator', {})
        organizer = event.get('organizer', {})

        creator_email = (creator.get('email') or '').lower()
        organizer_email = (organizer.get('email') or '').lower()

        if not creator or ds_email_lower in [creator_email, organizer_email]:
            start = event.get('start', {})
            end = event.get('end', {})
            start_time = start.get('dateTime') or start.get('date', '')
            end_time = end.get('dateTime') or end.get('date', '')

            # Get attendees
            attendees = event.get('attendees', [])
            attendee_list = ', '.join([a.get('email', '') for a in attendees[:5]])

            records.append({
                'date': format_date(event.get('created') or start_time),
                'type': 'calendar_event',
                'category': 'Google Calendar',
                'content': f"Event: {event.get('summary', 'Untitled')}\nStart: {format_date(start_time)}\nEnd: {format_date(end_time)}\nLocation: {event.get('location', 'N/A')}\nAttendees: {attendee_list or 'N/A'}\nDescription: {strip_html(event.get('description', '') or '')[:300]}",
            })

    # Chat messages
    for msg in data.get('chat_messages', []):
        sender = msg.get('sender', msg.get('creator', {}))
        sender_email = (sender.get('email') or sender.get('name', '') or '').lower()

        if not sender or ds_email_lower in sender_email:
            records.append({
                'date': format_date(msg.get('created_date') or msg.get('timestamp') or msg.get('createTime')),
                'type': 'chat_message',
                'category': 'Google Chat',
                'content': f"Space: {msg.get('space', {}).get('name', msg.get('conversation_id', 'N/A'))}\nMessage: {strip_html(msg.get('text', msg.get('message', '')))}",
            })

    # Activity/history
    for activity in data.get('activity', []):
        records.append({
            'date': format_date(activity.get('time') or activity.get('timestamp')),
            'type': 'activity',
            'category': f"Activity / {activity.get('header', activity.get('product', 'Google'))}",
            'content': f"Title: {activity.get('title', 'N/A')}\nDetails: {activity.get('description', activity.get('details', 'N/A'))}",
        })

    # Contacts (for completeness)
    for contact in data.get('contacts', []):
        names = contact.get('names', [])
        emails = contact.get('emailAddresses', [])

        contact_name = names[0].get('displayName', '') if names else 'Unknown'
        contact_email = emails[0].get('value', '') if emails else 'N/A'

        records.append({
            'date': format_date(contact.get('metadata', {}).get('sources', [{}])[0].get('updateTime')),
            'type': 'contact',
            'category': 'Google Contacts',
            'content': f"Name: {contact_name}\nEmail: {contact_email}\nPhone: {contact.get('phoneNumbers', [{}])[0].get('value', 'N/A') if contact.get('phoneNumbers') else 'N/A'}",
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
    """Process a Google Workspace export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Google Workspace export from {export_path}...")
    data = load_export(export_path)

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
        vendor_name=VENDOR_NAME.replace('_', ' '),
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
        'vendor': VENDOR_NAME.replace('_', ' '),
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
    print(f"\n✓ {VENDOR_NAME.replace('_', ' ')}: {len(redacted_records)} records processed")
    print(f"  Redacted: {stats['user']} users, {stats['external']} external")
    print(f"  → {docx_path}")
    print(f"  → {json_path}")

    return docx_path, json_path


if __name__ == '__main__':
    parser = setup_argparser(VENDOR_NAME.replace('_', ' '))
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
