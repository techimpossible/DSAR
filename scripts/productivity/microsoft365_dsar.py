#!/usr/bin/env python3
"""
Microsoft 365 DSAR Processor

Export source: Microsoft 365 Admin Center > Privacy > Data Subject Requests
               OR Microsoft Graph API export
Format: JSON/ZIP with users, emails, OneDrive files, Teams messages, and calendar

Usage:
    python microsoft365_dsar.py export.json "John Smith" --email john@company.com
    python microsoft365_dsar.py export.zip "John Smith" --email john@company.com
"""

import sys
import os
import zipfile
import json
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

VENDOR_NAME = "Microsoft365"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load Microsoft 365 export from ZIP or JSON."""
    if export_path.endswith('.zip'):
        return load_m365_zip(export_path)
    else:
        return load_json(export_path)


def load_m365_zip(zip_path: str) -> Dict[str, Any]:
    """Load and parse Microsoft 365 ZIP export."""
    data = {
        'user': {},
        'users': [],
        'emails': [],
        'files': [],
        'calendar_events': [],
        'teams_messages': [],
        'chats': [],
        'activity': [],
    }

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            try:
                if name.endswith('.json'):
                    content = json.loads(zf.read(name).decode('utf-8'))
                    basename = os.path.basename(name).lower()

                    # User profile
                    if 'user' in basename or 'profile' in basename:
                        if isinstance(content, dict):
                            data['user'] = content
                            data['users'].append(content)
                        elif isinstance(content, list):
                            data['users'].extend(content)

                    # Emails/messages
                    elif 'mail' in basename or 'email' in basename or 'message' in basename:
                        if isinstance(content, list):
                            data['emails'].extend(content)
                        elif isinstance(content, dict) and content.get('value'):
                            data['emails'].extend(content['value'])

                    # OneDrive files
                    elif 'drive' in basename or 'file' in basename or 'onedrive' in basename:
                        if isinstance(content, list):
                            data['files'].extend(content)
                        elif isinstance(content, dict) and content.get('value'):
                            data['files'].extend(content['value'])

                    # Calendar
                    elif 'calendar' in basename or 'event' in basename:
                        if isinstance(content, list):
                            data['calendar_events'].extend(content)
                        elif isinstance(content, dict) and content.get('value'):
                            data['calendar_events'].extend(content['value'])

                    # Teams
                    elif 'teams' in basename or 'chat' in basename:
                        if isinstance(content, list):
                            data['teams_messages'].extend(content)
                        elif isinstance(content, dict) and content.get('value'):
                            data['teams_messages'].extend(content['value'])

            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Microsoft 365 users."""
    # Check main user first
    user = data.get('user', {})
    if user:
        user_email = (user.get('mail') or user.get('userPrincipalName') or '').lower()
        user_name = (user.get('displayName') or '').lower()
        name_lower = name.lower()

        if (email and user_email == email.lower()) or name_lower in user_name or user_name in name_lower:
            return {
                'id': user.get('id'),
                'name': user.get('displayName'),
                'email': user.get('mail') or user.get('userPrincipalName'),
                'raw': user,
            }

    # Check users list
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('mail') or user.get('userPrincipalName') or '').lower()
        user_name = (user.get('displayName') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': user.get('displayName'),
                'email': user.get('mail') or user.get('userPrincipalName'),
                'raw': user,
            })

    if matches:
        return validate_data_subject_match(matches, name, email)

    # If no match, create from provided info
    return {
        'id': email or name,
        'name': name,
        'email': email,
        'raw': data.get('user', {}),
    }


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    for user in data.get('users', []):
        user_id = str(user.get('id', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('displayName'),
                'email': user.get('mail') or user.get('userPrincipalName'),
            }

    # Extract from emails
    for email in data.get('emails', []):
        sender = email.get('from', {}).get('emailAddress', {})
        if sender.get('address'):
            users[sender['address'].lower()] = {
                'name': sender.get('name'),
                'email': sender.get('address'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    return {
        'User ID': raw.get('id'),
        'Display Name': raw.get('displayName'),
        'Given Name': raw.get('givenName'),
        'Surname': raw.get('surname'),
        'Email': raw.get('mail'),
        'User Principal Name': raw.get('userPrincipalName'),
        'Job Title': raw.get('jobTitle'),
        'Department': raw.get('department'),
        'Office Location': raw.get('officeLocation'),
        'Company Name': raw.get('companyName'),
        'Business Phones': ', '.join(raw.get('businessPhones', [])) if raw.get('businessPhones') else 'N/A',
        'Mobile Phone': raw.get('mobilePhone'),
        'Street Address': raw.get('streetAddress'),
        'City': raw.get('city'),
        'State': raw.get('state'),
        'Postal Code': raw.get('postalCode'),
        'Country': raw.get('country'),
        'Preferred Language': raw.get('preferredLanguage'),
        'Mail Nickname': raw.get('mailNickname'),
        'Account Enabled': raw.get('accountEnabled'),
        'Created DateTime': format_date(raw.get('createdDateTime')),
        'Last Sign In': format_date(raw.get('signInActivity', {}).get('lastSignInDateTime')) if raw.get('signInActivity') else 'N/A',
        'Assigned Licenses': len(raw.get('assignedLicenses', [])) if raw.get('assignedLicenses') else 0,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all emails, files, events, and messages for the data subject."""
    records = []
    ds_email_lower = (data_subject_email or '').lower()

    # Emails
    for email in data.get('emails', []):
        sender = email.get('from', {}).get('emailAddress', {})
        sender_email = (sender.get('address') or '').lower()

        recipients = email.get('toRecipients', [])
        to_emails = [r.get('emailAddress', {}).get('address', '').lower() for r in recipients]

        # Include if user is sender or recipient
        if ds_email_lower in [sender_email] + to_emails or not ds_email_lower:
            records.append({
                'date': format_date(email.get('receivedDateTime') or email.get('sentDateTime')),
                'type': 'email',
                'category': f"Outlook / {email.get('parentFolderId', 'Inbox')}",
                'content': f"Subject: {email.get('subject')}\nFrom: {sender.get('name', '')} <{sender.get('address', '')}>\nTo: {', '.join(to_emails)}\nImportance: {email.get('importance', 'normal')}\nHas Attachments: {email.get('hasAttachments', False)}\nPreview: {strip_html(email.get('bodyPreview', email.get('body', {}).get('content', ''))[:500])}",
            })

    # OneDrive files
    for file in data.get('files', []):
        created_by = file.get('createdBy', {}).get('user', {})
        created_email = (created_by.get('email') or '').lower()

        last_modified_by = file.get('lastModifiedBy', {}).get('user', {})
        modified_email = (last_modified_by.get('email') or '').lower()

        if ds_email_lower in [created_email, modified_email] or not ds_email_lower:
            records.append({
                'date': format_date(file.get('createdDateTime')),
                'type': 'file',
                'category': 'OneDrive',
                'content': f"File: {file.get('name')}\nSize: {file.get('size', 'N/A')} bytes\nPath: {file.get('parentReference', {}).get('path', 'N/A')}\nShared: {bool(file.get('shared'))}\nLast Modified: {format_date(file.get('lastModifiedDateTime'))}",
            })

    # Calendar events
    for event in data.get('calendar_events', []):
        organizer = event.get('organizer', {}).get('emailAddress', {})
        organizer_email = (organizer.get('address') or '').lower()

        attendees = event.get('attendees', [])
        attendee_emails = [a.get('emailAddress', {}).get('address', '').lower() for a in attendees]

        if ds_email_lower in [organizer_email] + attendee_emails or not ds_email_lower:
            start = event.get('start', {})
            end = event.get('end', {})

            attendee_info = ', '.join([f"{a.get('emailAddress', {}).get('name', '')} ({a.get('status', {}).get('response', 'none')})" for a in attendees[:5]])

            records.append({
                'date': format_date(event.get('createdDateTime') or start.get('dateTime')),
                'type': 'calendar_event',
                'category': 'Outlook Calendar',
                'content': f"Event: {event.get('subject', 'Untitled')}\nStart: {format_date(start.get('dateTime'))}\nEnd: {format_date(end.get('dateTime'))}\nLocation: {event.get('location', {}).get('displayName', 'N/A')}\nAttendees: {attendee_info or 'N/A'}\nOrganizer: {organizer.get('name', 'N/A')}",
            })

    # Teams messages
    for msg in data.get('teams_messages', []):
        sender = msg.get('from', {})
        sender_email = (sender.get('user', {}).get('email') or sender.get('emailAddress', {}).get('address') or '').lower()

        if ds_email_lower == sender_email or not ds_email_lower:
            body = msg.get('body', {})
            content = strip_html(body.get('content', '') if isinstance(body, dict) else str(body))

            records.append({
                'date': format_date(msg.get('createdDateTime')),
                'type': 'teams_message',
                'category': 'Microsoft Teams',
                'content': f"Channel/Chat: {msg.get('channelIdentity', {}).get('channelId', msg.get('chatId', 'N/A'))}\nMessage: {content[:500]}",
            })

    # Activity
    for activity in data.get('activity', []):
        records.append({
            'date': format_date(activity.get('activityDateTime') or activity.get('createdDateTime')),
            'type': activity.get('activityType', 'activity'),
            'category': 'Activity',
            'content': f"Activity: {activity.get('activityType', 'unknown')}\nApp: {activity.get('appDisplayName', 'N/A')}\nLocation: {activity.get('location', {}).get('city', 'N/A')}",
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
    """Process a Microsoft 365 export for DSAR response."""
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
        print(f"Loading Microsoft 365 export from {export_path}...")
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
            vendor_name="Microsoft 365",
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
            'vendor': 'Microsoft 365',
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
        print(f"\n✓ Microsoft 365: {len(redacted_records)} records processed")
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
    parser = setup_argparser("Microsoft 365")
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
