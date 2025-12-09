#!/usr/bin/env python3
"""
Slack DSAR Processor

Export source: Slack Admin > Settings > Org Settings > Data > Export Data
               OR Corporate Export via Slack Support
Format: ZIP containing users.json, channels.json, integration_logs.json,
        and per-channel directories with message JSON files

Usage:
    python slack_dsar.py export.zip "John Smith" --email john@company.com
    python slack_dsar.py export.zip "Jane Doe" -e jane@company.com -o ./output

Slack Export Structure:
    export.zip/
    ├── users.json          # All workspace users
    ├── channels.json       # All channels
    ├── integration_logs.json  # App/bot activity (optional)
    ├── #general/
    │   ├── 2024-01-01.json
    │   ├── 2024-01-02.json
    │   └── ...
    ├── #random/
    │   └── ...
    └── ...
"""

import sys
import os
import json
import zipfile
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redaction import RedactionEngine
from core.docgen import create_vendor_report
from core.utils import (
    setup_argparser,
    parse_extra_redactions,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    validate_data_subject_match,
    print_progress,
    strip_html,
)

VENDOR_NAME = "Slack"


def load_slack_export(zip_path: str) -> Dict[str, Any]:
    """
    Load Slack export from ZIP file.

    Args:
        zip_path: Path to the Slack export ZIP

    Returns:
        Dictionary with users, channels, and messages
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Export file not found: {zip_path}")

    data = {
        'users': [],
        'channels': [],
        'messages': [],
        'integration_logs': [],
    }

    with zipfile.ZipFile(zip_path, 'r') as zf:
        file_list = zf.namelist()

        # Load users.json
        if 'users.json' in file_list:
            content = zf.read('users.json').decode('utf-8')
            data['users'] = json.loads(content)

        # Load channels.json
        if 'channels.json' in file_list:
            content = zf.read('channels.json').decode('utf-8')
            data['channels'] = json.loads(content)

        # Load integration_logs.json if present
        if 'integration_logs.json' in file_list:
            content = zf.read('integration_logs.json').decode('utf-8')
            data['integration_logs'] = json.loads(content)

        # Load messages from channel directories
        message_files = [
            f for f in file_list
            if f.endswith('.json')
            and f not in ['users.json', 'channels.json', 'integration_logs.json']
            and '/' in f
        ]

        for msg_file in message_files:
            try:
                content = zf.read(msg_file).decode('utf-8')
                messages = json.loads(content)

                # Extract channel name from path
                channel_dir = msg_file.split('/')[0]

                for msg in messages:
                    msg['_channel'] = channel_dir
                    msg['_source_file'] = msg_file
                    data['messages'].append(msg)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """
    Find the data subject in the Slack users list.

    Args:
        data: Loaded Slack export data
        name: Data subject's name
        email: Data subject's email (optional but recommended)

    Returns:
        User dictionary or None if not found
    """
    users = data.get('users', [])
    matches = []

    name_lower = name.lower()

    for user in users:
        # Skip deleted users and bots
        if user.get('deleted', False):
            continue

        profile = user.get('profile', {})
        user_email = profile.get('email', '').lower()
        real_name = profile.get('real_name', '').lower()
        display_name = profile.get('display_name', '').lower()
        full_name = f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip().lower()

        # Check for matches
        is_match = False

        # Email match (most reliable)
        if email and user_email == email.lower():
            is_match = True
        # Name matches
        elif name_lower in real_name or real_name in name_lower:
            is_match = True
        elif name_lower in display_name or display_name in name_lower:
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': profile.get('real_name') or profile.get('display_name') or user.get('name'),
                'email': profile.get('email'),
                'profile': profile,
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """
    Extract all users for redaction mapping.

    Args:
        data: Loaded Slack export data

    Returns:
        Dictionary mapping user_id to user info
    """
    users = {}

    for user in data.get('users', []):
        user_id = user.get('id')
        if not user_id:
            continue

        profile = user.get('profile', {})
        users[user_id] = {
            'name': profile.get('real_name') or profile.get('display_name') or user.get('name'),
            'email': profile.get('email'),
            'is_bot': user.get('is_bot', False),
        }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """
    Extract profile data for the data subject.

    Args:
        data_subject: Data subject's user dictionary

    Returns:
        Profile information dictionary
    """
    profile = data_subject.get('profile', {})
    raw = data_subject.get('raw', {})

    return {
        'User ID': data_subject.get('id'),
        'Display Name': profile.get('display_name'),
        'Real Name': profile.get('real_name'),
        'First Name': profile.get('first_name'),
        'Last Name': profile.get('last_name'),
        'Email': profile.get('email'),
        'Phone': profile.get('phone'),
        'Title': profile.get('title'),
        'Status Text': profile.get('status_text'),
        'Status Emoji': profile.get('status_emoji'),
        'Timezone': raw.get('tz'),
        'Timezone Label': raw.get('tz_label'),
        'Account Created': format_date(raw.get('updated')) if raw.get('updated') else 'N/A',
        'Is Admin': raw.get('is_admin', False),
        'Is Owner': raw.get('is_owner', False),
        'Has 2FA': raw.get('has_2fa', False),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """
    Extract all messages and activities for the data subject.

    Args:
        data: Loaded Slack export data
        data_subject_id: User ID of the data subject

    Returns:
        List of activity records
    """
    records = []

    # Build channel name lookup
    channel_names = {}
    for channel in data.get('channels', []):
        channel_id = channel.get('id')
        channel_name = channel.get('name', channel_id)
        channel_names[channel_id] = channel_name

    # Process messages
    messages = data.get('messages', [])
    total = len(messages)

    for i, msg in enumerate(messages):
        print_progress(i + 1, total, "Processing messages")

        # Only include messages from the data subject
        if msg.get('user') != data_subject_id:
            continue

        # Get channel name
        channel = msg.get('_channel', 'unknown')
        # Clean up channel directory name (remove leading characters)
        if channel.startswith('#'):
            channel = channel[1:]

        # Parse timestamp
        ts = msg.get('ts', '')
        try:
            dt = datetime.fromtimestamp(float(ts))
            date_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            date_str = ts

        # Get message content
        text = msg.get('text', '')

        # Determine message type
        msg_type = 'message'
        subtype = msg.get('subtype')
        if subtype:
            msg_type = subtype

        # Handle special message types
        if msg.get('files'):
            msg_type = 'file_share'
            file_names = [f.get('name', 'file') for f in msg.get('files', [])]
            text = f"{text}\n[Files: {', '.join(file_names)}]"

        if msg.get('attachments'):
            msg_type = 'message_with_attachment'

        if msg.get('reactions'):
            reactions = [r.get('name') for r in msg.get('reactions', [])]
            text = f"{text}\n[Reactions received: {', '.join(reactions)}]"

        records.append({
            'date': date_str,
            'type': msg_type,
            'category': f"#{channel}",
            'content': text,
            'thread_ts': msg.get('thread_ts'),
            'reply_count': msg.get('reply_count', 0),
        })

    return records


def extract_channel_memberships(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[str]:
    """
    Extract channels the data subject is a member of.

    Args:
        data: Loaded Slack export data
        data_subject_id: User ID of the data subject

    Returns:
        List of channel names
    """
    memberships = []

    for channel in data.get('channels', []):
        members = channel.get('members', [])
        if data_subject_id in members:
            channel_name = channel.get('name', channel.get('id'))
            channel_type = 'private' if channel.get('is_private') else 'public'
            memberships.append(f"#{channel_name} ({channel_type})")

    return sorted(memberships)


def process(
    export_path: str,
    data_subject_name: str,
    data_subject_email: str = None,
    extra_redactions: List[str] = None,
    output_dir: str = './output'
) -> tuple:
    """
    Process a Slack export for DSAR response.

    Args:
        export_path: Path to the Slack export ZIP
        data_subject_name: Full name of the data subject
        data_subject_email: Email of the data subject
        extra_redactions: Additional names to redact
        output_dir: Output directory for reports

    Returns:
        Tuple of (docx_path, json_path)
    """
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    # 1. Load export
    print(f"Loading Slack export from {export_path}...")
    data = load_slack_export(export_path)
    print(f"  Found {len(data['users'])} users, {len(data['channels'])} channels, {len(data['messages'])} messages")

    # 2. Find data subject
    print(f"Searching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(data, data_subject_name, data_subject_email)
    ds_id = data_subject['id']
    print(f"  Found: {data_subject['name']} ({data_subject.get('email', 'no email')})")

    # 3. Build redaction engine
    print("Building redaction map...")
    engine = RedactionEngine(data_subject_name, data_subject_email)

    # 4. Add all users to redaction map
    users = extract_users(data)
    for user_id, user_info in users.items():
        engine.add_user(
            user_id,
            user_info.get('name'),
            user_info.get('email'),
            user_info.get('is_bot', False)
        )
    print(f"  Mapped {engine.get_total_redactions()} users for redaction")

    # 5. Add extra redactions
    for name in (extra_redactions or []):
        engine.add_external(name)

    # 6. Extract profile and records
    print("Extracting profile data...")
    profile = extract_profile(data_subject)

    print("Extracting activity records...")
    records = extract_records(data, ds_id)
    print(f"  Found {len(records)} records for data subject")

    # 7. Extract channel memberships
    memberships = extract_channel_memberships(data, ds_id)

    # 8. Apply redaction to records
    print("Applying redactions...")
    redacted_records = []
    for i, record in enumerate(records):
        print_progress(i + 1, len(records), "Redacting")
        redacted = record.copy()
        if 'content' in redacted:
            redacted['content'] = engine.redact(str(redacted['content']))
        redacted_records.append(redacted)

    # 9. Generate outputs
    safe_name = safe_filename(data_subject_name)
    timestamp = get_timestamp()

    # Word document
    print("Generating Word report...")
    doc = create_vendor_report(
        vendor_name=VENDOR_NAME,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        profile_data=profile,
        records=redacted_records,
        categories=memberships,
        redaction_stats=engine.get_stats(),
        export_filename=os.path.basename(export_path)
    )
    docx_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.docx")
    doc.save(docx_path)

    # JSON export
    print("Generating JSON export...")
    json_data = {
        'vendor': VENDOR_NAME,
        'data_subject': data_subject_name,
        'email': data_subject_email,
        'generated': datetime.now().isoformat(),
        'profile': profile,
        'memberships': memberships,
        'records': redacted_records,
        'record_count': len(redacted_records),
    }
    json_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.json")
    save_json(json_data, json_path)

    # Redaction key (INTERNAL)
    key_path = os.path.join(
        output_dir, 'internal',
        f"{VENDOR_NAME}_REDACTION_KEY_{safe_name}_{timestamp}.json"
    )
    save_json(engine.get_redaction_key(), key_path)

    # Summary
    stats = engine.get_stats()
    print(f"\n✓ {VENDOR_NAME}: {len(redacted_records)} records processed")
    print(f"  Redacted: {stats['user']} users, {stats['bot']} bots, {stats['external']} external")
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
