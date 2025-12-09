#!/usr/bin/env python3
"""
Trello DSAR Processor

Export source: Trello Settings > Export Data
               OR Trello API export
Format: JSON with members, boards, cards, comments, and attachments

Usage:
    python trello_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Trello"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Trello members."""
    members = data.get('members', data.get('users', []))

    # Also check main member if this is a personal export
    if data.get('id') and data.get('fullName'):
        members = [data] + members

    matches = []
    name_lower = name.lower()

    for member in members:
        member_email = (member.get('email') or '').lower()
        full_name = (member.get('fullName') or member.get('full_name') or '').lower()
        username = (member.get('username') or '').lower()

        is_match = False
        if email and member_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True
        elif name_lower == username:
            is_match = True

        if is_match:
            matches.append({
                'id': member.get('id'),
                'name': member.get('fullName') or member.get('full_name') or member.get('username'),
                'email': member.get('email'),
                'raw': member,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all members for redaction mapping."""
    users = {}

    for member in data.get('members', data.get('users', [])):
        member_id = str(member.get('id', ''))
        if member_id:
            users[member_id] = {
                'name': member.get('fullName') or member.get('full_name') or member.get('username'),
                'email': member.get('email'),
            }
            # Also index by username
            if member.get('username'):
                users[member.get('username')] = users[member_id]

    # Extract members from boards
    for board in data.get('boards', []):
        for member in board.get('members', []):
            member_id = str(member.get('id') or member.get('idMember', ''))
            if member_id and member_id not in users:
                users[member_id] = {
                    'name': member.get('fullName') or member.get('username'),
                    'email': member.get('email'),
                }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    prefs = raw.get('prefs', {})

    return {
        'Member ID': raw.get('id'),
        'Username': raw.get('username'),
        'Full Name': raw.get('fullName') or raw.get('full_name'),
        'Email': raw.get('email'),
        'Initials': raw.get('initials'),
        'Bio': raw.get('bio'),
        'URL': raw.get('url'),
        'Avatar Hash': raw.get('avatarHash'),
        'Avatar URL': raw.get('avatarUrl'),
        'Member Type': raw.get('memberType'),
        'Status': raw.get('status'),
        'Confirmed': raw.get('confirmed'),
        'Locale': prefs.get('locale') if prefs else None,
        'Timezone': prefs.get('timezone') if prefs else None,
        'Color Blind': prefs.get('colorBlind') if prefs else None,
        'Activity Blocked': raw.get('activityBlocked'),
        'Limit': raw.get('limits', {}).get('boards', {}).get('totalPerMember', {}).get('status') if raw.get('limits') else None,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all boards, cards, comments, and activity for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Build board and list lookups
    boards = {}
    lists = {}

    for board in data.get('boards', []):
        board_id = str(board.get('id', ''))
        boards[board_id] = board.get('name', 'Unknown Board')

        for lst in board.get('lists', []):
            list_id = str(lst.get('id', ''))
            lists[list_id] = {'name': lst.get('name', 'Unknown List'), 'board': board.get('name')}

    # Board memberships
    for board in data.get('boards', []):
        memberships = board.get('memberships', board.get('members', []))
        for membership in memberships:
            member_id = str(membership.get('idMember') or membership.get('id', ''))
            if member_id == ds_id:
                records.append({
                    'date': format_date(board.get('dateLastActivity')),
                    'type': 'board_membership',
                    'category': 'Boards',
                    'content': f"Board: {board.get('name')}\nRole: {membership.get('memberType', 'normal')}\nDescription: {board.get('desc', 'N/A')}\nStatus: {'Closed' if board.get('closed') else 'Open'}",
                })

    # Cards created/assigned to user
    for board in data.get('boards', []):
        board_name = board.get('name', 'Unknown')
        for card in board.get('cards', []):
            member_ids = [str(m) for m in card.get('idMembers', [])]
            creator_id = str(card.get('idMemberCreator', ''))

            if ds_id in member_ids or creator_id == ds_id:
                list_info = lists.get(str(card.get('idList', '')), {})
                list_name = list_info.get('name', 'Unknown List')

                role = []
                if ds_id in member_ids:
                    role.append('assigned')
                if creator_id == ds_id:
                    role.append('creator')

                records.append({
                    'date': format_date(card.get('dateLastActivity')),
                    'type': 'card',
                    'category': f"{board_name} / {list_name}",
                    'content': f"Card: {card.get('name')}\nRole: {', '.join(role)}\nDue: {format_date(card.get('due')) or 'No due date'}\nDescription: {strip_html(card.get('desc', '') or '')[:500]}",
                })

    # Also check standalone cards list
    for card in data.get('cards', []):
        member_ids = [str(m) for m in card.get('idMembers', [])]
        creator_id = str(card.get('idMemberCreator', ''))

        if ds_id in member_ids or creator_id == ds_id:
            board_name = boards.get(str(card.get('idBoard', '')), 'Unknown Board')
            list_info = lists.get(str(card.get('idList', '')), {})
            list_name = list_info.get('name', 'Unknown List')

            role = []
            if ds_id in member_ids:
                role.append('assigned')
            if creator_id == ds_id:
                role.append('creator')

            records.append({
                'date': format_date(card.get('dateLastActivity')),
                'type': 'card',
                'category': f"{board_name} / {list_name}",
                'content': f"Card: {card.get('name')}\nRole: {', '.join(role)}\nDue: {format_date(card.get('due')) or 'No due date'}\nDescription: {strip_html(card.get('desc', '') or '')[:500]}",
            })

    # Comments/actions
    for action in data.get('actions', []):
        member_creator = action.get('memberCreator', {})
        if str(member_creator.get('id', '')) == ds_id or str(action.get('idMemberCreator', '')) == ds_id:
            action_type = action.get('type', 'unknown')
            action_data = action.get('data', {})

            # Get context
            board_name = action_data.get('board', {}).get('name', 'Unknown Board')
            card_name = action_data.get('card', {}).get('name', '')
            list_name = action_data.get('list', {}).get('name', '')

            content = f"Action: {action_type}"
            if card_name:
                content += f"\nCard: {card_name}"
            if list_name:
                content += f"\nList: {list_name}"

            # For comments, include the text
            if action_type == 'commentCard':
                content += f"\nComment: {action_data.get('text', '')}"

            records.append({
                'date': format_date(action.get('date')),
                'type': action_type,
                'category': f"Activity / {board_name}",
                'content': content,
            })

    # Attachments
    for attachment in data.get('attachments', []):
        if str(attachment.get('idMemberCreator', '')) == ds_id:
            records.append({
                'date': format_date(attachment.get('date')),
                'type': 'attachment',
                'category': 'Attachments',
                'content': f"File: {attachment.get('name')}\nType: {attachment.get('mimeType', 'unknown')}\nURL: {attachment.get('url', 'N/A')}",
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
    """Process a Trello export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Trello export from {export_path}...")
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
    print(f"  Mapped {engine.get_total_redactions()} members for redaction")

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
