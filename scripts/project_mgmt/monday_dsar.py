#!/usr/bin/env python3
"""
Monday.com DSAR Processor

Export source: Monday.com Admin > Account > Export Account Data
               OR Monday.com API export
Format: JSON with users, boards, items, and updates

Usage:
    python monday_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Monday"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Monday.com users."""
    users = data.get('users', data.get('data', {}).get('users', []))
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

    for user in data.get('users', data.get('data', {}).get('users', [])):
        user_id = str(user.get('id', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('name'),
                'email': user.get('email'),
            }

    # Extract from boards/items
    for board in data.get('boards', data.get('data', {}).get('boards', [])):
        for subscriber in board.get('subscribers', []):
            sub_id = str(subscriber.get('id', ''))
            if sub_id and sub_id not in users:
                users[sub_id] = {
                    'name': subscriber.get('name'),
                    'email': subscriber.get('email'),
                }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    # Extract teams
    teams = raw.get('teams', [])
    team_names = ', '.join([t.get('name', '') for t in teams]) if teams else 'N/A'

    return {
        'User ID': raw.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Phone': raw.get('phone'),
        'Title': raw.get('title'),
        'Birthday': raw.get('birthday'),
        'Country Code': raw.get('country_code'),
        'Location': raw.get('location'),
        'Timezone': raw.get('time_zone_identifier'),
        'Photo (Small)': raw.get('photo_small'),
        'Photo (Original)': raw.get('photo_original'),
        'Is Admin': raw.get('is_admin'),
        'Is Guest': raw.get('is_guest'),
        'Is Pending': raw.get('is_pending'),
        'Is Verified': raw.get('is_verified'),
        'Is View Only': raw.get('is_view_only'),
        'Enabled': raw.get('enabled'),
        'Account': raw.get('account', {}).get('name') if isinstance(raw.get('account'), dict) else None,
        'Teams': team_names,
        'Created At': format_date(raw.get('created_at')),
        'Join Date': format_date(raw.get('join_date')),
        'Last Activity': format_date(raw.get('last_activity')),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all boards, items, and updates for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Build board lookup
    boards_map = {}
    for board in data.get('boards', data.get('data', {}).get('boards', [])):
        boards_map[str(board.get('id', ''))] = board.get('name', 'Unknown Board')

    # Board subscriptions/memberships
    for board in data.get('boards', data.get('data', {}).get('boards', [])):
        subscribers = board.get('subscribers', [])
        subscriber_ids = [str(s.get('id', '')) for s in subscribers]

        owner_ids = []
        for owner in board.get('owners', []):
            owner_ids.append(str(owner.get('id', '')))

        if ds_id in subscriber_ids or ds_id in owner_ids:
            role = []
            if ds_id in owner_ids:
                role.append('owner')
            if ds_id in subscriber_ids:
                role.append('subscriber')

            records.append({
                'date': format_date(board.get('created_at') or board.get('updated_at')),
                'type': 'board_membership',
                'category': 'Boards',
                'content': f"Board: {board.get('name')}\nRole: {', '.join(role)}\nDescription: {board.get('description', 'N/A')}\nState: {board.get('state', 'active')}",
            })

        # Items on this board
        for item in board.get('items', board.get('items_page', {}).get('items', [])):
            creator_id = str(item.get('creator_id') or item.get('creator', {}).get('id', ''))

            # Check subscribers on item
            item_subscribers = item.get('subscribers', [])
            item_sub_ids = [str(s.get('id', '')) for s in item_subscribers]

            if creator_id == ds_id or ds_id in item_sub_ids:
                # Extract column values
                column_values = item.get('column_values', [])
                col_str = '\n'.join([f"  {cv.get('title', cv.get('id', ''))}: {cv.get('text', cv.get('value', ''))}" for cv in column_values[:5]]) if column_values else ''

                records.append({
                    'date': format_date(item.get('created_at') or item.get('updated_at')),
                    'type': 'item',
                    'category': f"Items / {board.get('name', 'Unknown')}",
                    'content': f"Item: {item.get('name')}\nGroup: {item.get('group', {}).get('title', 'N/A') if isinstance(item.get('group'), dict) else 'N/A'}\nState: {item.get('state', 'active')}\n{col_str}",
                })

            # Updates/comments on this item
            for update in item.get('updates', []):
                update_creator = update.get('creator', {})
                if str(update_creator.get('id', '') or update.get('creator_id', '')) == ds_id:
                    records.append({
                        'date': format_date(update.get('created_at')),
                        'type': 'update',
                        'category': f"Updates / {board.get('name', 'Unknown')}",
                        'content': f"Item: {item.get('name')}\nUpdate: {strip_html(update.get('body', update.get('text_body', '')))}",
                    })

    # Also check standalone items list
    for item in data.get('items', []):
        creator_id = str(item.get('creator_id') or item.get('creator', {}).get('id', ''))
        board_id = str(item.get('board', {}).get('id', item.get('board_id', '')))
        board_name = boards_map.get(board_id, 'Unknown Board')

        if creator_id == ds_id:
            records.append({
                'date': format_date(item.get('created_at')),
                'type': 'item',
                'category': f"Items / {board_name}",
                'content': f"Item: {item.get('name')}\nState: {item.get('state', 'active')}",
            })

    # Standalone updates
    for update in data.get('updates', []):
        creator = update.get('creator', {})
        if str(creator.get('id', '') or update.get('creator_id', '')) == ds_id:
            item_name = update.get('item', {}).get('name', 'Unknown') if isinstance(update.get('item'), dict) else 'Unknown'
            records.append({
                'date': format_date(update.get('created_at')),
                'type': 'update',
                'category': 'Updates',
                'content': f"Item: {item_name}\nUpdate: {strip_html(update.get('body', update.get('text_body', '')))}",
            })

    # Activity log
    for activity in data.get('activity_logs', []):
        user_id = str(activity.get('user_id', '') or activity.get('user', {}).get('id', ''))
        if user_id == ds_id:
            records.append({
                'date': format_date(activity.get('created_at')),
                'type': activity.get('event', 'activity'),
                'category': 'Activity Log',
                'content': f"Event: {activity.get('event', 'unknown')}\nData: {str(activity.get('data', ''))[:200]}",
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
    """Process a Monday.com export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Monday.com export from {export_path}...")
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
