#!/usr/bin/env python3
"""
Notion DSAR Processor

Export source: Notion Settings > Workspace settings > Export all workspace content
               OR Notion API export via integration
Format: ZIP containing markdown/CSV files or JSON API export

Usage:
    python notion_dsar.py export.zip "John Smith" --email john@company.com
    python notion_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Notion"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load Notion export from ZIP or JSON."""
    if export_path.endswith('.zip'):
        return load_zip_export(export_path)
    else:
        return load_json(export_path)


def load_zip_export(zip_path: str) -> Dict[str, Any]:
    """Load and parse Notion ZIP export."""
    data = {
        'users': [],
        'pages': [],
        'databases': [],
        'comments': [],
    }

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if name.endswith('.json'):
                try:
                    content = json.loads(zf.read(name).decode('utf-8'))
                    if isinstance(content, dict):
                        if 'users' in content:
                            data['users'].extend(content['users'])
                        if 'pages' in content or 'results' in content:
                            pages = content.get('pages', content.get('results', []))
                            data['pages'].extend(pages if isinstance(pages, list) else [])
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
            elif name.endswith('.md') or name.endswith('.csv'):
                # Track markdown/csv files as pages
                data['pages'].append({
                    'id': name,
                    'title': os.path.splitext(os.path.basename(name))[0],
                    'file_path': name,
                    'type': 'file',
                })

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Notion users."""
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or user.get('person', {}).get('email') or '').lower()
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
                'email': user.get('email') or user.get('person', {}).get('email'),
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
                'email': user.get('email') or user.get('person', {}).get('email'),
            }
    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    person = raw.get('person', {})

    return {
        'User ID': data_subject.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email') or person.get('email'),
        'Type': raw.get('type'),
        'Avatar URL': raw.get('avatar_url'),
        'Object Type': raw.get('object'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_name: str = None,
    data_subject_email: str = None
) -> List[Dict]:
    """
    Extract all pages, blocks, and comments for the data subject.

    GDPR Compliance: Includes content where the data subject is:
    - The creator/editor of the content
    - @mentioned in the content (Notion uses @user format)
    - Named in the content body (name or email appears in text)
    """
    records = []
    ds_id = str(data_subject_id)
    name_lower = data_subject_name.lower() if data_subject_name else None
    email_lower = data_subject_email.lower() if data_subject_email else None

    def is_mentioned_in(text: str) -> bool:
        """Check if data subject is mentioned in text."""
        if not text:
            return False
        text_lower = text.lower()
        if name_lower and name_lower in text_lower:
            return True
        if email_lower and email_lower in text_lower:
            return True
        return False

    def get_relationship(roles: list, text: str) -> str:
        """Determine data subject's relationship to the content."""
        relationships = list(roles)
        if text:
            text_lower = text.lower()
            if name_lower and name_lower in text_lower and not roles:
                relationships.append('named')
            if email_lower and email_lower in text_lower:
                relationships.append('email referenced')
        return ', '.join(relationships) if relationships else 'referenced'

    # Extract pages created/edited by user or mentioning user
    for page in data.get('pages', []):
        created_by = page.get('created_by', {})
        last_edited_by = page.get('last_edited_by', {})

        created_by_id = created_by.get('id') if isinstance(created_by, dict) else None
        edited_by_id = last_edited_by.get('id') if isinstance(last_edited_by, dict) else None

        title = page.get('title', '')
        if isinstance(title, list):
            # Rich text format
            title = ''.join([t.get('plain_text', '') for t in title])
        elif isinstance(title, dict):
            title = title.get('title', [{}])
            if isinstance(title, list):
                title = ''.join([t.get('plain_text', '') for t in title])

        is_creator = str(created_by_id) == ds_id
        is_editor = str(edited_by_id) == ds_id
        is_mentioned = is_mentioned_in(title)

        if is_creator or is_editor or is_mentioned or page.get('type') == 'file':
            role = []
            if is_creator:
                role.append('creator')
            if is_editor:
                role.append('editor')

            page_type = page.get('object', page.get('type', 'page'))

            records.append({
                'date': format_date(page.get('created_time') or page.get('last_edited_time')),
                'type': page_type,
                'category': 'Pages',
                'content': f"Title: {title or page.get('file_path', 'Untitled')}\nRole: {', '.join(role) if role else 'mentioned'}\nParent: {page.get('parent', {}).get('type', 'workspace')}",
                'data_subject_relationship': get_relationship(role, title),
            })

    # Extract comments by or mentioning user
    for comment in data.get('comments', []):
        created_by = comment.get('created_by', {})
        rich_text = comment.get('rich_text', [])
        text = ''.join([t.get('plain_text', '') for t in rich_text]) if isinstance(rich_text, list) else str(rich_text)

        is_author = str(created_by.get('id', '')) == ds_id
        is_mentioned = is_mentioned_in(text)

        if is_author or is_mentioned:
            records.append({
                'date': format_date(comment.get('created_time')),
                'type': 'comment',
                'category': 'Comments',
                'content': text,
                'data_subject_relationship': get_relationship(['author'] if is_author else [], text),
            })

    # Extract database entries
    for db in data.get('databases', []):
        created_by = db.get('created_by', {})
        title = db.get('title', [])
        if isinstance(title, list):
            title = ''.join([t.get('plain_text', '') for t in title])

        is_creator = str(created_by.get('id', '')) == ds_id
        is_mentioned = is_mentioned_in(title)

        if is_creator or is_mentioned:
            records.append({
                'date': format_date(db.get('created_time')),
                'type': 'database',
                'category': 'Databases',
                'content': f"Database: {title or 'Untitled'}",
                'data_subject_relationship': get_relationship(['creator'] if is_creator else [], title),
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
    """Process a Notion export for DSAR response."""
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
        print(f"Loading Notion export from {export_path}...")
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
        records = extract_records(data, ds_id, data_subject_name, data_subject_email)
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
