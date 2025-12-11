#!/usr/bin/env python3
"""
Confluence DSAR Processor

Export source: Confluence Admin > Content Tools > Export > Export Space
               OR Atlassian Cloud Admin > Data management > Data exports
Format: JSON/XML with users, spaces, pages, comments, and attachments

Usage:
    python confluence_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Confluence"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Confluence users."""
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or user.get('emailAddress') or '').lower()
        user_name = (user.get('displayName') or user.get('publicName') or user.get('name') or '').lower()
        account_id = user.get('accountId') or user.get('key') or user.get('username')

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': account_id,
                'name': user.get('displayName') or user.get('publicName') or user.get('name'),
                'email': user.get('email') or user.get('emailAddress'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    for user in data.get('users', []):
        user_id = str(user.get('accountId') or user.get('key') or user.get('username', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('displayName') or user.get('publicName') or user.get('name'),
                'email': user.get('email') or user.get('emailAddress'),
            }

    # Extract from pages
    for page in data.get('pages', data.get('content', [])):
        author = page.get('history', {}).get('createdBy', page.get('creator', {}))
        if author:
            author_id = str(author.get('accountId') or author.get('key', ''))
            if author_id and author_id not in users:
                users[author_id] = {
                    'name': author.get('displayName') or author.get('publicName'),
                    'email': author.get('email'),
                }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    operations = raw.get('operations', [])
    operation_names = ', '.join([op.get('operation', '') for op in operations[:5]]) if operations else 'N/A'

    return {
        'Account ID': raw.get('accountId') or raw.get('key'),
        'Username': raw.get('username'),
        'Display Name': raw.get('displayName') or raw.get('publicName'),
        'Email': raw.get('email') or raw.get('emailAddress'),
        'Account Type': raw.get('accountType') or raw.get('type'),
        'Active': raw.get('isActive', raw.get('active')),
        'Profile Picture': raw.get('profilePicture', {}).get('path') if isinstance(raw.get('profilePicture'), dict) else raw.get('profilePicture'),
        'Timezone': raw.get('timeZone'),
        'Locale': raw.get('locale'),
        'Operations': operation_names,
        'Personal Space': raw.get('personalSpace', {}).get('name') if isinstance(raw.get('personalSpace'), dict) else 'N/A',
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_name: str = None,
    data_subject_email: str = None
) -> List[Dict]:
    """
    Extract all pages, comments, and activity for the data subject.

    GDPR Compliance: Includes content where the data subject is:
    - The creator/editor/author of the content
    - @mentioned in the content (Confluence uses @user format)
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

    # Build space lookup
    spaces = {}
    for space in data.get('spaces', []):
        space_key = space.get('key', '')
        spaces[space_key] = space.get('name', space_key)

    # Pages
    for page in data.get('pages', data.get('content', [])):
        history = page.get('history', {})
        created_by = history.get('createdBy', page.get('creator', {}))
        last_updated_by = history.get('lastUpdated', {}).get('by', {})

        created_by_id = str(created_by.get('accountId') or created_by.get('key', ''))
        updated_by_id = str(last_updated_by.get('accountId') or last_updated_by.get('key', ''))

        body = page.get('body', {})
        content = ''
        if isinstance(body, dict):
            storage = body.get('storage', body.get('view', {}))
            content = strip_html(storage.get('value', '') if isinstance(storage, dict) else str(storage))[:500]

        title = page.get('title', '')
        is_creator = created_by_id == ds_id
        is_editor = updated_by_id == ds_id
        is_mentioned = is_mentioned_in(content) or is_mentioned_in(title)

        if is_creator or is_editor or is_mentioned:
            space_key = page.get('space', {}).get('key', page.get('spaceKey', ''))
            space_name = spaces.get(space_key, space_key)

            role = []
            if is_creator:
                role.append('creator')
            if is_editor:
                role.append('editor')

            records.append({
                'date': format_date(history.get('createdDate') or page.get('created')),
                'type': page.get('type', 'page'),
                'category': f"Pages / {space_name}",
                'content': f"Title: {title}\nRole: {', '.join(role) if role else 'mentioned'}\nStatus: {page.get('status', 'current')}\nVersion: {page.get('version', {}).get('number', 1) if isinstance(page.get('version'), dict) else 'N/A'}\nContent Preview: {content}",
                'data_subject_relationship': get_relationship(role, content + ' ' + title),
            })

    # Comments
    for comment in data.get('comments', []):
        author = comment.get('history', {}).get('createdBy', comment.get('author', {}))
        author_id = str(author.get('accountId') or author.get('key', ''))

        body = comment.get('body', {})
        content = ''
        if isinstance(body, dict):
            storage = body.get('storage', body.get('view', {}))
            content = strip_html(storage.get('value', '') if isinstance(storage, dict) else str(storage))

        is_author = author_id == ds_id
        is_mentioned = is_mentioned_in(content)

        if is_author or is_mentioned:
            container = comment.get('container', {})
            container_title = container.get('title', 'Unknown') if isinstance(container, dict) else 'Unknown'

            records.append({
                'date': format_date(comment.get('history', {}).get('createdDate') or comment.get('created')),
                'type': 'comment',
                'category': f"Comments / {container_title}",
                'content': content,
                'data_subject_relationship': get_relationship(['author'] if is_author else [], content),
            })

    # Blog posts
    for blog in data.get('blogposts', data.get('blogs', [])):
        history = blog.get('history', {})
        created_by = history.get('createdBy', blog.get('creator', {}))
        created_by_id = str(created_by.get('accountId') or created_by.get('key', ''))

        body = blog.get('body', {})
        content = ''
        if isinstance(body, dict):
            storage = body.get('storage', body.get('view', {}))
            content = strip_html(storage.get('value', '') if isinstance(storage, dict) else str(storage))[:500]

        title = blog.get('title', '')
        is_creator = created_by_id == ds_id
        is_mentioned = is_mentioned_in(content) or is_mentioned_in(title)

        if is_creator or is_mentioned:
            space_key = blog.get('space', {}).get('key', blog.get('spaceKey', ''))
            space_name = spaces.get(space_key, space_key)

            records.append({
                'date': format_date(history.get('createdDate') or blog.get('created')),
                'type': 'blogpost',
                'category': f"Blog Posts / {space_name}",
                'content': f"Title: {title}\nContent Preview: {content}",
                'data_subject_relationship': get_relationship(['creator'] if is_creator else [], content + ' ' + title),
            })

    # Attachments
    for attachment in data.get('attachments', []):
        history = attachment.get('history', {})
        created_by = history.get('createdBy', attachment.get('creator', {}))
        created_by_id = str(created_by.get('accountId') or created_by.get('key', ''))

        if created_by_id == ds_id:
            container = attachment.get('container', {})
            container_title = container.get('title', 'Unknown') if isinstance(container, dict) else 'Unknown'

            records.append({
                'date': format_date(history.get('createdDate') or attachment.get('created')),
                'type': 'attachment',
                'category': f"Attachments / {container_title}",
                'content': f"File: {attachment.get('title')}\nMedia Type: {attachment.get('mediaType', 'N/A')}\nSize: {attachment.get('extensions', {}).get('fileSize', 'N/A') if isinstance(attachment.get('extensions'), dict) else 'N/A'} bytes",
                'data_subject_relationship': 'creator',
            })

    # Labels (if assigned by user)
    for label in data.get('labels', []):
        owner = label.get('owner', {})
        if str(owner.get('accountId', '')) == ds_id:
            records.append({
                'date': format_date(label.get('created')),
                'type': 'label',
                'category': 'Labels',
                'content': f"Label: {label.get('name') or label.get('label')}\nPrefix: {label.get('prefix', 'global')}",
                'data_subject_relationship': 'owner',
            })

    # Watches/subscriptions
    for watch in data.get('watches', data.get('subscriptions', [])):
        if str(watch.get('accountId', '') or watch.get('userId', '')) == ds_id:
            content = watch.get('content', watch.get('target', {}))
            records.append({
                'date': format_date(watch.get('created')),
                'type': 'watch',
                'category': 'Watches',
                'content': f"Watching: {content.get('title', 'Unknown') if isinstance(content, dict) else 'Content'}",
                'data_subject_relationship': 'subscriber',
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
    """Process a Confluence export for DSAR response."""
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
        print(f"Loading Confluence export from {export_path}...")
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
