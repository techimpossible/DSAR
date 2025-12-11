#!/usr/bin/env python3
"""
Asana DSAR Processor

Export source: Asana Admin Console > Settings > Export Organization Data
               OR Asana API export via personal access token
Format: JSON with users, tasks, projects, comments, and attachments

Usage:
    python asana_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Asana"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Asana users."""
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
                'id': user.get('gid') or user.get('id'),
                'name': user.get('name'),
                'email': user.get('email'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    for user in data.get('users', data.get('data', {}).get('users', [])):
        user_id = str(user.get('gid') or user.get('id', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('name'),
                'email': user.get('email'),
            }

    # Extract from tasks assignees and creators
    for task in data.get('tasks', data.get('data', {}).get('tasks', [])):
        assignee = task.get('assignee', {})
        if assignee and assignee.get('gid'):
            users[str(assignee['gid'])] = {
                'name': assignee.get('name'),
                'email': assignee.get('email'),
            }
        created_by = task.get('created_by', {})
        if created_by and created_by.get('gid'):
            users[str(created_by['gid'])] = {
                'name': created_by.get('name'),
                'email': created_by.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    workspaces = raw.get('workspaces', [])
    workspace_names = ', '.join([w.get('name', '') for w in workspaces]) if workspaces else 'N/A'

    return {
        'User ID': raw.get('gid') or raw.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Resource Type': raw.get('resource_type'),
        'Workspaces': workspace_names,
        'Photo': raw.get('photo', {}).get('image_128x128') if isinstance(raw.get('photo'), dict) else None,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all tasks, comments, and stories for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Build project lookup
    projects = {}
    for project in data.get('projects', data.get('data', {}).get('projects', [])):
        project_id = str(project.get('gid') or project.get('id', ''))
        projects[project_id] = project.get('name', 'Unknown Project')

    # Extract tasks
    for task in data.get('tasks', data.get('data', {}).get('tasks', [])):
        assignee = task.get('assignee', {})
        created_by = task.get('created_by', {})

        assignee_id = str(assignee.get('gid', '')) if assignee else ''
        creator_id = str(created_by.get('gid', '')) if created_by else ''

        is_involved = assignee_id == ds_id or creator_id == ds_id

        # Check followers
        followers = task.get('followers', [])
        follower_ids = [str(f.get('gid', '')) for f in followers]
        if ds_id in follower_ids:
            is_involved = True

        if not is_involved:
            continue

        # Get project name
        task_projects = task.get('projects', task.get('memberships', []))
        project_name = 'No Project'
        if task_projects:
            first_project = task_projects[0]
            if isinstance(first_project, dict):
                project_id = str(first_project.get('gid') or first_project.get('project', {}).get('gid', ''))
                project_name = projects.get(project_id, first_project.get('name', 'Unknown'))

        role = []
        if assignee_id == ds_id:
            role.append('assignee')
        if creator_id == ds_id:
            role.append('creator')
        if ds_id in follower_ids:
            role.append('follower')

        records.append({
            'date': format_date(task.get('created_at')),
            'type': 'task',
            'category': f"Tasks / {project_name}",
            'content': f"Task: {task.get('name')}\nRole: {', '.join(role)}\nStatus: {'Completed' if task.get('completed') else 'Open'}\nDue: {format_date(task.get('due_on')) or 'No due date'}\nNotes: {strip_html(task.get('notes', '') or '')}",
        })

    # Extract stories/comments
    for story in data.get('stories', data.get('data', {}).get('stories', [])):
        created_by = story.get('created_by', {})
        if str(created_by.get('gid', '')) == ds_id:
            records.append({
                'date': format_date(story.get('created_at')),
                'type': story.get('resource_subtype', 'comment'),
                'category': 'Activity',
                'content': strip_html(story.get('text', '')),
            })

    # Extract comments (separate from stories in some exports)
    for comment in data.get('comments', []):
        author = comment.get('author', comment.get('created_by', {}))
        if str(author.get('gid', '')) == ds_id:
            records.append({
                'date': format_date(comment.get('created_at')),
                'type': 'comment',
                'category': 'Comments',
                'content': strip_html(comment.get('text', comment.get('body', ''))),
            })

    # Extract attachments
    for attachment in data.get('attachments', data.get('data', {}).get('attachments', [])):
        created_by = attachment.get('created_by', {})
        if str(created_by.get('gid', '')) == ds_id:
            records.append({
                'date': format_date(attachment.get('created_at')),
                'type': 'attachment',
                'category': 'Attachments',
                'content': f"File: {attachment.get('name')}\nType: {attachment.get('resource_subtype', 'file')}",
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
    """Process an Asana export for DSAR response."""
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
        print(f"Loading Asana export from {export_path}...")
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
