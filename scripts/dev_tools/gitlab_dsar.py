#!/usr/bin/env python3
"""
GitLab DSAR Processor

Export source: GitLab Profile > Account > Export Data
               OR GitLab Admin > Users > Export
Format: JSON/tar.gz with user data, projects, issues, merge requests, and comments

Usage:
    python gitlab_dsar.py export.json "John Smith" --email john@company.com
    python gitlab_dsar.py export.tar.gz "John Smith" --email john@company.com
"""

import sys
import os
import tarfile
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

VENDOR_NAME = "GitLab"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load GitLab export from tar.gz or JSON."""
    if export_path.endswith('.tar.gz') or export_path.endswith('.tgz'):
        return load_tar_export(export_path)
    else:
        return load_json(export_path)


def load_tar_export(tar_path: str) -> Dict[str, Any]:
    """Load and parse GitLab tar.gz export."""
    data = {
        'user': {},
        'users': [],
        'projects': [],
        'issues': [],
        'merge_requests': [],
        'comments': [],
        'snippets': [],
        'events': [],
    }

    with tarfile.open(tar_path, 'r:gz') as tf:
        for member in tf.getmembers():
            if not member.name.endswith('.json'):
                continue

            try:
                f = tf.extractfile(member)
                if f:
                    content = json.loads(f.read().decode('utf-8'))
                    basename = os.path.basename(member.name).lower()

                    if 'user' in basename:
                        if isinstance(content, dict):
                            data['user'] = content
                            data['users'].append(content)
                        elif isinstance(content, list):
                            data['users'].extend(content)

                    elif 'project' in basename:
                        if isinstance(content, list):
                            data['projects'].extend(content)
                        elif isinstance(content, dict):
                            data['projects'].append(content)

                    elif 'issue' in basename:
                        if isinstance(content, list):
                            data['issues'].extend(content)

                    elif 'merge' in basename or 'mr' in basename:
                        if isinstance(content, list):
                            data['merge_requests'].extend(content)

                    elif 'comment' in basename or 'note' in basename:
                        if isinstance(content, list):
                            data['comments'].extend(content)

                    elif 'snippet' in basename:
                        if isinstance(content, list):
                            data['snippets'].extend(content)

                    elif 'event' in basename or 'activity' in basename:
                        if isinstance(content, list):
                            data['events'].extend(content)

            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in GitLab users."""
    # Check main user first
    user = data.get('user', {})
    if user:
        user_email = (user.get('email') or user.get('public_email') or '').lower()
        user_name = (user.get('name') or user.get('username') or '').lower()
        name_lower = name.lower()

        if (email and user_email == email.lower()) or name_lower in user_name or user_name in name_lower:
            return {
                'id': user.get('id'),
                'name': user.get('name') or user.get('username'),
                'email': user.get('email') or user.get('public_email'),
                'username': user.get('username'),
                'raw': user,
            }

    # Check users list
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or user.get('public_email') or '').lower()
        user_name = (user.get('name') or user.get('username') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': user.get('name') or user.get('username'),
                'email': user.get('email') or user.get('public_email'),
                'username': user.get('username'),
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
                'name': user.get('name') or user.get('username'),
                'email': user.get('email') or user.get('public_email'),
            }
            if user.get('username'):
                users[user['username']] = users[user_id]

    # Extract from issues/MRs
    for issue in data.get('issues', []):
        author = issue.get('author', {})
        if author.get('id'):
            users[str(author['id'])] = {
                'name': author.get('name') or author.get('username'),
                'email': author.get('email'),
            }

    for mr in data.get('merge_requests', []):
        author = mr.get('author', {})
        if author.get('id'):
            users[str(author['id'])] = {
                'name': author.get('name') or author.get('username'),
                'email': author.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    return {
        'User ID': raw.get('id'),
        'Username': raw.get('username'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Public Email': raw.get('public_email'),
        'Commit Email': raw.get('commit_email'),
        'State': raw.get('state'),
        'Admin': raw.get('is_admin'),
        'External': raw.get('external'),
        'Bio': raw.get('bio'),
        'Location': raw.get('location'),
        'Skype': raw.get('skype'),
        'LinkedIn': raw.get('linkedin'),
        'Twitter': raw.get('twitter'),
        'Website URL': raw.get('website_url'),
        'Organization': raw.get('organization'),
        'Job Title': raw.get('job_title'),
        'Work Information': raw.get('work_information'),
        'Avatar URL': raw.get('avatar_url'),
        'Web URL': raw.get('web_url'),
        'Created At': format_date(raw.get('created_at')),
        'Last Sign In': format_date(raw.get('last_sign_in_at')),
        'Current Sign In': format_date(raw.get('current_sign_in_at')),
        'Last Activity': format_date(raw.get('last_activity_on')),
        'Two Factor Enabled': raw.get('two_factor_enabled'),
        'Projects Limit': raw.get('projects_limit'),
        'Can Create Group': raw.get('can_create_group'),
        'Can Create Project': raw.get('can_create_project'),
        'Theme ID': raw.get('theme_id'),
        'Color Scheme': raw.get('color_scheme_id'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject: Dict
) -> List[Dict]:
    """Extract all projects, issues, MRs, and comments for the data subject."""
    records = []
    ds_id = str(data_subject.get('id', ''))
    ds_username = (data_subject.get('username') or '').lower()

    def is_user_match(user_obj: Dict) -> bool:
        if not user_obj:
            return False
        user_id = str(user_obj.get('id', ''))
        username = (user_obj.get('username') or '').lower()
        return user_id == ds_id or username == ds_username

    # Projects
    for project in data.get('projects', []):
        creator = project.get('creator', project.get('owner', {}))
        if is_user_match(creator):
            records.append({
                'date': format_date(project.get('created_at')),
                'type': 'project',
                'category': 'Projects',
                'content': f"Project: {project.get('name') or project.get('path')}\nPath: {project.get('path_with_namespace', project.get('path'))}\nVisibility: {project.get('visibility', 'private')}\nDescription: {project.get('description', 'N/A')[:300]}",
            })

    # Issues
    for issue in data.get('issues', []):
        author = issue.get('author', {})
        assignees = issue.get('assignees', [])
        assignee_ids = [str(a.get('id', '')) for a in assignees]

        if is_user_match(author) or ds_id in assignee_ids:
            project_name = issue.get('project', {}).get('name', issue.get('project_id', 'Unknown'))

            role = []
            if is_user_match(author):
                role.append('author')
            if ds_id in assignee_ids:
                role.append('assignee')

            records.append({
                'date': format_date(issue.get('created_at')),
                'type': 'issue',
                'category': f"Issues / {project_name}",
                'content': f"Issue #{issue.get('iid', issue.get('id'))}: {issue.get('title')}\nRole: {', '.join(role)}\nState: {issue.get('state')}\nLabels: {', '.join(issue.get('labels', []))}\nDescription: {strip_html(issue.get('description', '') or '')[:400]}",
            })

    # Merge Requests
    for mr in data.get('merge_requests', []):
        author = mr.get('author', {})
        assignees = mr.get('assignees', [])
        assignee_ids = [str(a.get('id', '')) for a in assignees]

        if is_user_match(author) or ds_id in assignee_ids:
            project_name = mr.get('project', {}).get('name', mr.get('project_id', 'Unknown'))

            role = []
            if is_user_match(author):
                role.append('author')
            if ds_id in assignee_ids:
                role.append('assignee')

            records.append({
                'date': format_date(mr.get('created_at')),
                'type': 'merge_request',
                'category': f"Merge Requests / {project_name}",
                'content': f"MR !{mr.get('iid', mr.get('id'))}: {mr.get('title')}\nRole: {', '.join(role)}\nState: {mr.get('state')}\nSource: {mr.get('source_branch')} → {mr.get('target_branch')}\nDescription: {strip_html(mr.get('description', '') or '')[:400]}",
            })

    # Comments/Notes
    for comment in data.get('comments', data.get('notes', [])):
        author = comment.get('author', {})
        if is_user_match(author):
            records.append({
                'date': format_date(comment.get('created_at')),
                'type': 'comment',
                'category': f"Comments / {comment.get('noteable_type', 'Item')}",
                'content': strip_html(comment.get('body', '')),
            })

    # Snippets
    for snippet in data.get('snippets', []):
        author = snippet.get('author', {})
        if is_user_match(author):
            records.append({
                'date': format_date(snippet.get('created_at')),
                'type': 'snippet',
                'category': 'Snippets',
                'content': f"Snippet: {snippet.get('title')}\nVisibility: {snippet.get('visibility', 'private')}\nFilename: {snippet.get('file_name', 'N/A')}",
            })

    # Events/Activity
    for event in data.get('events', []):
        author = event.get('author', {})
        if is_user_match(author) or str(event.get('author_id', '')) == ds_id:
            records.append({
                'date': format_date(event.get('created_at')),
                'type': event.get('action_name', 'event'),
                'category': 'Activity',
                'content': f"Action: {event.get('action_name', 'unknown')}\nTarget: {event.get('target_type', 'N/A')} - {event.get('target_title', 'N/A')}",
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
    """Process a GitLab export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading GitLab export from {export_path}...")
    data = load_export(export_path)

    print(f"Searching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(data, data_subject_name, data_subject_email)
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
    records = extract_records(data, data_subject)
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
