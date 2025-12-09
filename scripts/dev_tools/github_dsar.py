#!/usr/bin/env python3
"""
GitHub DSAR Processor

Export source: GitHub Settings > Account > Export account data
               OR GitHub Enterprise Admin > Site admin > Data portability
Format: ZIP/tar.gz containing JSON files with user data, repos, issues, PRs, comments

Usage:
    python github_dsar.py export.tar.gz "John Smith" --email john@company.com
    python github_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
import zipfile
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

VENDOR_NAME = "GitHub"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load GitHub export from ZIP, tar.gz, or JSON."""
    if export_path.endswith('.zip'):
        return load_zip_export(export_path)
    elif export_path.endswith('.tar.gz') or export_path.endswith('.tgz'):
        return load_tar_export(export_path)
    else:
        return load_json(export_path)


def load_archive_json(archive, name: str, is_tar: bool = False) -> Any:
    """Load JSON from archive file."""
    try:
        if is_tar:
            member = archive.getmember(name)
            f = archive.extractfile(member)
            if f:
                return json.loads(f.read().decode('utf-8'))
        else:
            return json.loads(archive.read(name).decode('utf-8'))
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def load_zip_export(zip_path: str) -> Dict[str, Any]:
    """Load and parse GitHub ZIP export."""
    data = {
        'user': {},
        'users': [],
        'repositories': [],
        'issues': [],
        'pull_requests': [],
        'comments': [],
        'commits': [],
    }

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if not name.endswith('.json'):
                continue

            content = load_archive_json(zf, name, is_tar=False)
            if not content:
                continue

            basename = os.path.basename(name).lower()

            if 'user' in basename and isinstance(content, dict):
                data['user'] = content
                data['users'].append(content)
            elif 'repositories' in basename or 'repos' in basename:
                if isinstance(content, list):
                    data['repositories'].extend(content)
                elif isinstance(content, dict):
                    data['repositories'].append(content)
            elif 'issues' in basename:
                if isinstance(content, list):
                    data['issues'].extend(content)
            elif 'pull' in basename:
                if isinstance(content, list):
                    data['pull_requests'].extend(content)
            elif 'comment' in basename:
                if isinstance(content, list):
                    data['comments'].extend(content)
            elif 'commit' in basename:
                if isinstance(content, list):
                    data['commits'].extend(content)

    return data


def load_tar_export(tar_path: str) -> Dict[str, Any]:
    """Load and parse GitHub tar.gz export."""
    data = {
        'user': {},
        'users': [],
        'repositories': [],
        'issues': [],
        'pull_requests': [],
        'comments': [],
        'commits': [],
    }

    with tarfile.open(tar_path, 'r:gz') as tf:
        for member in tf.getmembers():
            if not member.name.endswith('.json'):
                continue

            content = load_archive_json(tf, member.name, is_tar=True)
            if not content:
                continue

            basename = os.path.basename(member.name).lower()

            if 'user' in basename and isinstance(content, dict):
                data['user'] = content
                data['users'].append(content)
            elif 'repositories' in basename or 'repos' in basename:
                if isinstance(content, list):
                    data['repositories'].extend(content)
            elif 'issues' in basename:
                if isinstance(content, list):
                    data['issues'].extend(content)
            elif 'pull' in basename:
                if isinstance(content, list):
                    data['pull_requests'].extend(content)
            elif 'comment' in basename:
                if isinstance(content, list):
                    data['comments'].extend(content)
            elif 'commit' in basename:
                if isinstance(content, list):
                    data['commits'].extend(content)

    return data


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in GitHub users."""
    users = data.get('users', [])
    if data.get('user'):
        users = [data['user']] + users

    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('email') or '').lower()
        user_name = (user.get('name') or user.get('login') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in user_name or user_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id') or user.get('login'),
                'name': user.get('name') or user.get('login'),
                'email': user.get('email'),
                'login': user.get('login'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # Main users
    for user in data.get('users', []):
        user_id = str(user.get('id') or user.get('login', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('name') or user.get('login'),
                'email': user.get('email'),
            }
        if user.get('login'):
            users[user.get('login')] = {
                'name': user.get('name') or user.get('login'),
                'email': user.get('email'),
            }

    # Extract from issues/PRs/comments
    for issue in data.get('issues', []) + data.get('pull_requests', []):
        author = issue.get('user', {})
        if author.get('login'):
            users[author['login']] = {'name': author.get('login'), 'email': None}

    for comment in data.get('comments', []):
        author = comment.get('user', {})
        if author.get('login'):
            users[author['login']] = {'name': author.get('login'), 'email': None}

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    return {
        'User ID': raw.get('id'),
        'Login': raw.get('login'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Company': raw.get('company'),
        'Location': raw.get('location'),
        'Bio': raw.get('bio'),
        'Blog': raw.get('blog'),
        'Twitter': raw.get('twitter_username'),
        'Public Repos': raw.get('public_repos'),
        'Public Gists': raw.get('public_gists'),
        'Followers': raw.get('followers'),
        'Following': raw.get('following'),
        'Created At': format_date(raw.get('created_at')),
        'Updated At': format_date(raw.get('updated_at')),
        'Two Factor': raw.get('two_factor_authentication'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject: Dict
) -> List[Dict]:
    """Extract all repositories, issues, PRs, and comments for the data subject."""
    records = []
    ds_login = data_subject.get('login', '').lower()
    ds_id = str(data_subject.get('id', ''))
    ds_email = (data_subject.get('email') or '').lower()

    def is_user_match(user_obj: Dict) -> bool:
        if not user_obj:
            return False
        login = (user_obj.get('login') or '').lower()
        user_id = str(user_obj.get('id', ''))
        return login == ds_login or user_id == ds_id

    # Repositories
    for repo in data.get('repositories', []):
        owner = repo.get('owner', {})
        if is_user_match(owner) or (repo.get('owner') == ds_login):
            records.append({
                'date': format_date(repo.get('created_at')),
                'type': 'repository',
                'category': 'Repositories',
                'content': f"Repository: {repo.get('full_name') or repo.get('name')}\nDescription: {repo.get('description', '')}\nVisibility: {'Private' if repo.get('private') else 'Public'}\nLanguage: {repo.get('language', 'N/A')}",
            })

    # Issues
    for issue in data.get('issues', []):
        user = issue.get('user', {})
        if is_user_match(user):
            repo_name = issue.get('repository_url', '').split('/')[-1] if issue.get('repository_url') else 'Unknown'
            records.append({
                'date': format_date(issue.get('created_at')),
                'type': 'issue',
                'category': f"Issues / {repo_name}",
                'content': f"Issue #{issue.get('number')}: {issue.get('title')}\nState: {issue.get('state')}\nBody: {strip_html(issue.get('body', '') or '')}",
            })

    # Pull Requests
    for pr in data.get('pull_requests', []):
        user = pr.get('user', {})
        if is_user_match(user):
            repo_name = pr.get('base', {}).get('repo', {}).get('name', 'Unknown')
            records.append({
                'date': format_date(pr.get('created_at')),
                'type': 'pull_request',
                'category': f"Pull Requests / {repo_name}",
                'content': f"PR #{pr.get('number')}: {pr.get('title')}\nState: {pr.get('state')}\nBody: {strip_html(pr.get('body', '') or '')}",
            })

    # Comments
    for comment in data.get('comments', []):
        user = comment.get('user', {})
        if is_user_match(user):
            records.append({
                'date': format_date(comment.get('created_at')),
                'type': 'comment',
                'category': 'Comments',
                'content': strip_html(comment.get('body', '')),
            })

    # Commits
    for commit in data.get('commits', []):
        author = commit.get('author', {}) or commit.get('commit', {}).get('author', {})
        author_email = (author.get('email') or '').lower()
        author_login = (author.get('login') or '').lower()

        if author_login == ds_login or author_email == ds_email:
            message = commit.get('commit', {}).get('message', commit.get('message', ''))
            records.append({
                'date': format_date(commit.get('commit', {}).get('author', {}).get('date') or commit.get('date')),
                'type': 'commit',
                'category': 'Commits',
                'content': f"SHA: {commit.get('sha', '')[:7]}\nMessage: {message}",
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
    """Process a GitHub export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading GitHub export from {export_path}...")
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
