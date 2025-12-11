#!/usr/bin/env python3
"""
Jira DSAR Processor

Export source: Jira Admin > System > Backup Manager > Create Backup
               OR Jira Cloud > Settings > System > Import and export > Backup manager
Format: JSON/XML export containing users, issues, comments, worklogs, and attachments

Usage:
    python jira_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
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

VENDOR_NAME = "Jira"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Jira users."""
    users = data.get('users', [])
    matches = []
    name_lower = name.lower()

    for user in users:
        user_email = (user.get('emailAddress') or user.get('email') or '').lower()
        display_name = (user.get('displayName') or user.get('name') or '').lower()
        account_id = user.get('accountId') or user.get('key') or user.get('name')

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in display_name or display_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': account_id,
                'name': user.get('displayName') or user.get('name'),
                'email': user.get('emailAddress') or user.get('email'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}
    for user in data.get('users', []):
        user_id = user.get('accountId') or user.get('key') or user.get('name')
        if user_id:
            users[str(user_id)] = {
                'name': user.get('displayName') or user.get('name'),
                'email': user.get('emailAddress') or user.get('email'),
            }
    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    return {
        'Account ID': data_subject.get('id'),
        'Display Name': raw.get('displayName'),
        'Email': raw.get('emailAddress') or raw.get('email'),
        'Username': raw.get('name'),
        'Active': raw.get('active'),
        'Timezone': raw.get('timeZone'),
        'Locale': raw.get('locale'),
        'Account Type': raw.get('accountType'),
        'Avatar URL': raw.get('avatarUrls', {}).get('48x48') if isinstance(raw.get('avatarUrls'), dict) else None,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_name: str = None,
    data_subject_email: str = None
) -> List[Dict]:
    """
    Extract all issues, comments, and worklogs for the data subject.

    GDPR Compliance: Includes content where the data subject is:
    - The reporter/assignee/creator of the issue
    - The author of comments or worklogs
    - @mentioned in the content (Jira uses [~accountId] or @user format)
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
        # Check for Jira @mention format [~accountId] or [~username]
        if f'[~{ds_id}]' in text or f'[~{ds_id.lower()}]' in text_lower:
            return True
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
            if f'[~{ds_id}]' in text or f'[~{ds_id.lower()}]' in text_lower:
                relationships.append('@mentioned')
            if name_lower and name_lower in text_lower and not roles:
                relationships.append('named')
            if email_lower and email_lower in text_lower:
                relationships.append('email referenced')
        return ', '.join(relationships) if relationships else 'referenced'

    # Build project lookup
    projects = {str(p.get('id')): p.get('name') or p.get('key') for p in data.get('projects', [])}

    # Track processed issues to handle comments on non-involved issues
    processed_issue_keys = set()

    # Extract issues where data subject is involved or mentioned
    for issue in data.get('issues', []):
        fields = issue.get('fields', {})
        reporter = fields.get('reporter', {})
        assignee = fields.get('assignee', {})
        creator = fields.get('creator', {})

        reporter_id = reporter.get('accountId') or reporter.get('key') or reporter.get('name') if reporter else None
        assignee_id = assignee.get('accountId') or assignee.get('key') or assignee.get('name') if assignee else None
        creator_id = creator.get('accountId') or creator.get('key') or creator.get('name') if creator else None

        description = fields.get('description', '') or ''
        summary = fields.get('summary', '') or ''

        is_reporter = str(reporter_id) == ds_id
        is_assignee = str(assignee_id) == ds_id
        is_creator = str(creator_id) == ds_id
        is_mentioned = is_mentioned_in(description) or is_mentioned_in(summary)

        issue_key = issue.get('key', '')
        project = projects.get(str(fields.get('project', {}).get('id', '')), 'Unknown')

        # Check if data subject is involved in the issue itself
        if is_reporter or is_assignee or is_creator or is_mentioned:
            processed_issue_keys.add(issue_key)

            role = []
            if is_reporter:
                role.append('reporter')
            if is_assignee:
                role.append('assignee')
            if is_creator:
                role.append('creator')

            records.append({
                'date': format_date(fields.get('created')),
                'type': 'issue',
                'category': f"{project} / {issue_key}",
                'content': f"Summary: {summary}\nRole: {', '.join(role) if role else 'mentioned'}\nStatus: {fields.get('status', {}).get('name', 'Unknown')}\nDescription: {strip_html(description)}",
                'data_subject_relationship': get_relationship(role, description + ' ' + summary),
            })

        # Extract comments - check ALL comments for mentions
        comments = fields.get('comment', {}).get('comments', [])
        for comment in comments:
            author = comment.get('author', {})
            author_id = author.get('accountId') or author.get('key') or author.get('name')
            body = comment.get('body', '') or ''

            is_author = str(author_id) == ds_id
            is_comment_mentioned = is_mentioned_in(body)

            if is_author or is_comment_mentioned:
                records.append({
                    'date': format_date(comment.get('created')),
                    'type': 'comment',
                    'category': f"{project} / {issue_key}",
                    'content': strip_html(body),
                    'data_subject_relationship': get_relationship(['author'] if is_author else [], body),
                })

        # Extract worklogs
        worklogs = fields.get('worklog', {}).get('worklogs', [])
        for worklog in worklogs:
            author = worklog.get('author', {})
            author_id = author.get('accountId') or author.get('key') or author.get('name')
            worklog_comment = worklog.get('comment', '') or ''

            is_author = str(author_id) == ds_id
            is_worklog_mentioned = is_mentioned_in(worklog_comment)

            if is_author or is_worklog_mentioned:
                records.append({
                    'date': format_date(worklog.get('started')),
                    'type': 'worklog',
                    'category': f"{project} / {issue_key}",
                    'content': f"Time logged: {worklog.get('timeSpent', 'Unknown')}\nComment: {strip_html(worklog_comment)}",
                    'data_subject_relationship': get_relationship(['author'] if is_author else [], worklog_comment),
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
    """Process a Jira export for DSAR response."""
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
        print(f"Loading Jira export from {export_path}...")
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
