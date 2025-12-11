#!/usr/bin/env python3
"""
Greenhouse DSAR Processor

Export source: Greenhouse Settings > Data Privacy > Export Candidate Data
               OR Greenhouse Harvest API export
Format: JSON with candidates, applications, interviews, and scorecards

Usage:
    python greenhouse_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Greenhouse"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Greenhouse candidates."""
    candidates = data.get('candidates', data.get('data', []))
    matches = []
    name_lower = name.lower()

    for candidate in candidates:
        emails = candidate.get('email_addresses', candidate.get('emails', []))
        candidate_emails = []
        for e in emails:
            if isinstance(e, dict):
                candidate_emails.append((e.get('value') or e.get('email') or '').lower())
            elif isinstance(e, str):
                candidate_emails.append(e.lower())

        first_name = (candidate.get('first_name') or '').lower()
        last_name = (candidate.get('last_name') or '').lower()
        full_name = f"{first_name} {last_name}".strip()

        is_match = False
        if email and email.lower() in candidate_emails:
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True

        if is_match:
            primary_email = candidate_emails[0] if candidate_emails else None
            matches.append({
                'id': candidate.get('id'),
                'name': f"{candidate.get('first_name', '')} {candidate.get('last_name', '')}".strip(),
                'email': primary_email,
                'raw': candidate,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    # Candidates
    for candidate in data.get('candidates', data.get('data', [])):
        cand_id = str(candidate.get('id', ''))
        if cand_id:
            users[cand_id] = {
                'name': f"{candidate.get('first_name', '')} {candidate.get('last_name', '')}".strip(),
                'email': None,
            }
            emails = candidate.get('email_addresses', candidate.get('emails', []))
            if emails:
                first_email = emails[0]
                if isinstance(first_email, dict):
                    users[cand_id]['email'] = first_email.get('value') or first_email.get('email')
                elif isinstance(first_email, str):
                    users[cand_id]['email'] = first_email

    # Recruiters/coordinators
    for user in data.get('users', data.get('recruiters', [])):
        user_id = str(user.get('id', ''))
        if user_id:
            users[user_id] = {
                'name': user.get('name') or f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                'email': user.get('email'),
            }

    # Extract from applications
    for app in data.get('applications', []):
        recruiter = app.get('recruiter', {})
        if recruiter and recruiter.get('id'):
            users[str(recruiter['id'])] = {
                'name': recruiter.get('name'),
                'email': recruiter.get('email'),
            }
        coordinator = app.get('coordinator', {})
        if coordinator and coordinator.get('id'):
            users[str(coordinator['id'])] = {
                'name': coordinator.get('name'),
                'email': coordinator.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    # Extract emails
    emails = raw.get('email_addresses', raw.get('emails', []))
    email_list = []
    for e in emails:
        if isinstance(e, dict):
            email_list.append(f"{e.get('value', '')} ({e.get('type', 'other')})")
        elif isinstance(e, str):
            email_list.append(e)

    # Extract phones
    phones = raw.get('phone_numbers', raw.get('phones', []))
    phone_list = []
    for p in phones:
        if isinstance(p, dict):
            phone_list.append(f"{p.get('value', '')} ({p.get('type', 'other')})")
        elif isinstance(p, str):
            phone_list.append(p)

    # Extract addresses
    addresses = raw.get('addresses', [])
    address_list = []
    for a in addresses:
        if isinstance(a, dict):
            address_list.append(a.get('value', ''))
        elif isinstance(a, str):
            address_list.append(a)

    # Extract social media
    social = raw.get('social_media_addresses', raw.get('social_media', []))
    social_list = []
    for s in social:
        if isinstance(s, dict):
            social_list.append(f"{s.get('type', 'other')}: {s.get('value', '')}")

    # Extract tags
    tags = raw.get('tags', [])
    tag_names = ', '.join(tags) if isinstance(tags, list) and tags else 'N/A'

    return {
        'Candidate ID': raw.get('id'),
        'First Name': raw.get('first_name'),
        'Last Name': raw.get('last_name'),
        'Company': raw.get('company'),
        'Title': raw.get('title'),
        'Emails': ', '.join(email_list) if email_list else 'N/A',
        'Phone Numbers': ', '.join(phone_list) if phone_list else 'N/A',
        'Addresses': ', '.join(address_list) if address_list else 'N/A',
        'Social Media': ', '.join(social_list) if social_list else 'N/A',
        'Website': ', '.join(raw.get('website_addresses', [])) if raw.get('website_addresses') else 'N/A',
        'Tags': tag_names,
        'Source': raw.get('source', {}).get('public_name') if isinstance(raw.get('source'), dict) else raw.get('source'),
        'Created At': format_date(raw.get('created_at')),
        'Updated At': format_date(raw.get('updated_at')),
        'Last Activity': format_date(raw.get('last_activity')),
        'Is Private': raw.get('is_private'),
        'Photo URL': raw.get('photo_url'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all applications, interviews, and scorecards for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Build job lookup
    jobs = {}
    for job in data.get('jobs', []):
        jobs[str(job.get('id', ''))] = job.get('name', 'Unknown Job')

    # Applications
    for app in data.get('applications', []):
        if str(app.get('candidate_id', '')) == ds_id or str(app.get('candidate', {}).get('id', '')) == ds_id:
            job_name = jobs.get(str(app.get('job_id', '')), app.get('job', {}).get('name', 'Unknown'))

            records.append({
                'date': format_date(app.get('applied_at') or app.get('created_at')),
                'type': 'application',
                'category': f"Applications / {job_name}",
                'content': f"Job: {job_name}\nStatus: {app.get('status')}\nStage: {app.get('current_stage', {}).get('name', 'Unknown') if isinstance(app.get('current_stage'), dict) else app.get('current_stage', 'Unknown')}\nSource: {app.get('source', {}).get('public_name', 'Unknown') if isinstance(app.get('source'), dict) else app.get('source', 'Unknown')}\nRejected: {app.get('rejected_at') or 'No'}",
            })

    # Interviews/scheduled interviews
    for interview in data.get('scheduled_interviews', data.get('interviews', [])):
        candidate_id = str(interview.get('candidate_id', ''))
        app = interview.get('application', {})
        app_candidate_id = str(app.get('candidate_id', app.get('candidate', {}).get('id', '')))

        if candidate_id == ds_id or app_candidate_id == ds_id:
            interviewers = interview.get('interviewers', [])
            interviewer_names = ', '.join([i.get('name', '') for i in interviewers]) if interviewers else 'N/A'

            records.append({
                'date': format_date(interview.get('start', {}).get('date_time') if isinstance(interview.get('start'), dict) else interview.get('scheduled_at')),
                'type': 'interview',
                'category': 'Interviews',
                'content': f"Interview: {interview.get('name', 'Interview')}\nStatus: {interview.get('status', 'scheduled')}\nInterviewers: {interviewer_names}\nLocation: {interview.get('location', 'N/A')}",
            })

    # Scorecards
    for scorecard in data.get('scorecards', []):
        candidate_id = str(scorecard.get('candidate_id', ''))
        app = scorecard.get('application', {})
        app_candidate_id = str(app.get('candidate_id', app.get('candidate', {}).get('id', '')))

        if candidate_id == ds_id or app_candidate_id == ds_id:
            attributes = scorecard.get('attributes', [])
            attr_str = '\n'.join([f"  - {a.get('name', '')}: {a.get('rating', '')} ({a.get('type', '')})" for a in attributes]) if attributes else 'N/A'

            records.append({
                'date': format_date(scorecard.get('submitted_at') or scorecard.get('created_at')),
                'type': 'scorecard',
                'category': 'Scorecards',
                'content': f"Recommendation: {scorecard.get('overall_recommendation', 'N/A')}\nSubmitted By: {scorecard.get('submitted_by', {}).get('name', 'Unknown') if isinstance(scorecard.get('submitted_by'), dict) else 'Unknown'}\nAttributes:\n{attr_str}\nNotes: {strip_html(scorecard.get('interview_notes', '') or '')}",
            })

    # Activity feed/notes
    for activity in data.get('activity_feed', data.get('activities', [])):
        if str(activity.get('candidate_id', '')) == ds_id:
            records.append({
                'date': format_date(activity.get('created_at')),
                'type': activity.get('type', 'activity'),
                'category': 'Activity',
                'content': strip_html(activity.get('body', activity.get('note', ''))),
            })

    # Notes
    for note in data.get('notes', []):
        if str(note.get('candidate_id', '')) == ds_id:
            records.append({
                'date': format_date(note.get('created_at')),
                'type': 'note',
                'category': 'Notes',
                'content': f"Author: {note.get('user', {}).get('name', 'Unknown') if isinstance(note.get('user'), dict) else 'Unknown'}\nNote: {strip_html(note.get('body', ''))}",
            })

    # Attachments/resumes
    candidate_raw = data_subject.get('raw', {})
    attachments = candidate_raw.get('attachments', [])
    for attachment in attachments:
        records.append({
            'date': format_date(attachment.get('created_at')),
            'type': attachment.get('type', 'attachment'),
            'category': 'Attachments',
            'content': f"File: {attachment.get('filename')}\nType: {attachment.get('type', 'document')}",
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
    """Process a Greenhouse export for DSAR response."""
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
        print(f"Loading Greenhouse export from {export_path}...")
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
