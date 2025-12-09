#!/usr/bin/env python3
"""
Salesforce DSAR Processor

Export source: Salesforce Setup > Data > Data Export > Export Now/Schedule Export
               OR Salesforce Data Loader export
Format: CSV/JSON with Contacts, Accounts, Opportunities, Activities, and Cases

Usage:
    python salesforce_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redaction import RedactionEngine
from core.docgen import create_vendor_report
from core.utils import (
    setup_argparser, parse_extra_redactions, load_json, save_json,
    ensure_output_dir, safe_filename, format_date, get_timestamp,
    validate_data_subject_match, strip_html,
)

VENDOR_NAME = "Salesforce"


def find_data_subject(data: Dict[str, Any], name: str, email: str = None) -> Optional[Dict]:
    contacts = data.get('contacts', data.get('Contact', []))
    matches = []
    name_lower = name.lower()

    for contact in contacts:
        contact_email = (contact.get('Email') or '').lower()
        full_name = f"{contact.get('FirstName', '')} {contact.get('LastName', '')}".strip().lower()

        if (email and contact_email == email.lower()) or (name_lower in full_name or full_name in name_lower):
            matches.append({
                'id': contact.get('Id'),
                'name': f"{contact.get('FirstName', '')} {contact.get('LastName', '')}".strip(),
                'email': contact.get('Email'),
                'raw': contact,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    users = {}
    for contact in data.get('contacts', data.get('Contact', [])):
        if contact.get('Id'):
            users[contact['Id']] = {
                'name': f"{contact.get('FirstName', '')} {contact.get('LastName', '')}".strip(),
                'email': contact.get('Email'),
            }
    for user in data.get('users', data.get('User', [])):
        if user.get('Id'):
            users[user['Id']] = {
                'name': user.get('Name'),
                'email': user.get('Email'),
            }
    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    raw = data_subject.get('raw', {})
    return {
        'Contact ID': raw.get('Id'),
        'First Name': raw.get('FirstName'),
        'Last Name': raw.get('LastName'),
        'Email': raw.get('Email'),
        'Phone': raw.get('Phone'),
        'Mobile': raw.get('MobilePhone'),
        'Title': raw.get('Title'),
        'Department': raw.get('Department'),
        'Account': raw.get('Account', {}).get('Name') if isinstance(raw.get('Account'), dict) else raw.get('AccountId'),
        'Mailing Address': f"{raw.get('MailingStreet', '')} {raw.get('MailingCity', '')} {raw.get('MailingState', '')} {raw.get('MailingPostalCode', '')} {raw.get('MailingCountry', '')}".strip(),
        'Created Date': format_date(raw.get('CreatedDate')),
        'Last Modified': format_date(raw.get('LastModifiedDate')),
        'Lead Source': raw.get('LeadSource'),
        'Owner': raw.get('Owner', {}).get('Name') if isinstance(raw.get('Owner'), dict) else raw.get('OwnerId'),
    }


def extract_records(data: Dict[str, Any], data_subject_id: str, data_subject_email: str = None) -> List[Dict]:
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Activities/Tasks
    for activity in data.get('activities', data.get('Task', [])):
        who_id = str(activity.get('WhoId', ''))
        if who_id == ds_id:
            records.append({
                'date': format_date(activity.get('CreatedDate') or activity.get('ActivityDate')),
                'type': activity.get('TaskSubtype', 'task'),
                'category': 'Activity',
                'content': f"Subject: {activity.get('Subject')}\nDescription: {strip_html(activity.get('Description', '') or '')}",
            })

    # Cases
    for case in data.get('cases', data.get('Case', [])):
        if str(case.get('ContactId', '')) == ds_id:
            records.append({
                'date': format_date(case.get('CreatedDate')),
                'type': 'case',
                'category': f"Case #{case.get('CaseNumber')}",
                'content': f"Subject: {case.get('Subject')}\nStatus: {case.get('Status')}\nDescription: {strip_html(case.get('Description', '') or '')}",
            })

    # Opportunities
    for opp in data.get('opportunities', data.get('Opportunity', [])):
        contact_ids = opp.get('ContactIds', [])
        if ds_id in [str(c) for c in contact_ids]:
            records.append({
                'date': format_date(opp.get('CreatedDate')),
                'type': 'opportunity',
                'category': 'Sales',
                'content': f"Opportunity: {opp.get('Name')}\nStage: {opp.get('StageName')}\nAmount: {opp.get('Amount')}",
            })

    # Email messages
    for email in data.get('email_messages', data.get('EmailMessage', [])):
        if ds_email_lower in (email.get('ToAddress', '') + email.get('FromAddress', '')).lower():
            records.append({
                'date': format_date(email.get('MessageDate') or email.get('CreatedDate')),
                'type': 'email',
                'category': 'Communication',
                'content': f"Subject: {email.get('Subject')}\nFrom: {email.get('FromAddress')}\nTo: {email.get('ToAddress')}\nBody: {strip_html(email.get('TextBody') or email.get('HtmlBody', '') or '')}",
            })

    records.sort(key=lambda r: r.get('date', ''), reverse=True)
    return records


def process(export_path: str, data_subject_name: str, data_subject_email: str = None,
            extra_redactions: List[str] = None, output_dir: str = './output') -> tuple:
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading {VENDOR_NAME} export...")
    data = load_json(export_path)

    print(f"Searching for data subject: {data_subject_name}...")
    data_subject = find_data_subject(data, data_subject_name, data_subject_email)
    ds_id = data_subject['id']
    print(f"  Found: {data_subject['name']}")

    engine = RedactionEngine(data_subject_name, data_subject_email)
    for user_id, user_info in extract_users(data).items():
        engine.add_user(user_id, user_info.get('name'), user_info.get('email'))
    for name in (extra_redactions or []):
        engine.add_external(name)

    profile = extract_profile(data_subject)
    records = extract_records(data, ds_id, data_subject_email)

    redacted_records = [
        {**r, 'content': engine.redact(str(r.get('content', '')))} for r in records
    ]

    safe_name = safe_filename(data_subject_name)
    timestamp = get_timestamp()

    doc = create_vendor_report(VENDOR_NAME, data_subject_name, data_subject_email,
                               profile, redacted_records, redaction_stats=engine.get_stats(),
                               export_filename=os.path.basename(export_path))
    docx_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.docx")
    doc.save(docx_path)

    json_data = {'vendor': VENDOR_NAME, 'data_subject': data_subject_name, 'email': data_subject_email,
                 'generated': datetime.now().isoformat(), 'profile': profile, 'records': redacted_records,
                 'record_count': len(redacted_records)}
    json_path = os.path.join(output_dir, f"{VENDOR_NAME}_DSAR_{safe_name}_{timestamp}.json")
    save_json(json_data, json_path)

    key_path = os.path.join(output_dir, 'internal', f"{VENDOR_NAME}_REDACTION_KEY_{safe_name}_{timestamp}.json")
    save_json(engine.get_redaction_key(), key_path)

    print(f"\n✓ {VENDOR_NAME}: {len(redacted_records)} records processed")
    print(f"  → {docx_path}")
    return docx_path, json_path


if __name__ == '__main__':
    parser = setup_argparser(VENDOR_NAME)
    args = parser.parse_args()
    try:
        process(args.export_path, args.data_subject_name, args.email,
                parse_extra_redactions(args.redact), args.output)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
