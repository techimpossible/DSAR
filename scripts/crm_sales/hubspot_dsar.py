#!/usr/bin/env python3
"""
HubSpot DSAR Processor

Export source: HubSpot Settings > Account Management > Privacy & Consent > GDPR > Export
               OR HubSpot API data export
Format: JSON with contacts, companies, deals, activities, and engagements

Usage:
    python hubspot_dsar.py export.json "John Smith" --email john@company.com
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
    print_progress,
    strip_html,
)
from core.activity_log import log_event

VENDOR_NAME = "HubSpot"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in HubSpot contacts."""
    contacts = data.get('contacts', [])
    if isinstance(data, list):
        contacts = data

    matches = []
    name_lower = name.lower()

    for contact in contacts:
        props = contact.get('properties', contact)
        contact_email = props.get('email', '').lower()
        first_name = props.get('firstname', '').lower()
        last_name = props.get('lastname', '').lower()
        full_name = f"{first_name} {last_name}".strip()

        is_match = False
        if email and contact_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True
        elif first_name and last_name and first_name in name_lower and last_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': contact.get('id') or contact.get('vid'),
                'name': f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or 'Unknown',
                'email': props.get('email'),
                'properties': props,
                'raw': contact,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all contacts for redaction mapping."""
    users = {}
    contacts = data.get('contacts', [])
    if isinstance(data, list):
        contacts = data

    for contact in contacts:
        props = contact.get('properties', contact)
        contact_id = str(contact.get('id') or contact.get('vid', ''))
        if contact_id:
            users[contact_id] = {
                'name': f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                'email': props.get('email'),
            }

    # Also extract owners
    for owner in data.get('owners', []):
        owner_id = str(owner.get('ownerId', owner.get('id', '')))
        if owner_id:
            users[owner_id] = {
                'name': f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip(),
                'email': owner.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    props = data_subject.get('properties', {})

    return {
        'Contact ID': data_subject.get('id'),
        'Email': props.get('email'),
        'First Name': props.get('firstname'),
        'Last Name': props.get('lastname'),
        'Phone': props.get('phone'),
        'Mobile Phone': props.get('mobilephone'),
        'Company': props.get('company'),
        'Job Title': props.get('jobtitle'),
        'Website': props.get('website'),
        'City': props.get('city'),
        'State': props.get('state'),
        'Country': props.get('country'),
        'Lifecycle Stage': props.get('lifecyclestage'),
        'Lead Status': props.get('hs_lead_status'),
        'Created Date': format_date(props.get('createdate')),
        'Last Modified': format_date(props.get('lastmodifieddate')),
        'Last Activity': format_date(props.get('notes_last_updated')),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all activities and engagements for the data subject."""
    records = []
    ds_email_lower = (data_subject_email or '').lower()

    # Extract engagements
    engagements = data.get('engagements', [])
    for eng in engagements:
        engagement = eng.get('engagement', {})
        associations = eng.get('associations', {})
        metadata = eng.get('metadata', {})

        # Check if associated with data subject
        contact_ids = associations.get('contactIds', [])
        if data_subject_id and int(data_subject_id) not in contact_ids:
            if str(data_subject_id) not in [str(c) for c in contact_ids]:
                continue

        eng_type = engagement.get('type', 'UNKNOWN')
        timestamp = engagement.get('timestamp') or engagement.get('createdAt')

        content = ''
        if eng_type == 'EMAIL':
            content = f"Subject: {metadata.get('subject', '')}\n{strip_html(metadata.get('text', metadata.get('html', '')))}"
        elif eng_type == 'NOTE':
            content = strip_html(metadata.get('body', ''))
        elif eng_type == 'CALL':
            content = f"Call ({metadata.get('status', '')}): {metadata.get('body', '')}"
        elif eng_type == 'MEETING':
            content = f"Meeting: {metadata.get('title', '')}\n{metadata.get('body', '')}"
        elif eng_type == 'TASK':
            content = f"Task: {metadata.get('subject', '')}\n{metadata.get('body', '')}"
        else:
            content = str(metadata)

        records.append({
            'date': format_date(timestamp),
            'type': eng_type.lower(),
            'category': 'engagement',
            'content': content,
        })

    # Extract form submissions
    for form in data.get('form_submissions', []):
        if form.get('contact_id') == data_subject_id:
            records.append({
                'date': format_date(form.get('submitted_at')),
                'type': 'form_submission',
                'category': 'form',
                'content': f"Form: {form.get('form_name', 'Unknown')}\nFields: {json.dumps(form.get('values', {}))}",
            })

    # Extract email events
    for event in data.get('email_events', []):
        recipient = event.get('recipient', '').lower()
        if recipient == ds_email_lower:
            records.append({
                'date': format_date(event.get('created')),
                'type': event.get('type', 'email_event').lower(),
                'category': 'email',
                'content': f"Email {event.get('type', 'event')}: {event.get('emailCampaignId', '')}",
            })

    # Extract deals
    for deal in data.get('deals', []):
        associations = deal.get('associations', {})
        contact_ids = associations.get('associatedVids', [])
        if data_subject_id and str(data_subject_id) in [str(c) for c in contact_ids]:
            props = deal.get('properties', {})
            records.append({
                'date': format_date(props.get('createdate', {}).get('value')),
                'type': 'deal',
                'category': 'sales',
                'content': f"Deal: {props.get('dealname', {}).get('value', 'Unknown')}\nStage: {props.get('dealstage', {}).get('value', 'Unknown')}\nAmount: {props.get('amount', {}).get('value', 'N/A')}",
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
    """Process a HubSpot export for DSAR response."""
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
        print(f"Loading HubSpot export from {export_path}...")
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
        print(f"  Mapped {engine.get_total_redactions()} contacts for redaction")

        for name in (extra_redactions or []):
            engine.add_external(name)

        print("Extracting profile data...")
        profile = extract_profile(data_subject)

        print("Extracting activity records...")
        records = extract_records(data, ds_id, data_subject_email)
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
