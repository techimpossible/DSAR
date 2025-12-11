#!/usr/bin/env python3
"""
Mailchimp DSAR Processor

Export source: Mailchimp Audience > Settings > Export audience
               OR Mailchimp API export
Format: CSV/JSON with contacts, campaigns, and activity

Usage:
    python mailchimp_dsar.py export.json "John Smith" --email john@company.com
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
    load_csv,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    validate_data_subject_match,
    strip_html,
)
from core.activity_log import log_event

VENDOR_NAME = "Mailchimp"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load Mailchimp export from CSV or JSON."""
    if export_path.endswith('.csv'):
        members = load_csv(export_path)
        return {'members': members}
    else:
        return load_json(export_path)


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Mailchimp members."""
    members = data.get('members', data.get('contacts', data.get('subscribers', [])))
    matches = []
    name_lower = name.lower()

    for member in members:
        member_email = (member.get('email_address') or member.get('email') or '').lower()
        merge_fields = member.get('merge_fields', {})
        first_name = (merge_fields.get('FNAME', '') or member.get('first_name', '') or member.get('First Name', '')).lower()
        last_name = (merge_fields.get('LNAME', '') or member.get('last_name', '') or member.get('Last Name', '')).lower()
        full_name = f"{first_name} {last_name}".strip()

        is_match = False
        if email and member_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': member.get('id') or member.get('email_id') or member_email,
                'name': f"{first_name.title()} {last_name.title()}".strip() or member_email,
                'email': member.get('email_address') or member.get('email'),
                'raw': member,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all members for redaction mapping."""
    users = {}

    for member in data.get('members', data.get('contacts', [])):
        member_id = str(member.get('id') or member.get('email_address', ''))
        if member_id:
            merge_fields = member.get('merge_fields', {})
            users[member_id] = {
                'name': f"{merge_fields.get('FNAME', '')} {merge_fields.get('LNAME', '')}".strip(),
                'email': member.get('email_address') or member.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    merge_fields = raw.get('merge_fields', {})
    location = raw.get('location', {})
    stats = raw.get('stats', {})

    # Extract tags
    tags = raw.get('tags', [])
    tag_names = ', '.join([t.get('name', t) if isinstance(t, dict) else t for t in tags]) if tags else 'N/A'

    # Extract interests
    interests = raw.get('interests', {})
    interest_list = [k for k, v in interests.items() if v] if isinstance(interests, dict) else []

    return {
        'Member ID': raw.get('id'),
        'Email': raw.get('email_address') or raw.get('email'),
        'First Name': merge_fields.get('FNAME') or raw.get('first_name'),
        'Last Name': merge_fields.get('LNAME') or raw.get('last_name'),
        'Phone': merge_fields.get('PHONE') or raw.get('phone'),
        'Address': merge_fields.get('ADDRESS') or raw.get('address'),
        'Birthday': merge_fields.get('BIRTHDAY'),
        'Status': raw.get('status'),
        'Email Client': raw.get('email_client'),
        'Language': raw.get('language'),
        'VIP': raw.get('vip'),
        'Tags': tag_names,
        'Interests': ', '.join(interest_list) if interest_list else 'N/A',
        'Location': f"{location.get('city', '')}, {location.get('region', '')} {location.get('country_code', '')}".strip(', ') if location else 'N/A',
        'Latitude': location.get('latitude') if location else None,
        'Longitude': location.get('longitude') if location else None,
        'Timezone': location.get('timezone') if location else None,
        'IP Signup': raw.get('ip_signup'),
        'IP Opt-in': raw.get('ip_opt'),
        'Signup Timestamp': format_date(raw.get('timestamp_signup')),
        'Opt-in Timestamp': format_date(raw.get('timestamp_opt')),
        'Last Changed': format_date(raw.get('last_changed')),
        'Member Rating': raw.get('member_rating'),
        'Avg Open Rate': f"{stats.get('avg_open_rate', 0) * 100:.1f}%" if stats else 'N/A',
        'Avg Click Rate': f"{stats.get('avg_click_rate', 0) * 100:.1f}%" if stats else 'N/A',
        'Source': raw.get('source'),
        'Web ID': raw.get('web_id'),
        'List ID': raw.get('list_id'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all activity and campaign interactions for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Member activity
    for activity in data.get('activity', data.get('member_activity', [])):
        member_id = str(activity.get('email_id') or activity.get('member_id', ''))
        member_email = (activity.get('email_address') or '').lower()

        if member_id == ds_id or member_email == ds_email_lower:
            records.append({
                'date': format_date(activity.get('timestamp') or activity.get('created_at')),
                'type': activity.get('action', 'activity'),
                'category': 'Email Activity',
                'content': f"Action: {activity.get('action')}\nCampaign: {activity.get('campaign_title', activity.get('title', 'N/A'))}\nURL: {activity.get('url', 'N/A')}",
            })

    # Campaign interactions
    for campaign in data.get('campaigns', []):
        recipients = campaign.get('recipients', {})
        if ds_email_lower in str(recipients).lower():
            records.append({
                'date': format_date(campaign.get('send_time')),
                'type': 'campaign_sent',
                'category': 'Campaigns',
                'content': f"Campaign: {campaign.get('settings', {}).get('title', campaign.get('title', 'Untitled'))}\nSubject: {campaign.get('settings', {}).get('subject_line', 'N/A')}\nStatus: {campaign.get('status')}",
            })

    # Opens
    for open_event in data.get('opens', []):
        if (open_event.get('email_address') or '').lower() == ds_email_lower:
            records.append({
                'date': format_date(open_event.get('timestamp')),
                'type': 'open',
                'category': 'Email Activity',
                'content': f"Opened campaign: {open_event.get('campaign_id', 'N/A')}",
            })

    # Clicks
    for click in data.get('clicks', []):
        if (click.get('email_address') or '').lower() == ds_email_lower:
            records.append({
                'date': format_date(click.get('timestamp')),
                'type': 'click',
                'category': 'Email Activity',
                'content': f"Clicked URL: {click.get('url', 'N/A')}\nCampaign: {click.get('campaign_id', 'N/A')}",
            })

    # Unsubscribes
    for unsub in data.get('unsubscribes', []):
        if (unsub.get('email_address') or '').lower() == ds_email_lower:
            records.append({
                'date': format_date(unsub.get('timestamp')),
                'type': 'unsubscribe',
                'category': 'Subscription',
                'content': f"Unsubscribed\nReason: {unsub.get('reason', 'N/A')}\nCampaign: {unsub.get('campaign_id', 'N/A')}",
            })

    # E-commerce activity
    for order in data.get('ecommerce', data.get('orders', [])):
        if (order.get('email_address') or order.get('customer', {}).get('email', '') or '').lower() == ds_email_lower:
            records.append({
                'date': format_date(order.get('processed_at_foreign') or order.get('created_at')),
                'type': 'order',
                'category': 'E-commerce',
                'content': f"Order #{order.get('id')}\nTotal: {order.get('currency_code', '$')}{order.get('order_total', 'N/A')}\nStore: {order.get('store_id', 'N/A')}",
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
    """Process a Mailchimp export for DSAR response."""
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
        print(f"Loading Mailchimp export from {export_path}...")
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
        print(f"  Mapped {engine.get_total_redactions()} members for redaction")

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
