#!/usr/bin/env python3
"""
Okta DSAR Processor

Export source: Okta Admin Console > Reports > Reports > Download User Data
               OR Okta API export via admin token
Format: JSON with users, groups, applications, and logs

Usage:
    python okta_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
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
)

VENDOR_NAME = "Okta"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Okta users."""
    users = data.get('users', data.get('data', []))
    matches = []
    name_lower = name.lower()

    for user in users:
        profile = user.get('profile', user)
        user_email = (profile.get('email') or profile.get('login') or '').lower()
        first_name = (profile.get('firstName') or profile.get('first_name') or '').lower()
        last_name = (profile.get('lastName') or profile.get('last_name') or '').lower()
        full_name = f"{first_name} {last_name}".strip()
        display_name = (profile.get('displayName') or '').lower()

        is_match = False
        if email and user_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True
        elif display_name and (name_lower in display_name or display_name in name_lower):
            is_match = True

        if is_match:
            matches.append({
                'id': user.get('id'),
                'name': f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip(),
                'email': profile.get('email') or profile.get('login'),
                'raw': user,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all users for redaction mapping."""
    users = {}

    for user in data.get('users', data.get('data', [])):
        user_id = str(user.get('id', ''))
        profile = user.get('profile', user)
        if user_id:
            users[user_id] = {
                'name': f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip(),
                'email': profile.get('email') or profile.get('login'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    profile = raw.get('profile', raw)
    credentials = raw.get('credentials', {})

    # Extract groups
    embedded = raw.get('_embedded', {})
    groups = embedded.get('groups', [])
    group_names = ', '.join([g.get('profile', {}).get('name', g.get('name', '')) for g in groups]) if groups else 'N/A'

    return {
        'User ID': raw.get('id'),
        'Login': profile.get('login'),
        'Email': profile.get('email'),
        'First Name': profile.get('firstName') or profile.get('first_name'),
        'Last Name': profile.get('lastName') or profile.get('last_name'),
        'Display Name': profile.get('displayName'),
        'Nick Name': profile.get('nickName'),
        'Second Email': profile.get('secondEmail'),
        'Mobile Phone': profile.get('mobilePhone'),
        'Primary Phone': profile.get('primaryPhone'),
        'Title': profile.get('title'),
        'Department': profile.get('department'),
        'Division': profile.get('division'),
        'Organization': profile.get('organization'),
        'Manager': profile.get('manager'),
        'Manager ID': profile.get('managerId'),
        'Employee Number': profile.get('employeeNumber'),
        'Employee Type': profile.get('employeeType'),
        'Cost Center': profile.get('costCenter'),
        'Street Address': profile.get('streetAddress'),
        'City': profile.get('city'),
        'State': profile.get('state'),
        'Zip Code': profile.get('zipCode'),
        'Country Code': profile.get('countryCode'),
        'Locale': profile.get('locale'),
        'Timezone': profile.get('timezone'),
        'User Type': profile.get('userType'),
        'Status': raw.get('status'),
        'Created': format_date(raw.get('created')),
        'Activated': format_date(raw.get('activated')),
        'Status Changed': format_date(raw.get('statusChanged')),
        'Last Login': format_date(raw.get('lastLogin')),
        'Last Updated': format_date(raw.get('lastUpdated')),
        'Password Changed': format_date(raw.get('passwordChanged')),
        'Provider Type': credentials.get('provider', {}).get('type'),
        'Provider Name': credentials.get('provider', {}).get('name'),
        'Groups': group_names,
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all app assignments, logs, and factors for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # App assignments
    for assignment in data.get('appUsers', data.get('app_assignments', [])):
        if str(assignment.get('userId') or assignment.get('user_id', '')) == ds_id:
            records.append({
                'date': format_date(assignment.get('created')),
                'type': 'app_assignment',
                'category': 'Applications',
                'content': f"App ID: {assignment.get('appId') or assignment.get('app_id')}\nStatus: {assignment.get('status', 'ACTIVE')}\nSync State: {assignment.get('syncState', 'N/A')}\nCredentials: {'Username stored' if assignment.get('credentials') else 'N/A'}",
            })

    # Group memberships
    for membership in data.get('groupMemberships', data.get('group_memberships', [])):
        if str(membership.get('userId') or membership.get('user_id', '')) == ds_id:
            records.append({
                'date': format_date(membership.get('created') or membership.get('lastMembershipUpdated')),
                'type': 'group_membership',
                'category': 'Groups',
                'content': f"Group: {membership.get('groupName') or membership.get('profile', {}).get('name', 'Unknown')}\nGroup ID: {membership.get('groupId') or membership.get('id')}\nType: {membership.get('type', 'OKTA_GROUP')}",
            })

    # MFA factors
    for factor in data.get('factors', data.get('enrolled_factors', [])):
        if str(factor.get('userId') or factor.get('user_id', '')) == ds_id:
            records.append({
                'date': format_date(factor.get('created')),
                'type': 'mfa_factor',
                'category': 'Security',
                'content': f"Factor Type: {factor.get('factorType', factor.get('type', 'unknown'))}\nProvider: {factor.get('provider', 'OKTA')}\nStatus: {factor.get('status', 'ACTIVE')}\nDevice: {factor.get('profile', {}).get('deviceType', 'N/A')}",
            })

    # System logs (audit events)
    for log in data.get('logs', data.get('system_logs', [])):
        # Check if user is actor or target
        actor = log.get('actor', {})
        targets = log.get('target', [])

        actor_id = actor.get('id', '')
        actor_email = (actor.get('alternateId', '') or '').lower()
        target_ids = [t.get('id', '') for t in targets]

        is_involved = (str(actor_id) == ds_id or
                       actor_email == ds_email_lower or
                       ds_id in [str(t) for t in target_ids])

        if is_involved:
            target_info = ', '.join([f"{t.get('type', 'Unknown')}: {t.get('displayName', t.get('alternateId', 'Unknown'))}" for t in targets[:3]])

            records.append({
                'date': format_date(log.get('published')),
                'type': log.get('eventType', 'event'),
                'category': 'Audit Logs',
                'content': f"Event: {log.get('displayMessage', log.get('eventType', 'Unknown'))}\nOutcome: {log.get('outcome', {}).get('result', 'UNKNOWN')}\nClient IP: {log.get('client', {}).get('ipAddress', 'N/A')}\nUser Agent: {log.get('client', {}).get('userAgent', {}).get('rawUserAgent', 'N/A')[:100]}\nTargets: {target_info or 'N/A'}",
            })

    # Sessions
    for session in data.get('sessions', []):
        if str(session.get('userId', '')) == ds_id:
            records.append({
                'date': format_date(session.get('createdAt')),
                'type': 'session',
                'category': 'Sessions',
                'content': f"Session ID: {session.get('id')}\nStatus: {session.get('status')}\nLast Password Verification: {format_date(session.get('lastPasswordVerification'))}\nLast Factor Verification: {format_date(session.get('lastFactorVerification'))}\nExpires: {format_date(session.get('expiresAt'))}",
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
    """Process an Okta export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Okta export from {export_path}...")
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
