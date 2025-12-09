#!/usr/bin/env python3
"""
CharlieHR DSAR Processor

Export source: CharlieHR Admin > Settings > Data Export
               OR CharlieHR API export
Format: CSV/JSON with employee data, time off, reviews, and documents

Usage:
    python charliehr_dsar.py export.csv "John Smith" --email john@company.com
    python charliehr_dsar.py export.json "John Smith" --email john@company.com
"""

import sys
import os
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
    load_csv,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    validate_data_subject_match,
)

VENDOR_NAME = "CharlieHR"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load CharlieHR export from CSV or JSON."""
    if export_path.endswith('.csv'):
        employees = load_csv(export_path)
        return {'employees': employees}
    else:
        return load_json(export_path)


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in CharlieHR employees."""
    employees = data.get('employees', data.get('team_members', []))
    matches = []
    name_lower = name.lower()

    for emp in employees:
        emp_email = (emp.get('email') or emp.get('work_email') or '').lower()
        first_name = (emp.get('first_name') or emp.get('firstName') or '').lower()
        last_name = (emp.get('last_name') or emp.get('lastName') or '').lower()
        full_name = f"{first_name} {last_name}".strip()
        preferred_name = (emp.get('preferred_name') or emp.get('preferredName') or '').lower()

        is_match = False
        if email and emp_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True
        elif preferred_name and (name_lower in preferred_name or preferred_name in name_lower):
            is_match = True

        if is_match:
            matches.append({
                'id': emp.get('id') or emp.get('employee_id'),
                'name': f"{emp.get('first_name', emp.get('firstName', ''))} {emp.get('last_name', emp.get('lastName', ''))}".strip(),
                'email': emp.get('email') or emp.get('work_email'),
                'raw': emp,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all employees for redaction mapping."""
    users = {}
    employees = data.get('employees', data.get('team_members', []))

    for emp in employees:
        emp_id = str(emp.get('id') or emp.get('employee_id', ''))
        if emp_id:
            users[emp_id] = {
                'name': f"{emp.get('first_name', emp.get('firstName', ''))} {emp.get('last_name', emp.get('lastName', ''))}".strip(),
                'email': emp.get('email') or emp.get('work_email'),
            }

    # Extract managers
    for emp in employees:
        manager_id = emp.get('manager_id') or emp.get('managerId')
        manager_name = emp.get('manager_name') or emp.get('managerName')
        if manager_id and manager_name and str(manager_id) not in users:
            users[str(manager_id)] = {'name': manager_name, 'email': None}

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    return {
        'Employee ID': data_subject.get('id'),
        'First Name': raw.get('first_name') or raw.get('firstName'),
        'Last Name': raw.get('last_name') or raw.get('lastName'),
        'Preferred Name': raw.get('preferred_name') or raw.get('preferredName'),
        'Email': raw.get('email') or raw.get('work_email'),
        'Personal Email': raw.get('personal_email') or raw.get('personalEmail'),
        'Phone': raw.get('phone') or raw.get('mobile_phone'),
        'Job Title': raw.get('job_title') or raw.get('jobTitle'),
        'Department': raw.get('department'),
        'Location': raw.get('location') or raw.get('office'),
        'Manager': raw.get('manager_name') or raw.get('managerName'),
        'Start Date': format_date(raw.get('start_date') or raw.get('startDate')),
        'Probation End': format_date(raw.get('probation_end_date') or raw.get('probationEndDate')),
        'Employment Type': raw.get('employment_type') or raw.get('employmentType'),
        'Contract Type': raw.get('contract_type') or raw.get('contractType'),
        'Working Pattern': raw.get('working_pattern') or raw.get('workingPattern'),
        'Date of Birth': format_date(raw.get('date_of_birth') or raw.get('dateOfBirth')),
        'Gender': raw.get('gender'),
        'Nationality': raw.get('nationality'),
        'Address': f"{raw.get('address_line_1', '')} {raw.get('address_line_2', '')} {raw.get('city', '')} {raw.get('postcode', '')}".strip(),
        'Emergency Contact': raw.get('emergency_contact_name') or raw.get('emergencyContactName'),
        'Emergency Phone': raw.get('emergency_contact_phone') or raw.get('emergencyContactPhone'),
        'Bank Account (masked)': '****' + str(raw.get('bank_account_number', ''))[-4:] if raw.get('bank_account_number') else 'N/A',
        'NI Number (masked)': '****' + str(raw.get('ni_number') or raw.get('national_insurance_number', ''))[-4:] if raw.get('ni_number') or raw.get('national_insurance_number') else 'N/A',
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all activity records for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Time off requests
    for request in data.get('time_off_requests', data.get('timeOffRequests', [])):
        if str(request.get('employee_id') or request.get('employeeId', '')) == ds_id:
            records.append({
                'date': format_date(request.get('created_at') or request.get('createdAt')),
                'type': 'time_off_request',
                'category': 'Time Off',
                'content': f"Type: {request.get('type') or request.get('leave_type')}\nDates: {request.get('start_date')} to {request.get('end_date')}\nStatus: {request.get('status')}\nDays: {request.get('days') or request.get('duration')}",
            })

    # Reviews/check-ins
    for review in data.get('reviews', data.get('check_ins', [])):
        if str(review.get('employee_id') or review.get('employeeId', '')) == ds_id:
            records.append({
                'date': format_date(review.get('date') or review.get('created_at')),
                'type': 'review',
                'category': 'Performance',
                'content': f"Type: {review.get('type', 'Review')}\nNotes: {review.get('notes') or review.get('content', '')}",
            })

    # Goals
    for goal in data.get('goals', []):
        if str(goal.get('employee_id') or goal.get('employeeId', '')) == ds_id:
            records.append({
                'date': format_date(goal.get('created_at') or goal.get('createdAt')),
                'type': 'goal',
                'category': 'Goals',
                'content': f"Goal: {goal.get('title') or goal.get('name')}\nDescription: {goal.get('description', '')}\nStatus: {goal.get('status', '')}",
            })

    # Documents
    for doc in data.get('documents', []):
        if str(doc.get('employee_id') or doc.get('employeeId', '')) == ds_id:
            records.append({
                'date': format_date(doc.get('uploaded_at') or doc.get('created_at')),
                'type': 'document',
                'category': 'Documents',
                'content': f"Document: {doc.get('name') or doc.get('filename')}\nType: {doc.get('document_type') or doc.get('category', '')}",
            })

    # Notes
    for note in data.get('notes', data.get('employee_notes', [])):
        if str(note.get('employee_id') or note.get('employeeId', '')) == ds_id:
            records.append({
                'date': format_date(note.get('created_at') or note.get('date')),
                'type': 'note',
                'category': 'Notes',
                'content': note.get('content') or note.get('body', ''),
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
    """Process a CharlieHR export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading CharlieHR export from {export_path}...")
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
    print(f"  Mapped {engine.get_total_redactions()} employees for redaction")

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
