#!/usr/bin/env python3
"""
BambooHR DSAR Processor

Export source: BambooHR Settings > Data > Export Data
               OR BambooHR API export
Format: CSV/JSON with employee data, time off, documents, and performance

Usage:
    python bamboohr_dsar.py export.json "John Smith" --email john@company.com
    python bamboohr_dsar.py export.csv "John Smith" --email john@company.com
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
    load_csv,
    save_json,
    ensure_output_dir,
    safe_filename,
    format_date,
    get_timestamp,
    validate_data_subject_match,
)

VENDOR_NAME = "BambooHR"


def load_export(export_path: str) -> Dict[str, Any]:
    """Load BambooHR export from CSV or JSON."""
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
    """Find the data subject in BambooHR employees."""
    employees = data.get('employees', data.get('data', []))
    matches = []
    name_lower = name.lower()

    for emp in employees:
        emp_email = (emp.get('workEmail') or emp.get('email') or emp.get('homeEmail') or '').lower()
        first_name = (emp.get('firstName') or emp.get('first_name') or '').lower()
        last_name = (emp.get('lastName') or emp.get('last_name') or '').lower()
        full_name = f"{first_name} {last_name}".strip()
        display_name = (emp.get('displayName') or emp.get('display_name') or '').lower()

        is_match = False
        if email and emp_email == email.lower():
            is_match = True
        elif name_lower in full_name or full_name in name_lower:
            is_match = True
        elif display_name and (name_lower in display_name or display_name in name_lower):
            is_match = True

        if is_match:
            matches.append({
                'id': emp.get('id') or emp.get('employeeId') or emp.get('employee_id'),
                'name': f"{emp.get('firstName', emp.get('first_name', ''))} {emp.get('lastName', emp.get('last_name', ''))}".strip(),
                'email': emp.get('workEmail') or emp.get('email'),
                'raw': emp,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all employees for redaction mapping."""
    users = {}

    for emp in data.get('employees', data.get('data', [])):
        emp_id = str(emp.get('id') or emp.get('employeeId') or emp.get('employee_id', ''))
        if emp_id:
            users[emp_id] = {
                'name': f"{emp.get('firstName', emp.get('first_name', ''))} {emp.get('lastName', emp.get('last_name', ''))}".strip(),
                'email': emp.get('workEmail') or emp.get('email'),
            }

    # Extract supervisors
    for emp in data.get('employees', data.get('data', [])):
        supervisor_id = emp.get('supervisorId') or emp.get('supervisor_id')
        supervisor_name = emp.get('supervisor') or emp.get('supervisorName')
        if supervisor_id and supervisor_name and str(supervisor_id) not in users:
            users[str(supervisor_id)] = {'name': supervisor_name, 'email': None}

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})

    return {
        'Employee ID': raw.get('id') or raw.get('employeeId'),
        'First Name': raw.get('firstName') or raw.get('first_name'),
        'Last Name': raw.get('lastName') or raw.get('last_name'),
        'Display Name': raw.get('displayName') or raw.get('display_name'),
        'Preferred Name': raw.get('preferredName') or raw.get('preferred_name'),
        'Work Email': raw.get('workEmail') or raw.get('email'),
        'Home Email': raw.get('homeEmail') or raw.get('home_email'),
        'Mobile Phone': raw.get('mobilePhone') or raw.get('mobile_phone'),
        'Work Phone': raw.get('workPhone') or raw.get('work_phone'),
        'Home Phone': raw.get('homePhone') or raw.get('home_phone'),
        'Job Title': raw.get('jobTitle') or raw.get('job_title'),
        'Department': raw.get('department'),
        'Division': raw.get('division'),
        'Location': raw.get('location'),
        'Supervisor': raw.get('supervisor') or raw.get('supervisorName'),
        'Hire Date': format_date(raw.get('hireDate') or raw.get('hire_date')),
        'Original Hire Date': format_date(raw.get('originalHireDate') or raw.get('original_hire_date')),
        'Termination Date': format_date(raw.get('terminationDate') or raw.get('termination_date')),
        'Employment Status': raw.get('employmentStatus') or raw.get('employment_status') or raw.get('status'),
        'Employment Type': raw.get('employmentHistoryStatus') or raw.get('employment_type'),
        'Pay Rate': raw.get('payRate') or raw.get('pay_rate'),
        'Pay Type': raw.get('payType') or raw.get('pay_type'),
        'Pay Period': raw.get('payPeriod') or raw.get('pay_period'),
        'Date of Birth': format_date(raw.get('dateOfBirth') or raw.get('date_of_birth')),
        'Age': raw.get('age'),
        'Gender': raw.get('gender'),
        'Marital Status': raw.get('maritalStatus') or raw.get('marital_status'),
        'SSN (masked)': '***-**-' + str(raw.get('ssn', raw.get('socialSecurityNumber', '')))[-4:] if raw.get('ssn') or raw.get('socialSecurityNumber') else 'N/A',
        'Address': f"{raw.get('address1', raw.get('street1', ''))} {raw.get('address2', raw.get('street2', ''))} {raw.get('city', '')} {raw.get('state', '')} {raw.get('zipcode', raw.get('zip', ''))} {raw.get('country', '')}".strip(),
        'Emergency Contact': raw.get('emergencyContactName') or raw.get('emergency_contact_name'),
        'Emergency Phone': raw.get('emergencyContactPhone') or raw.get('emergency_contact_phone'),
        'Ethnicity': raw.get('ethnicity'),
        'EEO Job Category': raw.get('eeo') or raw.get('eeoCategory'),
        'FLSA Status': raw.get('flsaCode') or raw.get('exempt'),
    }


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str
) -> List[Dict]:
    """Extract all time off, documents, and performance records for the data subject."""
    records = []
    ds_id = str(data_subject_id)

    # Time off requests
    for request in data.get('timeOffRequests', data.get('time_off_requests', data.get('timeOff', []))):
        if str(request.get('employeeId') or request.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(request.get('created') or request.get('created_at')),
                'type': 'time_off_request',
                'category': 'Time Off',
                'content': f"Type: {request.get('type') or request.get('timeOffType') or request.get('time_off_type')}\nDates: {request.get('start')} to {request.get('end')}\nStatus: {request.get('status')}\nAmount: {request.get('amount', '')} {request.get('unit', 'days')}\nNotes: {request.get('notes', request.get('note', ''))}",
            })

    # Performance/goals
    for goal in data.get('goals', data.get('performance', [])):
        if str(goal.get('employeeId') or goal.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(goal.get('createdDate') or goal.get('created_at')),
                'type': 'goal',
                'category': 'Performance',
                'content': f"Goal: {goal.get('title') or goal.get('description', '')}\nStatus: {goal.get('status', '')}\nDue: {format_date(goal.get('dueDate') or goal.get('due_date'))}",
            })

    # Training records
    for training in data.get('training', data.get('trainingRecords', [])):
        if str(training.get('employeeId') or training.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(training.get('completedDate') or training.get('completed_date')),
                'type': 'training',
                'category': 'Training',
                'content': f"Training: {training.get('type') or training.get('name')}\nStatus: {training.get('status', 'completed')}\nCost: {training.get('cost', 'N/A')}",
            })

    # Documents
    for doc in data.get('documents', data.get('files', [])):
        if str(doc.get('employeeId') or doc.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(doc.get('createdDate') or doc.get('created_at') or doc.get('dateAdded')),
                'type': 'document',
                'category': 'Documents',
                'content': f"Document: {doc.get('name') or doc.get('filename')}\nCategory: {doc.get('category', 'N/A')}",
            })

    # Employment history
    for history in data.get('employmentHistory', data.get('employment_history', [])):
        if str(history.get('employeeId') or history.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(history.get('date') or history.get('effectiveDate')),
                'type': 'employment_change',
                'category': 'Employment History',
                'content': f"Change: {history.get('employmentStatus', '')} - {history.get('jobTitle', '')}\nDepartment: {history.get('department', '')}",
            })

    # Compensation history
    for comp in data.get('compensation', data.get('compensationHistory', [])):
        if str(comp.get('employeeId') or comp.get('employee_id', '')) == ds_id:
            records.append({
                'date': format_date(comp.get('startDate') or comp.get('effectiveDate')),
                'type': 'compensation_change',
                'category': 'Compensation',
                'content': f"Rate: {comp.get('rate', '')} {comp.get('type', '')}\nReason: {comp.get('reason', 'N/A')}",
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
    """Process a BambooHR export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading BambooHR export from {export_path}...")
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
