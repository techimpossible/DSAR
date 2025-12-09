#!/usr/bin/env python3
"""
Stripe DSAR Processor

Export source: Stripe Dashboard > Settings > Compliance > Data Export
               OR Stripe API export
Format: JSON with customers, charges, invoices, and subscriptions

Usage:
    python stripe_dsar.py export.json "John Smith" --email john@company.com
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

VENDOR_NAME = "Stripe"


def find_data_subject(
    data: Dict[str, Any],
    name: str,
    email: str = None
) -> Optional[Dict]:
    """Find the data subject in Stripe customers."""
    customers = data.get('customers', data.get('data', []))
    matches = []
    name_lower = name.lower()

    for customer in customers:
        customer_email = (customer.get('email') or '').lower()
        customer_name = (customer.get('name') or '').lower()

        is_match = False
        if email and customer_email == email.lower():
            is_match = True
        elif name_lower in customer_name or customer_name in name_lower:
            is_match = True

        if is_match:
            matches.append({
                'id': customer.get('id'),
                'name': customer.get('name') or customer.get('email'),
                'email': customer.get('email'),
                'raw': customer,
            })

    return validate_data_subject_match(matches, name, email)


def extract_users(data: Dict[str, Any]) -> Dict[str, Dict]:
    """Extract all customers for redaction mapping."""
    users = {}

    for customer in data.get('customers', data.get('data', [])):
        cust_id = str(customer.get('id', ''))
        if cust_id:
            users[cust_id] = {
                'name': customer.get('name'),
                'email': customer.get('email'),
            }

    return users


def extract_profile(data_subject: Dict) -> Dict[str, Any]:
    """Extract profile data for the data subject."""
    raw = data_subject.get('raw', {})
    address = raw.get('address', {}) or {}
    shipping = raw.get('shipping', {}) or {}
    shipping_address = shipping.get('address', {}) or {}

    # Extract default source info
    default_source = raw.get('default_source')
    sources = raw.get('sources', {}).get('data', [])
    card_info = 'N/A'
    for source in sources:
        if source.get('id') == default_source:
            card_info = f"**** **** **** {source.get('last4', '****')} ({source.get('brand', 'Card')})"
            break

    return {
        'Customer ID': raw.get('id'),
        'Name': raw.get('name'),
        'Email': raw.get('email'),
        'Phone': raw.get('phone'),
        'Description': raw.get('description'),
        'Created': format_date(raw.get('created')),
        'Currency': raw.get('currency', 'N/A').upper() if raw.get('currency') else 'N/A',
        'Balance': f"{raw.get('balance', 0) / 100:.2f}" if raw.get('balance') is not None else 'N/A',
        'Delinquent': raw.get('delinquent'),
        'Default Payment': card_info,
        'Invoice Prefix': raw.get('invoice_prefix'),
        'Billing Address': f"{address.get('line1', '')} {address.get('line2', '')} {address.get('city', '')} {address.get('state', '')} {address.get('postal_code', '')} {address.get('country', '')}".strip() or 'N/A',
        'Shipping Name': shipping.get('name', 'N/A'),
        'Shipping Phone': shipping.get('phone', 'N/A'),
        'Shipping Address': f"{shipping_address.get('line1', '')} {shipping_address.get('city', '')} {shipping_address.get('country', '')}".strip() or 'N/A',
        'Tax Exempt': raw.get('tax_exempt'),
        'Livemode': raw.get('livemode'),
    }


def format_amount(amount: Any, currency: str = 'usd') -> str:
    """Format amount from cents to currency string."""
    if amount is None:
        return 'N/A'
    try:
        return f"{currency.upper()} {int(amount) / 100:.2f}"
    except (ValueError, TypeError):
        return str(amount)


def extract_records(
    data: Dict[str, Any],
    data_subject_id: str,
    data_subject_email: str = None
) -> List[Dict]:
    """Extract all charges, invoices, and subscriptions for the data subject."""
    records = []
    ds_id = str(data_subject_id)
    ds_email_lower = (data_subject_email or '').lower()

    # Charges
    for charge in data.get('charges', []):
        customer_id = str(charge.get('customer', ''))
        receipt_email = (charge.get('receipt_email') or '').lower()

        if customer_id == ds_id or receipt_email == ds_email_lower:
            records.append({
                'date': format_date(charge.get('created')),
                'type': 'charge',
                'category': 'Payments',
                'content': f"Charge ID: {charge.get('id')}\nAmount: {format_amount(charge.get('amount'), charge.get('currency', 'usd'))}\nStatus: {charge.get('status')}\nDescription: {charge.get('description', 'N/A')}\nPayment Method: **** {charge.get('payment_method_details', {}).get('card', {}).get('last4', '****')}\nReceipt: {charge.get('receipt_url', 'N/A')}",
            })

    # Invoices
    for invoice in data.get('invoices', []):
        customer_id = str(invoice.get('customer', ''))
        customer_email = (invoice.get('customer_email') or '').lower()

        if customer_id == ds_id or customer_email == ds_email_lower:
            # Extract line items
            lines = invoice.get('lines', {}).get('data', [])
            line_items = '\n'.join([f"  - {l.get('description', 'Item')}: {format_amount(l.get('amount'), invoice.get('currency', 'usd'))}" for l in lines[:5]])

            records.append({
                'date': format_date(invoice.get('created')),
                'type': 'invoice',
                'category': 'Invoices',
                'content': f"Invoice #{invoice.get('number', invoice.get('id'))}\nAmount Due: {format_amount(invoice.get('amount_due'), invoice.get('currency', 'usd'))}\nAmount Paid: {format_amount(invoice.get('amount_paid'), invoice.get('currency', 'usd'))}\nStatus: {invoice.get('status')}\nItems:\n{line_items}\nPDF: {invoice.get('invoice_pdf', 'N/A')}",
            })

    # Subscriptions
    for sub in data.get('subscriptions', []):
        customer_id = str(sub.get('customer', ''))

        if customer_id == ds_id:
            items = sub.get('items', {}).get('data', [])
            plan_names = ', '.join([i.get('plan', {}).get('nickname', i.get('price', {}).get('nickname', 'Plan')) or 'Plan' for i in items])

            records.append({
                'date': format_date(sub.get('created')),
                'type': 'subscription',
                'category': 'Subscriptions',
                'content': f"Subscription ID: {sub.get('id')}\nStatus: {sub.get('status')}\nPlans: {plan_names}\nCurrent Period: {format_date(sub.get('current_period_start'))} to {format_date(sub.get('current_period_end'))}\nCancel At Period End: {sub.get('cancel_at_period_end')}",
            })

    # Payment Intents
    for intent in data.get('payment_intents', []):
        customer_id = str(intent.get('customer', ''))

        if customer_id == ds_id:
            records.append({
                'date': format_date(intent.get('created')),
                'type': 'payment_intent',
                'category': 'Payments',
                'content': f"Payment Intent: {intent.get('id')}\nAmount: {format_amount(intent.get('amount'), intent.get('currency', 'usd'))}\nStatus: {intent.get('status')}\nDescription: {intent.get('description', 'N/A')}",
            })

    # Refunds
    for refund in data.get('refunds', []):
        # Find if this refund is for a charge belonging to the customer
        charge_id = refund.get('charge')
        charge = next((c for c in data.get('charges', []) if c.get('id') == charge_id), None)

        if charge and str(charge.get('customer', '')) == ds_id:
            records.append({
                'date': format_date(refund.get('created')),
                'type': 'refund',
                'category': 'Refunds',
                'content': f"Refund ID: {refund.get('id')}\nAmount: {format_amount(refund.get('amount'), refund.get('currency', 'usd'))}\nStatus: {refund.get('status')}\nReason: {refund.get('reason', 'N/A')}\nOriginal Charge: {charge_id}",
            })

    # Disputes
    for dispute in data.get('disputes', []):
        charge_id = dispute.get('charge')
        charge = next((c for c in data.get('charges', []) if c.get('id') == charge_id), None)

        if charge and str(charge.get('customer', '')) == ds_id:
            records.append({
                'date': format_date(dispute.get('created')),
                'type': 'dispute',
                'category': 'Disputes',
                'content': f"Dispute ID: {dispute.get('id')}\nAmount: {format_amount(dispute.get('amount'), dispute.get('currency', 'usd'))}\nStatus: {dispute.get('status')}\nReason: {dispute.get('reason', 'N/A')}",
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
    """Process a Stripe export for DSAR response."""
    ensure_output_dir(output_dir)
    ensure_output_dir(os.path.join(output_dir, 'internal'))

    print(f"Loading Stripe export from {export_path}...")
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
    print(f"  Mapped {engine.get_total_redactions()} customers for redaction")

    for name in (extra_redactions or []):
        engine.add_external(name)

    print("Extracting profile data...")
    profile = extract_profile(data_subject)

    print("Extracting transaction records...")
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
