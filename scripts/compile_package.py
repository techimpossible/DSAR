#!/usr/bin/env python3
"""
Compile complete DSAR package from vendor reports.

This script aggregates outputs from individual vendor processors into a
complete DSAR response package including:
- Cover letter with GDPR-compliant language
- All vendor reports (Word documents)
- All data exports (JSON)
- Package manifest

Usage:
    python compile_package.py output/ "John Smith" \\
        --email john@company.com \\
        --request-date "15 January 2025" \\
        --company "Acme Corp" \\
        --dpo-name "Jane Doe" \\
        --dpo-email "dpo@acme.com"

Output:
    Creates a ZIP file containing:
    - 00_COVER_LETTER.docx
    - 01_Slack_DSAR_Report.docx
    - 02_HubSpot_DSAR_Report.docx
    - ...
    - json_exports/
        - Slack_export.json
        - HubSpot_export.json
        - ...
    - manifest.json (list of all included files)
"""

import sys
import os
import json
import zipfile
import shutil
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.docgen import create_cover_letter
from core.utils import safe_filename, ensure_output_dir, get_timestamp
from core.activity_log import log_event, get_activity_summary


def discover_vendor_reports(
    reports_dir: str,
    data_subject_name: str
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, int]]:
    """
    Discover all vendor reports in the output directory.

    Args:
        reports_dir: Directory containing vendor outputs
        data_subject_name: Name of the data subject (for filtering)

    Returns:
        Tuple of (docx_files, json_files, record_counts)
    """
    reports_path = Path(reports_dir)
    safe_name = safe_filename(data_subject_name)

    docx_files: Dict[str, str] = {}
    json_files: Dict[str, str] = {}
    record_counts: Dict[str, int] = {}

    # Find all DSAR report files
    for docx_file in reports_path.glob(f"*_DSAR_{safe_name}*.docx"):
        # Extract vendor name from filename
        # Format: {VENDOR}_DSAR_{safe_name}_{timestamp}.docx
        name = docx_file.name
        vendor = name.split('_DSAR_')[0]
        docx_files[vendor] = str(docx_file)

        # Find corresponding JSON
        json_pattern = f"{vendor}_DSAR_{safe_name}*.json"
        json_matches = list(reports_path.glob(json_pattern))
        if json_matches:
            json_file = json_matches[0]
            json_files[vendor] = str(json_file)

            # Read record count from JSON
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    record_counts[vendor] = data.get('record_count', 0)
            except (json.JSONDecodeError, KeyError):
                record_counts[vendor] = 0

    return docx_files, json_files, record_counts


def create_manifest(
    data_subject_name: str,
    data_subject_email: str,
    vendors_processed: Dict[str, int],
    files_included: List[str],
    package_created: str
) -> Dict:
    """
    Create a manifest describing the DSAR package contents.

    Args:
        data_subject_name: Name of the data subject
        data_subject_email: Email of the data subject
        vendors_processed: Dictionary of vendor -> record count
        files_included: List of files in the package
        package_created: Timestamp of package creation

    Returns:
        Manifest dictionary
    """
    return {
        'dsar_package': {
            'version': '1.0',
            'created': package_created,
            'generator': 'DSAR Toolkit',
        },
        'data_subject': {
            'name': data_subject_name,
            'email': data_subject_email,
        },
        'summary': {
            'vendors_searched': len(vendors_processed),
            'total_records': sum(vendors_processed.values()),
            'vendors': vendors_processed,
        },
        'files': files_included,
        'compliance': {
            'regulation': 'GDPR',
            'article': '15',
            'third_party_redaction': 'Article 15(4)',
        },
    }


def compile_package(
    reports_dir: str,
    data_subject_name: str,
    data_subject_email: str,
    request_date: str,
    company_name: str,
    dpo_name: str = "Data Protection Officer",
    dpo_email: str = None,
    company_address: str = None
) -> str:
    """
    Compile all vendor reports into a complete DSAR package.

    Args:
        reports_dir: Directory containing vendor outputs
        data_subject_name: Name of the data subject
        data_subject_email: Email of the data subject
        request_date: Date the DSAR was received
        company_name: Name of the responding organization
        dpo_name: Name of the DPO
        dpo_email: Email of the DPO
        company_address: Address of the organization

    Returns:
        Path to the created ZIP package
    """
    start_time = time.time()
    reports_path = Path(reports_dir)

    # Log compilation start
    log_event(
        'package_compilation_started',
        output_dir=reports_dir,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        reports_dir=reports_dir,
        company_name=company_name,
    )

    try:
        # 1. Discover all vendor reports
        print("Discovering vendor reports...")
        docx_files, json_files, record_counts = discover_vendor_reports(
            reports_dir, data_subject_name
        )

        if not docx_files:
            print(f"No vendor reports found for '{data_subject_name}' in {reports_dir}")
            print("Ensure vendor processors have been run first.")
            sys.exit(1)

        print(f"Found reports from {len(docx_files)} vendors")

        # 2. Create package directory
        timestamp = get_timestamp()
        safe_name = safe_filename(data_subject_name)
        package_name = f"DSAR_{safe_name}_{timestamp}"
        package_dir = reports_path / package_name
        package_dir.mkdir(exist_ok=True)

        json_export_dir = package_dir / "json_exports"
        json_export_dir.mkdir(exist_ok=True)

        files_included = []

        # 3. Generate cover letter
        print("Generating cover letter...")
        cover_doc = create_cover_letter(
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            vendors_processed=record_counts,
            request_date=request_date,
            company_name=company_name,
            dpo_name=dpo_name,
            dpo_email=dpo_email,
            company_address=company_address
        )

        cover_path = package_dir / "00_COVER_LETTER.docx"
        cover_doc.save(str(cover_path))
        files_included.append("00_COVER_LETTER.docx")

        # 4. Copy vendor reports (numbered by vendor name)
        print("Copying vendor reports...")
        sorted_vendors = sorted(docx_files.keys())
        for i, vendor in enumerate(sorted_vendors, start=1):
            # Copy Word document
            src_docx = docx_files[vendor]
            dest_docx = package_dir / f"{i:02d}_{vendor}_DSAR_Report.docx"
            shutil.copy(src_docx, dest_docx)
            files_included.append(dest_docx.name)

            # Copy JSON export if exists
            if vendor in json_files:
                src_json = json_files[vendor]
                dest_json = json_export_dir / f"{vendor}_export.json"
                shutil.copy(src_json, dest_json)
                files_included.append(f"json_exports/{vendor}_export.json")

            print(f"  Added {vendor}: {record_counts.get(vendor, 0):,} records")

        # 5. Create manifest with activity summary
        print("Creating manifest...")
        activity_summary = get_activity_summary(data_subject_name, reports_dir)

        manifest = create_manifest(
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            vendors_processed=record_counts,
            files_included=files_included,
            package_created=datetime.now().isoformat()
        )
        # Add processing activity to manifest
        manifest['processing_activity'] = activity_summary

        manifest_path = package_dir / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        files_included.append("manifest.json")

        # 6. Create ZIP package
        print("Creating ZIP package...")
        zip_path = reports_path / f"{package_name}.zip"

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file in package_dir.rglob('*'):
                if file.is_file():
                    arcname = file.relative_to(package_dir)
                    zf.write(file, arcname)

        # 7. Clean up temporary directory
        shutil.rmtree(package_dir)

        # 8. Print summary
        print("\n" + "=" * 60)
        print("DSAR PACKAGE COMPILED SUCCESSFULLY")
        print("=" * 60)
        print(f"\nData Subject: {data_subject_name}")
        print(f"Email: {data_subject_email}")
        print(f"\nVendors Included: {len(docx_files)}")
        for vendor in sorted_vendors:
            print(f"  • {vendor}: {record_counts.get(vendor, 0):,} records")
        print(f"\nTotal Records: {sum(record_counts.values()):,}")
        print(f"\nPackage: {zip_path}")
        print("=" * 60)

        # 9. Reminder about internal files
        internal_dir = reports_path / 'internal'
        if internal_dir.exists() and any(internal_dir.iterdir()):
            print("\n⚠️  REMINDER: Internal redaction keys are in:")
            print(f"   {internal_dir}")
            print("   DO NOT SEND THESE TO THE DATA SUBJECT!")

        # Log successful completion
        elapsed = time.time() - start_time
        log_event(
            'package_compilation_complete',
            output_dir=reports_dir,
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            status='success',
            vendors_included=sorted_vendors,
            total_records=sum(record_counts.values()),
            package_file=os.path.basename(str(zip_path)),
            execution_time_seconds=round(elapsed, 2),
        )

        return str(zip_path)

    except Exception as e:
        elapsed = time.time() - start_time
        log_event(
            'package_compilation_failed',
            output_dir=reports_dir,
            data_subject_name=data_subject_name,
            data_subject_email=data_subject_email,
            status='failure',
            error=str(e),
            execution_time_seconds=round(elapsed, 2),
        )
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Compile complete DSAR package from vendor reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python compile_package.py output/ "John Smith" \\
        --email john@company.com \\
        --request-date "15 January 2025" \\
        --company "Acme Corp"
        """
    )
    parser.add_argument(
        'reports_dir',
        help='Directory containing vendor reports'
    )
    parser.add_argument(
        'data_subject_name',
        help='Full name of the data subject'
    )
    parser.add_argument(
        '--email', '-e',
        required=True,
        help='Email address of the data subject'
    )
    parser.add_argument(
        '--request-date', '-d',
        required=True,
        help='Date the DSAR was received (e.g., "15 January 2025")'
    )
    parser.add_argument(
        '--company', '-c',
        required=True,
        help='Your company name'
    )
    parser.add_argument(
        '--dpo-name',
        default='Data Protection Officer',
        help='Name of the DPO (default: "Data Protection Officer")'
    )
    parser.add_argument(
        '--dpo-email',
        help='Email of the DPO'
    )
    parser.add_argument(
        '--company-address',
        help='Company address for the cover letter'
    )

    args = parser.parse_args()

    try:
        compile_package(
            reports_dir=args.reports_dir,
            data_subject_name=args.data_subject_name,
            data_subject_email=args.email,
            request_date=args.request_date,
            company_name=args.company,
            dpo_name=args.dpo_name,
            dpo_email=args.dpo_email,
            company_address=args.company_address
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
