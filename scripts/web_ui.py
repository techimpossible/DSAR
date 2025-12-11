#!/usr/bin/env python3
"""
DSAR Toolkit Web UI

A minimalist Streamlit interface for processing DSAR requests.

Usage:
    cd /path/to/DSAR
    streamlit run scripts/web_ui.py

Features:
    - Upload vendor export files (ZIP, JSON, CSV, XLSX)
    - Select vendor processor from dropdown
    - Enter data subject name and email
    - Process exports with progress feedback
    - Download generated Word and JSON reports
    - Compile multiple vendor reports into final DSAR package
"""

import sys
import os
import importlib
import tempfile
from pathlib import Path
from datetime import datetime

# Add scripts directory to path for imports
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

import streamlit as st
from werkzeug.utils import secure_filename

# Page config - must be first Streamlit command
st.set_page_config(
    page_title="DSAR Toolkit",
    page_icon="🔒",
    layout="wide"
)

# Vendor processor mapping - module paths relative to scripts/
VENDOR_MAP = {
    # Communication
    "Slack": "communication.slack_dsar",
    # CRM & Sales
    "HubSpot": "crm_sales.hubspot_dsar",
    "Salesforce": "crm_sales.salesforce_dsar",
    "Pipedrive": "crm_sales.pipedrive_dsar",
    # Support
    "Zendesk": "support.zendesk_dsar",
    "Intercom": "support.intercom_dsar",
    "Freshdesk": "support.freshdesk_dsar",
    # Project Management
    "Jira": "project_mgmt.jira_dsar",
    "Asana": "project_mgmt.asana_dsar",
    "Monday": "project_mgmt.monday_dsar",
    "Trello": "project_mgmt.trello_dsar",
    # Developer Tools
    "GitHub": "dev_tools.github_dsar",
    "GitLab": "dev_tools.gitlab_dsar",
    # Productivity
    "Notion": "productivity.notion_dsar",
    "Confluence": "productivity.confluence_dsar",
    "Google Workspace": "productivity.google_workspace_dsar",
    "Microsoft 365": "productivity.microsoft365_dsar",
    # HR & People
    "BambooHR": "hr_people.bamboohr_dsar",
    "CharlieHR": "hr_people.charliehr_dsar",
    "Greenhouse": "hr_people.greenhouse_dsar",
    # Marketing
    "Mailchimp": "marketing.mailchimp_dsar",
    # Finance
    "Stripe": "finance.stripe_dsar",
    # Identity
    "Okta": "identity.okta_dsar",
    # Generic (fallback)
    "Generic JSON": "generic.generic_json_dsar",
    "Generic CSV": "generic.generic_csv_dsar",
}

# Group vendors by category for better UX
VENDOR_CATEGORIES = {
    "Communication": ["Slack"],
    "CRM & Sales": ["HubSpot", "Salesforce", "Pipedrive"],
    "Support": ["Zendesk", "Intercom", "Freshdesk"],
    "Project Management": ["Jira", "Asana", "Monday", "Trello"],
    "Developer Tools": ["GitHub", "GitLab"],
    "Productivity": ["Notion", "Confluence", "Google Workspace", "Microsoft 365"],
    "HR & People": ["BambooHR", "CharlieHR", "Greenhouse"],
    "Marketing": ["Mailchimp"],
    "Finance": ["Stripe"],
    "Identity": ["Okta"],
    "Generic": ["Generic JSON", "Generic CSV"],
}


def get_output_dir() -> Path:
    """Get the output directory path."""
    # Use project root's output directory
    project_root = SCRIPTS_DIR.parent
    output_dir = project_root / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir


def process_vendor_export(
    vendor: str,
    file_path: str,
    data_subject_name: str,
    data_subject_email: str = None,
    extra_redactions: list = None
) -> tuple:
    """
    Process a vendor export file.

    Args:
        vendor: Vendor name from VENDOR_MAP
        file_path: Path to the uploaded export file
        data_subject_name: Name of the data subject
        data_subject_email: Email of the data subject (optional)
        extra_redactions: Additional names to redact (optional)

    Returns:
        Tuple of (docx_path, json_path)
    """
    module_path = VENDOR_MAP[vendor]
    processor = importlib.import_module(module_path)

    output_dir = str(get_output_dir())

    return processor.process(
        export_path=file_path,
        data_subject_name=data_subject_name,
        data_subject_email=data_subject_email,
        extra_redactions=extra_redactions,
        output_dir=output_dir
    )


def main():
    """Main Streamlit application."""

    # Initialize session state
    if "processed_vendors" not in st.session_state:
        st.session_state.processed_vendors = []

    # Title and description
    st.title("🔒 DSAR Toolkit")
    st.markdown("Process Data Subject Access Requests across multiple vendors")

    # Sidebar for data subject and company info
    with st.sidebar:
        st.header("Data Subject Information")

        data_subject_name = st.text_input(
            "Full Name *",
            placeholder="John Smith",
            help="The name of the person requesting their data"
        )

        data_subject_email = st.text_input(
            "Email Address",
            placeholder="john@company.com",
            help="Email helps identify the correct person if multiple matches"
        )

        st.divider()

        st.header("Company Information")
        st.caption("Required for cover letter generation")

        company_name = st.text_input(
            "Company Name",
            placeholder="Acme Corp"
        )

        dpo_name = st.text_input(
            "DPO Name",
            value="Data Protection Officer"
        )

        dpo_email = st.text_input(
            "DPO Email",
            placeholder="dpo@company.com"
        )

        request_date = st.date_input(
            "Request Date",
            value=datetime.now()
        )

        # Clear session button
        st.divider()
        if st.button("🗑️ Clear Session", help="Reset all processed vendors"):
            st.session_state.processed_vendors = []
            st.rerun()

    # Main content area - two columns
    col1, col2 = st.columns([2, 1])

    with col1:
        st.header("Process Vendor Export")

        # Vendor selection with categories
        vendor = st.selectbox(
            "Select Vendor",
            options=list(VENDOR_MAP.keys()),
            help="Choose the vendor whose export you're uploading"
        )

        # Show which category this vendor belongs to
        for category, vendors in VENDOR_CATEGORIES.items():
            if vendor in vendors:
                st.caption(f"Category: {category}")
                break

        # File upload
        uploaded_file = st.file_uploader(
            f"Upload {vendor} Export",
            type=["zip", "json", "csv", "xlsx"],
            help="Upload the data export file from the vendor"
        )

        # Extra redactions
        extra_redactions_input = st.text_input(
            "Additional Names to Redact (optional)",
            placeholder="Jane Doe, Bob Wilson",
            help="Comma-separated list of other people's names to redact"
        )

        # Process button
        process_disabled = not (uploaded_file and data_subject_name)

        if st.button("🚀 Process DSAR", type="primary", disabled=process_disabled):
            if not data_subject_name:
                st.error("Please enter the data subject's name in the sidebar")
            elif not uploaded_file:
                st.error("Please upload an export file")
            else:
                try:
                    with st.spinner(f"Processing {vendor} export..."):
                        # Create temp directory for uploads
                        temp_dir = Path(tempfile.gettempdir()) / "dsar_uploads"
                        temp_dir.mkdir(exist_ok=True)

                        # Save uploaded file with secure filename
                        safe_name = secure_filename(uploaded_file.name)
                        temp_path = temp_dir / safe_name
                        temp_path.write_bytes(uploaded_file.read())

                        # Parse extra redactions
                        extra_redactions = None
                        if extra_redactions_input:
                            extra_redactions = [
                                r.strip()
                                for r in extra_redactions_input.split(",")
                                if r.strip()
                            ]

                        # Process the export
                        docx_path, json_path = process_vendor_export(
                            vendor=vendor,
                            file_path=str(temp_path),
                            data_subject_name=data_subject_name,
                            data_subject_email=data_subject_email or None,
                            extra_redactions=extra_redactions
                        )

                        # Track in session state
                        st.session_state.processed_vendors.append({
                            "vendor": vendor,
                            "docx_path": docx_path,
                            "json_path": json_path,
                            "timestamp": datetime.now().isoformat()
                        })

                        # Cleanup temp file
                        try:
                            temp_path.unlink(missing_ok=True)
                        except Exception:
                            pass  # Ignore cleanup errors

                    st.success(f"✅ Successfully processed {vendor}!")

                    # Download buttons
                    dl_col1, dl_col2 = st.columns(2)

                    with dl_col1:
                        with open(docx_path, "rb") as f:
                            st.download_button(
                                "📄 Download Word Report",
                                f,
                                file_name=Path(docx_path).name,
                                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                            )

                    with dl_col2:
                        with open(json_path, "rb") as f:
                            st.download_button(
                                "📊 Download JSON Export",
                                f,
                                file_name=Path(json_path).name,
                                mime="application/json"
                            )

                except Exception as e:
                    st.error(f"❌ Error processing {vendor}: {str(e)}")
                    with st.expander("Error Details"):
                        st.exception(e)

    with col2:
        st.header("Processed Vendors")

        if st.session_state.processed_vendors:
            # Show list of processed vendors
            for item in st.session_state.processed_vendors:
                st.markdown(f"✅ **{item['vendor']}**")

            st.divider()

            # Compile package section
            st.subheader("Compile DSAR Package")

            if not data_subject_name:
                st.warning("Enter data subject name to compile")
            elif not company_name:
                st.warning("Enter company name in sidebar to compile")
            else:
                if st.button("📦 Compile Package", type="secondary"):
                    try:
                        with st.spinner("Compiling DSAR package..."):
                            from compile_package import compile_package

                            output_dir = str(get_output_dir())

                            zip_path = compile_package(
                                reports_dir=output_dir,
                                data_subject_name=data_subject_name,
                                data_subject_email=data_subject_email or "",
                                request_date=request_date.strftime("%d %B %Y"),
                                company_name=company_name,
                                dpo_name=dpo_name,
                                dpo_email=dpo_email or None
                            )

                        st.success("✅ Package compiled!")

                        with open(zip_path, "rb") as f:
                            st.download_button(
                                "📥 Download DSAR Package",
                                f,
                                file_name=Path(zip_path).name,
                                mime="application/zip",
                                type="primary"
                            )

                    except Exception as e:
                        st.error(f"❌ Error compiling package: {str(e)}")
                        with st.expander("Error Details"):
                            st.exception(e)

            # Re-download previous reports
            st.divider()
            st.subheader("Download Previous Reports")

            for item in st.session_state.processed_vendors:
                with st.expander(f"{item['vendor']}"):
                    try:
                        with open(item['docx_path'], "rb") as f:
                            st.download_button(
                                f"📄 {item['vendor']} Word",
                                f,
                                file_name=Path(item['docx_path']).name,
                                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                key=f"docx_{item['vendor']}_{item['timestamp']}"
                            )
                    except FileNotFoundError:
                        st.warning("Report file not found")

        else:
            st.info("No vendors processed yet. Upload and process exports to get started.")

    # Footer
    st.divider()
    st.caption(
        "🔒 DSAR Toolkit | "
        "Files are processed locally and not uploaded to any external server"
    )


if __name__ == "__main__":
    main()
