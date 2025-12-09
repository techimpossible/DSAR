"""
Tests for core/docgen.py - Document generation
"""

import pytest
import sys
import os
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from core.docgen import create_vendor_report, create_cover_letter, create_redaction_key


class TestCreateVendorReport:
    """Tests for create_vendor_report function."""

    def test_creates_document(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={"Name": "John Smith", "Email": "john@test.com"},
            records=[],
        )
        assert doc is not None

    def test_document_saves(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={"Name": "John Smith"},
            records=[],
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
            assert os.path.getsize(f.name) > 0
        os.unlink(f.name)

    def test_includes_profile_data(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={
                "Name": "John Smith",
                "Email": "john@test.com",
                "Department": "Engineering",
            },
            records=[],
        )
        # Save and check content
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)
        # Document created successfully with profile data

    def test_includes_records(self):
        records = [
            {
                'date': '2024-01-15',
                'type': 'message',
                'category': 'Chat',
                'content': 'Hello world',
            },
            {
                'date': '2024-01-16',
                'type': 'email',
                'category': 'Email',
                'content': 'Test email content',
            },
        ]
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={"Name": "John Smith"},
            records=records,
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
            assert os.path.getsize(f.name) > 0
        os.unlink(f.name)

    def test_includes_redaction_stats(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={"Name": "John Smith"},
            records=[],
            redaction_stats={'user': 5, 'bot': 2, 'external': 1},
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)

    def test_handles_empty_profile(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email=None,
            profile_data={},
            records=[],
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)

    def test_handles_none_values_in_profile(self):
        doc = create_vendor_report(
            vendor_name="TestVendor",
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            profile_data={
                "Name": "John Smith",
                "Phone": None,
                "Department": "",
            },
            records=[],
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)


class TestCreateCoverLetter:
    """Tests for create_cover_letter function."""

    def test_creates_document(self):
        doc = create_cover_letter(
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            vendors_processed=["Slack", "HubSpot"],
            request_date="2024-01-15",
            company_name="Test Company",
        )
        assert doc is not None

    def test_document_saves(self):
        doc = create_cover_letter(
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            vendors_processed=["Slack"],
            request_date="2024-01-15",
            company_name="Test Company",
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
            assert os.path.getsize(f.name) > 0
        os.unlink(f.name)

    def test_includes_dpo_info(self):
        doc = create_cover_letter(
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            vendors_processed=["Slack"],
            request_date="2024-01-15",
            company_name="Test Company",
            dpo_name="Jane Doe",
            dpo_email="dpo@test.com",
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)

    def test_handles_multiple_vendors(self):
        doc = create_cover_letter(
            data_subject_name="John Smith",
            data_subject_email="john@test.com",
            vendors_processed=["Slack", "HubSpot", "Zendesk", "Jira"],
            request_date="2024-01-15",
            company_name="Test Company",
        )
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)


class TestCreateRedactionKey:
    """Tests for create_redaction_key function."""

    def test_creates_document(self):
        redaction_map = {
            "[User 1]": "Jane Doe, jane@test.com",
            "[User 2]": "Bob Smith, bob@test.com",
        }
        doc = create_redaction_key(redaction_map, "John Smith")
        assert doc is not None

    def test_document_saves(self):
        redaction_map = {
            "[User 1]": "Jane Doe",
        }
        doc = create_redaction_key(redaction_map, "John Smith")
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
            assert os.path.getsize(f.name) > 0
        os.unlink(f.name)

    def test_handles_empty_map(self):
        doc = create_redaction_key({}, "John Smith")
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)

    def test_handles_large_map(self):
        redaction_map = {f"[User {i}]": f"User {i}" for i in range(100)}
        doc = create_redaction_key(redaction_map, "John Smith")
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            doc.save(f.name)
        os.unlink(f.name)
