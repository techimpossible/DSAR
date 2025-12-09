"""
Tests for core/redaction.py - RedactionEngine
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from core.redaction import RedactionEngine


class TestRedactionEngineInit:
    """Tests for RedactionEngine initialization."""

    def test_init_with_name_only(self):
        engine = RedactionEngine("John Smith")
        assert engine.ds_name == "John Smith"
        assert engine.ds_email is None
        assert "john" in engine.ds_name_parts
        assert "smith" in engine.ds_name_parts

    def test_init_with_name_and_email(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        assert engine.ds_name == "John Smith"
        assert engine.ds_email == "john@example.com"

    def test_init_filters_short_name_parts(self):
        engine = RedactionEngine("Jo Q Smith")
        assert "jo" not in engine.ds_name_parts  # Too short (2 chars)
        assert "smith" in engine.ds_name_parts


class TestIsDataSubject:
    """Tests for data subject identification."""

    def test_is_data_subject_by_exact_name(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        assert engine.is_data_subject(name="John Smith")

    def test_is_data_subject_by_email(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        assert engine.is_data_subject(email="john@example.com")

    def test_is_data_subject_case_insensitive(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        assert engine.is_data_subject(name="JOHN SMITH")
        assert engine.is_data_subject(email="JOHN@EXAMPLE.COM")

    def test_is_data_subject_by_partial_name(self):
        engine = RedactionEngine("John Smith")
        assert engine.is_data_subject(name="John")
        assert engine.is_data_subject(name="Smith")

    def test_is_not_data_subject(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        assert not engine.is_data_subject(name="Jane Doe")
        assert not engine.is_data_subject(email="jane@example.com")


class TestAddUser:
    """Tests for adding users to redaction map."""

    def test_add_user_creates_label(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_user("u123", "Jane Doe", "jane@example.com")
        assert label == "[User 1]"

    def test_add_data_subject_returns_none(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        label = engine.add_user("u123", "John Smith", "john@example.com")
        assert label is None

    def test_add_bot_creates_bot_label(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_user("b123", "Slack Bot", is_bot=True)
        assert label == "[Bot 1]"

    def test_add_user_increments_counter(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u1", "User One")
        engine.add_user("u2", "User Two")
        label = engine.add_user("u3", "User Three")
        assert label == "[User 3]"

    def test_add_user_maps_name_and_email(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u123", "Jane Doe", "jane@example.com")
        assert "Jane Doe" in engine.redaction_map
        assert "jane@example.com" in engine.redaction_map


class TestAddExternal:
    """Tests for adding external names."""

    def test_add_external_creates_label(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_external("External Person")
        assert label == "[External 1]"

    def test_add_external_maps_name(self):
        engine = RedactionEngine("John Smith")
        engine.add_external("External Person")
        assert "External Person" in engine.redaction_map


class TestAddEmail:
    """Tests for adding standalone emails."""

    def test_add_email_creates_label(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_email("someone@example.com")
        assert label == "[Email 1]"

    def test_add_data_subject_email_returns_none(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        label = engine.add_email("john@example.com")
        assert label is None

    def test_add_email_skips_invalid(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_email("not-an-email")
        assert label is None


class TestAddPhone:
    """Tests for adding phone numbers."""

    def test_add_phone_creates_label(self):
        engine = RedactionEngine("John Smith")
        label = engine.add_phone("+1-555-123-4567")
        assert label == "[Phone 1]"

    def test_add_phone_normalizes_number(self):
        engine = RedactionEngine("John Smith")
        engine.add_phone("+1 (555) 123-4567")
        assert "+15551234567" in engine.redaction_map


class TestRedact:
    """Tests for text redaction."""

    def test_redact_replaces_name(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u123", "Jane Doe")
        result = engine.redact("Hello Jane Doe!")
        assert result == "Hello [User 1]!"

    def test_redact_replaces_email(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u123", "Jane Doe", "jane@example.com")
        result = engine.redact("Contact jane@example.com")
        assert result == "Contact [User 1]"

    def test_redact_preserves_data_subject(self):
        engine = RedactionEngine("John Smith", "john@example.com")
        result = engine.redact("Hello John Smith at john@example.com")
        assert "John Smith" in result
        assert "john@example.com" in result

    def test_redact_longest_match_first(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u1", "Jane")
        engine.add_user("u2", "Jane Doe")
        result = engine.redact("Hello Jane Doe!")
        assert result == "Hello [User 2]!"

    def test_redact_case_insensitive(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u123", "Jane Doe")
        result = engine.redact("Hello JANE DOE!")
        assert result == "Hello [User 1]!"

    def test_redact_empty_string(self):
        engine = RedactionEngine("John Smith")
        result = engine.redact("")
        assert result == ""


class TestGetRedactionKey:
    """Tests for redaction key generation."""

    def test_get_redaction_key_returns_reverse_map(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u123", "Jane Doe", "jane@example.com")
        key = engine.get_redaction_key()
        assert "[User 1]" in key
        assert "Jane Doe" in key["[User 1]"] or "jane@example.com" in key["[User 1]"]


class TestGetStats:
    """Tests for statistics."""

    def test_get_stats_counts_users(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u1", "User One")
        engine.add_user("u2", "User Two")
        stats = engine.get_stats()
        assert stats['user'] == 2

    def test_get_stats_counts_bots(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("b1", "Bot One", is_bot=True)
        stats = engine.get_stats()
        assert stats['bot'] == 1

    def test_get_stats_counts_external(self):
        engine = RedactionEngine("John Smith")
        engine.add_external("External One")
        stats = engine.get_stats()
        assert stats['external'] == 1


class TestGetTotalRedactions:
    """Tests for total redaction count."""

    def test_get_total_redactions(self):
        engine = RedactionEngine("John Smith")
        engine.add_user("u1", "User One", "user1@example.com")  # 2 entries
        engine.add_external("External One")  # 1 entry
        total = engine.get_total_redactions()
        assert total >= 3
