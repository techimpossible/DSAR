"""
Tests for core/utils.py - Utility functions
"""

import pytest
import sys
import os
import json
import tempfile
import csv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from core.utils import (
    safe_filename,
    format_date,
    get_timestamp,
    strip_html,
    load_json,
    save_json,
    load_csv,
    ensure_output_dir,
    validate_data_subject_match,
    AmbiguousMatchError,
)


class TestSafeFilename:
    """Tests for safe_filename function."""

    def test_replaces_spaces(self):
        result = safe_filename("John Smith")
        assert " " not in result

    def test_removes_special_chars(self):
        result = safe_filename("John/Smith@Company")
        assert "/" not in result
        assert "@" not in result

    def test_handles_unicode(self):
        result = safe_filename("José García")
        assert result  # Should not raise

    def test_truncates_long_names(self):
        long_name = "A" * 100
        result = safe_filename(long_name)
        assert len(result) <= 50


class TestFormatDate:
    """Tests for format_date function."""

    def test_formats_iso_date(self):
        result = format_date("2024-01-15T10:30:00Z")
        assert "2024" in result
        assert "01" in result or "Jan" in result

    def test_formats_date_only(self):
        result = format_date("2024-01-15")
        assert "2024" in result

    def test_handles_unix_timestamp(self):
        result = format_date(1705312200)  # Unix timestamp
        assert result  # Should not raise

    def test_returns_none_for_none(self):
        result = format_date(None)
        assert result is None

    def test_returns_none_for_empty(self):
        result = format_date("")
        assert result is None

    def test_returns_original_for_invalid(self):
        result = format_date("not-a-date")
        assert result == "not-a-date"


class TestGetTimestamp:
    """Tests for get_timestamp function."""

    def test_returns_string(self):
        result = get_timestamp()
        assert isinstance(result, str)

    def test_format_is_correct(self):
        result = get_timestamp()
        # Should be YYYYMMDD_HHMMSS format
        assert len(result) == 15
        assert "_" in result


class TestStripHtml:
    """Tests for strip_html function."""

    def test_removes_tags(self):
        result = strip_html("<p>Hello <b>World</b></p>")
        assert "<p>" not in result
        assert "<b>" not in result
        assert "Hello" in result
        assert "World" in result

    def test_handles_plain_text(self):
        result = strip_html("Plain text")
        assert result == "Plain text"

    def test_handles_empty(self):
        result = strip_html("")
        assert result == ""

    def test_handles_none(self):
        result = strip_html(None)
        assert result == ""

    def test_decodes_entities(self):
        result = strip_html("&amp; &lt; &gt;")
        assert "&" in result


class TestLoadJson:
    """Tests for load_json function."""

    def test_loads_valid_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"name": "test"}, f)
            f.flush()
            result = load_json(f.name)
        os.unlink(f.name)
        assert result == {"name": "test"}

    def test_raises_on_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json")
            f.flush()
            with pytest.raises(json.JSONDecodeError):
                load_json(f.name)
        os.unlink(f.name)

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_json("/nonexistent/file.json")


class TestSaveJson:
    """Tests for save_json function."""

    def test_saves_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name
        save_json({"name": "test"}, path)
        with open(path) as f:
            result = json.load(f)
        os.unlink(path)
        assert result == {"name": "test"}

    def test_creates_pretty_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name
        save_json({"name": "test"}, path)
        with open(path) as f:
            content = f.read()
        os.unlink(path)
        assert "\n" in content  # Pretty printed


class TestLoadCsv:
    """Tests for load_csv function."""

    def test_loads_valid_csv(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'email'])
            writer.writeheader()
            writer.writerow({'name': 'John', 'email': 'john@test.com'})
            f.flush()
            result = load_csv(f.name)
        os.unlink(f.name)
        assert len(result) == 1
        assert result[0]['name'] == 'John'

    def test_handles_empty_csv(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("")
            f.flush()
            result = load_csv(f.name)
        os.unlink(f.name)
        assert result == []


class TestEnsureOutputDir:
    """Tests for ensure_output_dir function."""

    def test_creates_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            new_dir = os.path.join(tmpdir, "new_subdir")
            ensure_output_dir(new_dir)
            assert os.path.isdir(new_dir)

    def test_handles_existing_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ensure_output_dir(tmpdir)  # Should not raise


class TestValidateDataSubjectMatch:
    """Tests for validate_data_subject_match function."""

    def test_returns_single_match(self):
        matches = [{'id': '1', 'name': 'John', 'email': 'john@test.com'}]
        result = validate_data_subject_match(matches, "John")
        assert result['id'] == '1'

    def test_raises_on_no_matches(self):
        with pytest.raises(ValueError, match="not found"):
            validate_data_subject_match([], "John")

    def test_raises_ambiguous_without_email(self):
        matches = [
            {'id': '1', 'name': 'John Smith', 'email': 'john1@test.com'},
            {'id': '2', 'name': 'John Smith', 'email': 'john2@test.com'},
        ]
        with pytest.raises(AmbiguousMatchError):
            validate_data_subject_match(matches, "John Smith")

    def test_disambiguates_with_email(self):
        matches = [
            {'id': '1', 'name': 'John Smith', 'email': 'john1@test.com'},
            {'id': '2', 'name': 'John Smith', 'email': 'john2@test.com'},
        ]
        result = validate_data_subject_match(matches, "John Smith", "john2@test.com")
        assert result['id'] == '2'

    def test_raises_if_email_no_match(self):
        matches = [
            {'id': '1', 'name': 'John Smith', 'email': 'john1@test.com'},
            {'id': '2', 'name': 'John Smith', 'email': 'john2@test.com'},
        ]
        with pytest.raises(AmbiguousMatchError):
            validate_data_subject_match(matches, "John Smith", "different@test.com")


class TestAmbiguousMatchError:
    """Tests for AmbiguousMatchError exception."""

    def test_contains_matches(self):
        matches = [{'id': '1'}, {'id': '2'}]
        error = AmbiguousMatchError("Test message", matches)
        assert error.matches == matches

    def test_message_is_correct(self):
        matches = [{'id': '1'}]
        error = AmbiguousMatchError("Test message", matches)
        assert str(error) == "Test message"
