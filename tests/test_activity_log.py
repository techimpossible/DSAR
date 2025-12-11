"""
Tests for core/activity_log.py - Activity logging
"""

import pytest
import sys
import os
import json
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from core.activity_log import (
    log_event,
    read_activity_log,
    get_activity_summary,
    get_activity_log_path,
    clear_activity_log,
)


class TestLogEvent:
    """Tests for log_event function."""

    def test_creates_log_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event('test_event', output_dir=tmpdir, test_field='value')
            log_path = get_activity_log_path(tmpdir)
            assert os.path.exists(log_path)

    def test_writes_json_line(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event('test_event', output_dir=tmpdir, vendor='TestVendor')
            log_path = get_activity_log_path(tmpdir)

            with open(log_path, 'r') as f:
                line = f.readline().strip()

            entry = json.loads(line)
            assert entry['event_type'] == 'test_event'
            assert entry['vendor'] == 'TestVendor'

    def test_includes_timestamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event('test_event', output_dir=tmpdir)

            entries = read_activity_log(tmpdir)
            assert len(entries) == 1
            assert 'timestamp' in entries[0]
            assert entries[0]['timestamp'].endswith('Z')

    def test_appends_multiple_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event('event_1', output_dir=tmpdir)
            log_event('event_2', output_dir=tmpdir)
            log_event('event_3', output_dir=tmpdir)

            entries = read_activity_log(tmpdir)
            assert len(entries) == 3
            assert entries[0]['event_type'] == 'event_1'
            assert entries[1]['event_type'] == 'event_2'
            assert entries[2]['event_type'] == 'event_3'

    def test_handles_complex_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event(
                'processing_complete',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='John Smith',
                records_processed=145,
                redaction_stats={'users': 42, 'bots': 12, 'external': 3},
                files_generated=['report.docx', 'export.json'],
            )

            entries = read_activity_log(tmpdir)
            assert len(entries) == 1
            assert entries[0]['records_processed'] == 145
            assert entries[0]['redaction_stats']['users'] == 42
            assert len(entries[0]['files_generated']) == 2

    def test_does_not_raise_on_write_error(self):
        # Log to non-writable path should not raise
        # Just silently fails (logging should never crash main process)
        try:
            log_event('test_event', output_dir='/nonexistent/path/unlikely')
        except Exception:
            pytest.fail("log_event raised an exception")


class TestReadActivityLog:
    """Tests for read_activity_log function."""

    def test_returns_empty_list_if_no_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            entries = read_activity_log(tmpdir)
            assert entries == []

    def test_reads_all_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(5):
                log_event(f'event_{i}', output_dir=tmpdir, index=i)

            entries = read_activity_log(tmpdir)
            assert len(entries) == 5
            for i, entry in enumerate(entries):
                assert entry['index'] == i

    def test_handles_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = get_activity_log_path(tmpdir)
            # Create empty file
            open(log_path, 'w').close()

            entries = read_activity_log(tmpdir)
            assert entries == []

    def test_skips_malformed_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = get_activity_log_path(tmpdir)
            os.makedirs(tmpdir, exist_ok=True)

            with open(log_path, 'w') as f:
                f.write('{"event_type": "valid"}\n')
                f.write('not valid json\n')
                f.write('{"event_type": "also_valid"}\n')

            entries = read_activity_log(tmpdir)
            assert len(entries) == 2
            assert entries[0]['event_type'] == 'valid'
            assert entries[1]['event_type'] == 'also_valid'


class TestGetActivitySummary:
    """Tests for get_activity_summary function."""

    def test_returns_empty_summary_for_no_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = get_activity_summary('John Smith', tmpdir)
            assert summary['vendors_processed'] == []
            assert summary['total_records'] == 0
            assert summary['all_successful'] is True

    def test_summarizes_successful_processing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Simulate vendor processing
            log_event(
                'processing_started',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='John Smith',
            )
            log_event(
                'processing_complete',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='John Smith',
                records_processed=145,
                execution_time_seconds=12.5,
            )
            log_event(
                'processing_started',
                output_dir=tmpdir,
                vendor='HubSpot',
                data_subject_name='John Smith',
            )
            log_event(
                'processing_complete',
                output_dir=tmpdir,
                vendor='HubSpot',
                data_subject_name='John Smith',
                records_processed=230,
                execution_time_seconds=8.3,
            )

            summary = get_activity_summary('John Smith', tmpdir)

            assert 'Slack' in summary['vendors_processed']
            assert 'HubSpot' in summary['vendors_processed']
            assert summary['total_records'] == 375
            assert summary['total_execution_time_seconds'] == 20.8
            assert summary['all_successful'] is True
            assert len(summary['vendors_failed']) == 0

    def test_tracks_failures(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event(
                'processing_failed',
                output_dir=tmpdir,
                vendor='Zendesk',
                data_subject_name='John Smith',
                error='File not found',
            )

            summary = get_activity_summary('John Smith', tmpdir)

            assert 'Zendesk' in summary['vendors_failed']
            assert summary['all_successful'] is False

    def test_filters_by_data_subject(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # John's processing
            log_event(
                'processing_complete',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='John Smith',
                records_processed=100,
            )
            # Jane's processing (different person)
            log_event(
                'processing_complete',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='Jane Doe',
                records_processed=200,
            )

            john_summary = get_activity_summary('John Smith', tmpdir)
            jane_summary = get_activity_summary('Jane Doe', tmpdir)

            assert john_summary['total_records'] == 100
            assert jane_summary['total_records'] == 200

    def test_extracts_processing_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event(
                'processing_started',
                output_dir=tmpdir,
                vendor='Slack',
                data_subject_name='John Smith',
            )

            summary = get_activity_summary('John Smith', tmpdir)

            # Should have a date in YYYY-MM-DD format
            assert summary['processing_date'] is not None
            assert len(summary['processing_date']) == 10


class TestClearActivityLog:
    """Tests for clear_activity_log function."""

    def test_removes_log_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_event('test_event', output_dir=tmpdir)
            log_path = get_activity_log_path(tmpdir)
            assert os.path.exists(log_path)

            clear_activity_log(tmpdir)
            assert not os.path.exists(log_path)

    def test_handles_nonexistent_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Should not raise
            clear_activity_log(tmpdir)


class TestThreadSafety:
    """Tests for thread-safe logging."""

    def test_concurrent_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            num_threads = 10
            events_per_thread = 100

            def write_events(thread_id):
                for i in range(events_per_thread):
                    log_event(
                        'concurrent_test',
                        output_dir=tmpdir,
                        thread_id=thread_id,
                        event_index=i,
                    )

            threads = []
            for t in range(num_threads):
                thread = threading.Thread(target=write_events, args=(t,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            # All events should be logged
            entries = read_activity_log(tmpdir)
            assert len(entries) == num_threads * events_per_thread

            # Each entry should be valid JSON (no corruption)
            for entry in entries:
                assert 'event_type' in entry
                assert entry['event_type'] == 'concurrent_test'
