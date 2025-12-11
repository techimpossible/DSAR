"""
Tests for GDPR compliance - Mention detection in DSAR scripts.

GDPR Article 15 requires that data subjects receive ALL personal data
concerning them, not just data they authored. This includes:
- Messages/content they created
- Messages/content where they are @mentioned
- Messages/content where their name appears
- Messages/content where their email appears
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class TestSlackMentionDetection:
    """Tests for Slack DSAR mention detection."""

    def test_detects_authored_messages(self):
        """Should detect messages authored by the data subject."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U12345', 'text': 'Hello world', 'ts': '1234567890.000'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'author' in records[0]['data_subject_relationship']

    def test_detects_at_mentions(self):
        """Should detect messages where data subject is @mentioned."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'Hey <@U12345> can you help?', 'ts': '1234567890.000'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert '@mentioned' in records[0]['data_subject_relationship']

    def test_detects_name_in_message(self):
        """Should detect messages where data subject's name appears."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'I talked to John Smith about this', 'ts': '1234567890.000'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'named' in records[0]['data_subject_relationship']

    def test_detects_email_in_message(self):
        """Should detect messages where data subject's email appears."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'Send it to john@example.com', 'ts': '1234567890.000'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'email referenced' in records[0]['data_subject_relationship']

    def test_ignores_unrelated_messages(self):
        """Should not include messages that don't relate to data subject."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'Random message about nothing', 'ts': '1234567890.000'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 0


class TestZendeskMentionDetection:
    """Tests for Zendesk DSAR mention detection."""

    def test_detects_requester_tickets(self):
        """Should detect tickets where data subject is the requester."""
        from support.zendesk_dsar import extract_records

        data = {
            'tickets': [
                {'id': 1, 'requester_id': 123, 'submitter_id': 999,
                 'subject': 'Help needed', 'description': 'Please help', 'created_at': '2024-01-01'},
            ],
            'comments': []
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'requester' in records[0]['data_subject_relationship']

    def test_detects_name_in_ticket(self):
        """Should detect tickets mentioning data subject's name."""
        from support.zendesk_dsar import extract_records

        data = {
            'tickets': [
                {'id': 1, 'requester_id': 999, 'submitter_id': 999,
                 'subject': 'Issue with John Smith account', 'description': 'Details here', 'created_at': '2024-01-01'},
            ],
            'comments': []
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'named' in records[0]['data_subject_relationship']

    def test_detects_name_in_comment(self):
        """Should detect comments mentioning data subject's name."""
        from support.zendesk_dsar import extract_records

        data = {
            'tickets': [{'id': 1, 'requester_id': 999, 'subject': 'Test'}],
            'comments': [
                {'ticket_id': 1, 'author_id': 999, 'body': 'John Smith confirmed this', 'created_at': '2024-01-01'},
            ]
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'named' in records[0]['data_subject_relationship']


class TestJiraMentionDetection:
    """Tests for Jira DSAR mention detection."""

    def test_detects_reporter_issues(self):
        """Should detect issues where data subject is the reporter."""
        from project_mgmt.jira_dsar import extract_records

        data = {
            'projects': [],
            'issues': [
                {
                    'key': 'TEST-1',
                    'fields': {
                        'reporter': {'accountId': '123'},
                        'assignee': None,
                        'creator': None,
                        'summary': 'Bug report',
                        'description': 'Details',
                        'created': '2024-01-01',
                        'status': {'name': 'Open'},
                        'project': {'id': '1'},
                        'comment': {'comments': []},
                        'worklog': {'worklogs': []},
                    }
                }
            ]
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'reporter' in records[0]['data_subject_relationship']

    def test_detects_jira_mention_format(self):
        """Should detect Jira @mentions in [~accountId] format."""
        from project_mgmt.jira_dsar import extract_records

        data = {
            'projects': [],
            'issues': [
                {
                    'key': 'TEST-1',
                    'fields': {
                        'reporter': {'accountId': '999'},
                        'assignee': None,
                        'creator': None,
                        'summary': 'Task',
                        'description': 'Please review [~123] feedback',
                        'created': '2024-01-01',
                        'status': {'name': 'Open'},
                        'project': {'id': '1'},
                        'comment': {'comments': []},
                        'worklog': {'worklogs': []},
                    }
                }
            ]
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert '@mentioned' in records[0]['data_subject_relationship']

    def test_detects_name_in_comment(self):
        """Should detect comments mentioning data subject's name."""
        from project_mgmt.jira_dsar import extract_records

        data = {
            'projects': [],
            'issues': [
                {
                    'key': 'TEST-1',
                    'fields': {
                        'reporter': {'accountId': '999'},
                        'assignee': None,
                        'creator': None,
                        'summary': 'Task',
                        'description': 'Details',
                        'created': '2024-01-01',
                        'status': {'name': 'Open'},
                        'project': {'id': '1'},
                        'comment': {
                            'comments': [
                                {'author': {'accountId': '999'}, 'body': 'John Smith will handle this', 'created': '2024-01-02'}
                            ]
                        },
                        'worklog': {'worklogs': []},
                    }
                }
            ]
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert records[0]['type'] == 'comment'
        assert 'named' in records[0]['data_subject_relationship']


class TestConfluenceMentionDetection:
    """Tests for Confluence DSAR mention detection."""

    def test_detects_creator_pages(self):
        """Should detect pages created by data subject."""
        from productivity.confluence_dsar import extract_records

        data = {
            'spaces': [],
            'pages': [
                {
                    'title': 'My Page',
                    'history': {'createdBy': {'accountId': '123'}, 'createdDate': '2024-01-01'},
                    'body': {'storage': {'value': 'Content'}},
                    'status': 'current',
                    'space': {'key': 'TEST'},
                }
            ],
            'comments': [],
            'blogposts': [],
            'attachments': [],
            'labels': [],
            'watches': [],
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'creator' in records[0]['data_subject_relationship']

    def test_detects_name_in_page_content(self):
        """Should detect pages mentioning data subject's name."""
        from productivity.confluence_dsar import extract_records

        data = {
            'spaces': [],
            'pages': [
                {
                    'title': 'Team Members',
                    'history': {'createdBy': {'accountId': '999'}, 'createdDate': '2024-01-01'},
                    'body': {'storage': {'value': 'Contact John Smith for details'}},
                    'status': 'current',
                    'space': {'key': 'TEST'},
                }
            ],
            'comments': [],
            'blogposts': [],
            'attachments': [],
            'labels': [],
            'watches': [],
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'named' in records[0]['data_subject_relationship']


class TestNotionMentionDetection:
    """Tests for Notion DSAR mention detection."""

    def test_detects_creator_pages(self):
        """Should detect pages created by data subject."""
        from productivity.notion_dsar import extract_records

        data = {
            'pages': [
                {
                    'title': 'My Page',
                    'created_by': {'id': '123'},
                    'last_edited_by': {},
                    'created_time': '2024-01-01',
                    'parent': {'type': 'workspace'},
                }
            ],
            'comments': [],
            'databases': [],
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'creator' in records[0]['data_subject_relationship']

    def test_detects_name_in_comment(self):
        """Should detect comments mentioning data subject's name."""
        from productivity.notion_dsar import extract_records

        data = {
            'pages': [],
            'comments': [
                {
                    'created_by': {'id': '999'},
                    'rich_text': [{'plain_text': 'Ask John Smith about this'}],
                    'created_time': '2024-01-01',
                }
            ],
            'databases': [],
        }

        records = extract_records(data, '123', 'John Smith', 'john@example.com')
        assert len(records) == 1
        assert 'named' in records[0]['data_subject_relationship']


class TestGDPRComplianceIntegration:
    """Integration tests for GDPR Article 15 compliance."""

    def test_slack_includes_all_related_content(self):
        """Slack should include authored, mentioned, and named content."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                # Authored by data subject
                {'user': 'U12345', 'text': 'I wrote this', 'ts': '1234567890.001'},
                # @mentions data subject
                {'user': 'U99999', 'text': 'Hey <@U12345> check this', 'ts': '1234567890.002'},
                # Names data subject
                {'user': 'U99999', 'text': 'John Smith said to do this', 'ts': '1234567890.003'},
                # Includes email
                {'user': 'U99999', 'text': 'Forward to john@example.com', 'ts': '1234567890.004'},
                # Unrelated message
                {'user': 'U99999', 'text': 'Random unrelated message', 'ts': '1234567890.005'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')

        # Should find 4 records (all except the unrelated one)
        assert len(records) == 4

        relationships = [r['data_subject_relationship'] for r in records]
        assert any('author' in r for r in relationships)
        assert any('@mentioned' in r for r in relationships)
        assert any('named' in r for r in relationships)
        assert any('email referenced' in r for r in relationships)

    def test_case_insensitive_name_matching(self):
        """Name matching should be case insensitive."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'JOHN SMITH said this', 'ts': '1234567890.001'},
                {'user': 'U99999', 'text': 'john smith mentioned that', 'ts': '1234567890.002'},
                {'user': 'U99999', 'text': 'JoHn SmItH confirmed', 'ts': '1234567890.003'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 3

    def test_case_insensitive_email_matching(self):
        """Email matching should be case insensitive."""
        from communication.slack_dsar import extract_records

        data = {
            'channels': [],
            'messages': [
                {'user': 'U99999', 'text': 'Send to JOHN@EXAMPLE.COM', 'ts': '1234567890.001'},
            ]
        }

        records = extract_records(data, 'U12345', 'John Smith', 'john@example.com')
        assert len(records) == 1
