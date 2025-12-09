# DSAR Toolkit

A comprehensive toolkit for processing Data Subject Access Requests (DSARs) under GDPR Article 15 and CCPA.

## Features

- **26 Vendor Processors**: Pre-built processors for common SaaS platforms
- **GDPR-Compliant Redaction**: Automatic third-party data redaction per Article 15(4)
- **Word Report Generation**: Professional DSAR response documents
- **Package Compilation**: Combine multi-vendor reports with cover letters
- **Generic Fallbacks**: CSV and JSON processors for unlisted vendors

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd DSAR

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Supported Vendors

### Communication
- Slack

### CRM & Sales
- HubSpot
- Salesforce
- Pipedrive

### Support
- Zendesk
- Intercom
- Freshdesk

### Project Management
- Jira
- Asana
- Trello
- Monday.com

### HR & People
- CharlieHR
- BambooHR
- Greenhouse

### Productivity
- Notion
- Google Workspace
- Microsoft 365
- Confluence

### Developer Tools
- GitHub
- GitLab

### Marketing
- Mailchimp

### Finance
- Stripe

### Identity
- Okta

### Generic
- Generic JSON (fallback for any JSON export)
- Generic CSV (fallback for any CSV export)

## Quick Start

### Process a single vendor export

```bash
cd scripts/communication
python slack_dsar.py ~/exports/slack_export.zip "John Smith" --email john@company.com
```

### Process with additional redactions

```bash
python slack_dsar.py export.zip "John Smith" --email john@company.com --redact "External Contact" "Vendor Name"
```

### Compile multi-vendor package

```bash
cd scripts
python compile_package.py ./output "John Smith" --email john@company.com \
    --request-date "2025-01-15" --company "Your Company" \
    --dpo-name "Privacy Officer" --dpo-email dpo@company.com
```

## Directory Structure

```
DSAR/
├── scripts/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── redaction.py     # RedactionEngine class
│   │   ├── docgen.py        # Document generation
│   │   └── utils.py         # Utilities
│   ├── communication/
│   │   └── slack_dsar.py
│   ├── crm_sales/
│   │   ├── hubspot_dsar.py
│   │   ├── salesforce_dsar.py
│   │   └── pipedrive_dsar.py
│   ├── support/
│   │   ├── zendesk_dsar.py
│   │   ├── intercom_dsar.py
│   │   └── freshdesk_dsar.py
│   ├── project_mgmt/
│   │   ├── jira_dsar.py
│   │   ├── asana_dsar.py
│   │   ├── trello_dsar.py
│   │   └── monday_dsar.py
│   ├── hr_people/
│   │   ├── charliehr_dsar.py
│   │   ├── bamboohr_dsar.py
│   │   └── greenhouse_dsar.py
│   ├── productivity/
│   │   ├── notion_dsar.py
│   │   ├── google_workspace_dsar.py
│   │   ├── microsoft365_dsar.py
│   │   └── confluence_dsar.py
│   ├── dev_tools/
│   │   ├── github_dsar.py
│   │   └── gitlab_dsar.py
│   ├── marketing/
│   │   └── mailchimp_dsar.py
│   ├── finance/
│   │   └── stripe_dsar.py
│   ├── identity/
│   │   └── okta_dsar.py
│   ├── generic/
│   │   ├── generic_json_dsar.py
│   │   └── generic_csv_dsar.py
│   └── compile_package.py
├── tests/
│   ├── test_redaction.py
│   ├── test_docgen.py
│   └── test_utils.py
├── output/              # Generated reports (gitignored)
│   └── internal/        # Redaction keys (DO NOT SEND)
├── requirements.txt
├── pyproject.toml
└── README.md
```

## How It Works

### 1. Data Subject Identification
Each processor finds the data subject in the export by matching name and/or email.

### 2. Third-Party Redaction
Per GDPR Article 15(4), third-party personal data is replaced with placeholders:
- `[User 1]`, `[User 2]` - Other users
- `[Bot 1]`, `[Bot 2]` - Bot accounts
- `[External 1]` - External contacts
- `[Email 1]`, `[Phone 1]` - Standalone PII

### 3. Report Generation
- **Word Document**: Professional report with profile data and activity records
- **JSON Export**: Machine-readable data for integration
- **Redaction Key**: Internal document mapping placeholders to real names (DO NOT send to data subject)

### 4. Package Compilation
Combine reports from multiple vendors into a single ZIP with:
- Cover letter
- All vendor reports
- JSON manifest

## CLI Arguments

All processors accept these standard arguments:

| Argument | Description |
|----------|-------------|
| `export_path` | Path to the vendor export file |
| `data_subject_name` | Name of the data subject |
| `--email, -e` | Email of the data subject (recommended) |
| `--redact, -r` | Additional names to redact |
| `--output, -o` | Output directory (default: ./output) |

## Output Files

For each processor run:
- `{Vendor}_DSAR_{Name}_{Timestamp}.docx` - Word report
- `{Vendor}_DSAR_{Name}_{Timestamp}.json` - JSON export
- `internal/{Vendor}_REDACTION_KEY_{Name}_{Timestamp}.json` - Redaction mapping

## Ambiguous Match Handling

If multiple users match the data subject name, the processor will:
1. Raise an error listing all matches
2. Require the `--email` flag to disambiguate

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=scripts/core

# Run specific test file
pytest tests/test_redaction.py -v
```

## Security Notes

1. **Redaction Keys**: Files in `output/internal/` contain the mapping between placeholders and real names. These are for internal audit only and MUST NOT be sent to the data subject.

2. **Export Files**: Vendor exports may contain sensitive data. Handle according to your data protection policies.

3. **Output Files**: DSAR responses contain personal data. Transmit securely to the data subject.

## Adding New Vendors

1. Create a new processor in the appropriate category directory
2. Implement these functions:
   - `find_data_subject(data, name, email)` - Locate the data subject
   - `extract_users(data)` - Get all users for redaction
   - `extract_profile(data_subject)` - Get profile data
   - `extract_records(data, data_subject_id)` - Get activity records
   - `process(...)` - Main entry point
3. Use the generic processors as templates

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

[Contributing guidelines here]
