# Script Development Guide

Guidelines for writing migration and sync scripts for SoundboxStore ERPNext.

## Table of Contents

- [Environment Variables](#environment-variables)
- [HTTP Client Setup](#http-client-setup)
- [Error Handling](#error-handling)
- [Data Validation](#data-validation)
- [Batch Processing](#batch-processing)
- [Logging and Reports](#logging-and-reports)
- [Testing](#testing)

---

## Environment Variables

**Never hardcode credentials.** All scripts must read configuration from environment variables.

### Required Variables

```python
import os
import sys

def get_config():
    """Load configuration from environment variables"""
    config = {
        'erpnext': {
            'url': os.environ.get('ERPNEXT_URL'),
            'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
            'password': os.environ.get('ERPNEXT_PASSWORD'),
        },
        'google_sheets': {
            'credentials': os.environ.get('GOOGLE_SHEETS_CREDS'),
            'spreadsheet_id': os.environ.get('SPREADSHEET_ID', 'default-id'),
        }
    }

    # Validate required config
    missing = []
    if not config['erpnext']['url']:
        missing.append('ERPNEXT_URL')
    if not config['erpnext']['password']:
        missing.append('ERPNEXT_PASSWORD')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    return config
```

### Flexible Credentials

Support both file paths and JSON content for credentials:

```python
import json

def load_credentials(creds_input):
    """Load credentials from file path or JSON string"""
    if os.path.isfile(creds_input):
        with open(creds_input) as f:
            return json.load(f)
    else:
        try:
            return json.loads(creds_input)
        except json.JSONDecodeError:
            raise ValueError("Credentials must be a file path or valid JSON")
```

---

## HTTP Client Setup

### Request Timeouts

**Always set timeouts** to prevent scripts from hanging indefinitely.

```python
REQUEST_TIMEOUT = 30  # seconds

response = session.post(url, json=data, timeout=REQUEST_TIMEOUT)
```

### Retry Logic

Use `HTTPAdapter` with `Retry` for transient failures:

```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session_with_retry():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,                      # Max retries
        backoff_factor=1,             # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
```

### ERPNext Client Template

```python
class ERPNextClient:
    """ERPNext API Client with best practices"""

    def __init__(self, url, username, password):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.login(username, password)

    def login(self, username, password):
        """Login and get session cookie"""
        response = self.session.post(
            f'{self.url}/api/method/login',
            data={'usr': username, 'pwd': password},
            timeout=REQUEST_TIMEOUT
        )
        # SECURITY: Never expose response.text in errors (may contain sensitive data)
        if response.status_code != 200:
            raise Exception(f'Login failed with status {response.status_code}')
        if 'Logged In' not in response.text:
            raise Exception('Login failed: Invalid credentials')

    def create_document(self, doctype, data):
        """Create a document in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/{doctype}',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        # Check status before parsing JSON
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def document_exists(self, doctype, name):
        """Check if a document exists"""
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200
```

---

## Error Handling

### Security: Never Expose Sensitive Data

```python
# BAD - May expose passwords or tokens in logs
raise Exception(f'Login failed: {response.text}')

# GOOD - Only expose safe information
raise Exception(f'Login failed with status {response.status_code}')
```

### Handle HTTP Errors Before JSON Parsing

```python
# BAD - Will crash if response isn't JSON
return response.json()

# GOOD - Check status first
if response.status_code not in (200, 201):
    return {'error': f'HTTP {response.status_code}'}
try:
    return response.json()
except json.JSONDecodeError:
    return {'error': 'Invalid JSON response'}
```

### Catch Specific Exceptions

```python
try:
    response = client.create_item(item)
    # ... handle success
except requests.exceptions.Timeout:
    # Handle timeout specifically
    results['errors'].append({'item': item['name'], 'error': 'Request timeout'})
except requests.exceptions.RequestException as e:
    # Handle network errors
    results['errors'].append({'item': item['name'], 'error': f'Network error: {type(e).__name__}'})
except Exception as e:
    # Catch-all for unexpected errors
    results['errors'].append({'item': item['name'], 'error': str(e)})
```

---

## Data Validation

### Email Validation

```python
import re

EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

def is_valid_email(email):
    """Validate email format"""
    if not email:
        return False
    return bool(EMAIL_PATTERN.match(email))
```

### Phone Number Cleaning

```python
def clean_phone(value):
    """Clean and validate phone number"""
    if not value:
        return ''
    # Keep only digits and leading +
    cleaned = re.sub(r'[^\d+]', '', str(value))
    # Basic length validation (E.164 standard)
    if len(cleaned) < 7 or len(cleaned) > 15:
        return ''
    return cleaned
```

### Price Parsing

Handle edge cases like multiple decimal points:

```python
def clean_price(value):
    """Convert price string to float: '$1,486.00' -> 1486.00"""
    if not value:
        return 0.0
    # Remove all non-digit and non-period characters
    cleaned = re.sub(r'[^\d.]', '', str(value))
    if not cleaned:
        return 0.0

    # Handle multiple decimal points by keeping only the last one
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]

    try:
        return float(cleaned)
    except ValueError:
        return 0.0
```

### Company Detection with Word Boundaries

Use word boundary matching to avoid false positives:

```python
COMPANY_KEYWORDS = [
    r'\bltd\b', r'\blimited\b', r'\binc\b', r'\bplc\b', r'\bllc\b',
    r'\bcorp\b', r'\bgmbh\b', r'\bab\b', r'\bsa\b', r'\bag\b',
    r'\buniversity\b', r'\bcollege\b', r'\bcouncil\b', r'\btrust\b'
]
COMPANY_PATTERN = re.compile('|'.join(COMPANY_KEYWORDS), re.IGNORECASE)

def is_company(name):
    """Check if name appears to be a company"""
    if not name:
        return False
    return bool(COMPANY_PATTERN.search(name))

# Example:
# is_company("ABC Ltd") -> True
# is_company("Colin Ltd") -> True (still matches, but word boundary helps)
# is_company("Altdorf") -> False (no word boundary match)
```

---

## Batch Processing

### Rate Limiting

Add pauses between batches to avoid overwhelming the server:

```python
def import_items(client, items, batch_size=50):
    """Import items with rate limiting"""
    total = len(items)

    for i, item in enumerate(items):
        # ... process item ...

        # Pause every batch_size items
        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} items, pausing...')
            time.sleep(1)

    return results
```

### Skip Existing Records

Check before creating to make scripts idempotent:

```python
if client.document_exists('Item', item['item_code']):
    print(f'Skipping (exists): {item["item_code"]}')
    results['skipped'] += 1
    continue
```

### Track Progress

```python
results = {
    'created': 0,
    'skipped': 0,
    'failed': 0,
    'errors': []
}

# After each operation:
if success:
    results['created'] += 1
    print(f'[{i+1}/{total}] Created: {item["name"]}')
else:
    results['failed'] += 1
    results['errors'].append({'item': item['name'], 'error': error_msg})
    print(f'[{i+1}/{total}] Failed: {item["name"]} - {error_msg[:80]}')
```

---

## Logging and Reports

### Report Files

Use unique filenames with timestamps:

```python
import tempfile
from datetime import datetime

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
report_path = os.path.join(tempfile.gettempdir(), f'migration_report_{timestamp}.json')

with open(report_path, 'w') as f:
    json.dump({
        'total': len(items),
        'created': results['created'],
        'skipped': results['skipped'],
        'failed': results['failed'],
        'errors': results['errors']
    }, f, indent=2)

print(f'Report saved to: {report_path}')
```

### Exit Codes

Return proper exit codes for CI/CD integration:

```python
# Exit with error if any failures
sys.exit(1 if results['failed'] > 0 else 0)
```

### Console Output

Print clear progress and summary:

```python
def main():
    print('=' * 60)
    print('SBS-XX: Migration Script Name')
    print('=' * 60)

    # ... run migration ...

    print('\n' + '=' * 60)
    print('MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Skipped: {results["skipped"]}')
    print(f'Failed:  {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["item"]}: {err["error"][:80]}')
```

---

## Testing

### Local Testing

Always test against the local ERPNext instance first:

```bash
# Local ERPNext
ERPNEXT_URL="http://100.65.0.28:8080" \
ERPNEXT_PASSWORD="soundbox-admin-2026" \
GOOGLE_SHEETS_CREDS="~/.config/gcloud/service-accounts/sheets-api-service.json" \
python scripts/migrate_xxx.py
```

### Production

```bash
# Production ERPNext
ERPNEXT_URL="https://erp.soundboxstore.com" \
ERPNEXT_PASSWORD="$ERPNEXT_ADMIN_PASSWORD" \
GOOGLE_SHEETS_CREDS="$GOOGLE_SHEETS_CREDS_JSON" \
python scripts/migrate_xxx.py
```

### Truncate Test Data

Before re-testing, truncate existing data:

```python
def truncate_doctype(client, doctype):
    """Delete all documents of a doctype"""
    response = client.session.get(
        f'{client.url}/api/resource/{doctype}?limit_page_length=0'
    )
    docs = response.json().get('data', [])

    for doc in docs:
        client.session.delete(f'{client.url}/api/resource/{doctype}/{doc["name"]}')
```

---

## Script Template

Use this template for new migration scripts:

```python
#!/usr/bin/env python3
"""
SBS-XX: [Description]
[Detailed description of what this script does]

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR JSON content
"""

import os
import re
import json
import time
import sys
import tempfile
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 30


def get_config():
    # ... (see Environment Variables section)
    pass


def create_session_with_retry():
    # ... (see HTTP Client Setup section)
    pass


class ERPNextClient:
    # ... (see HTTP Client Setup section)
    pass


def main():
    print('=' * 60)
    print('SBS-XX: Script Name')
    print('=' * 60)

    config = get_config()

    # ... your migration logic ...

    print('\n' + '=' * 60)
    print('COMPLETE')
    print('=' * 60)

    sys.exit(0)


if __name__ == '__main__':
    main()
```

---

## Checklist

Before submitting a migration script PR:

- [ ] Uses environment variables for all credentials
- [ ] Has request timeouts on all HTTP calls
- [ ] Has retry logic for transient failures
- [ ] Handles HTTP errors before parsing JSON
- [ ] Never exposes sensitive data in error messages
- [ ] Validates input data (emails, phones, prices)
- [ ] Skips existing records (idempotent)
- [ ] Has rate limiting between batches
- [ ] Saves reports with unique timestamps
- [ ] Returns proper exit codes
- [ ] Tested on local ERPNext first
- [ ] Has clear progress output
- [ ] Includes docstring with env var documentation
