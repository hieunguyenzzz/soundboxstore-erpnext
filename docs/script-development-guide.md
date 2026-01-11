# Script Development Guide

Guidelines for writing migration and sync scripts for SoundboxStore ERPNext.

## Environment Variables

**Never hardcode credentials.** Use environment variables:

```python
def get_config():
    config = {
        'erpnext': {
            'url': os.environ.get('ERPNEXT_URL'),
            'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
            'password': os.environ.get('ERPNEXT_PASSWORD'),
        },
        'google_sheets': {
            'credentials': os.environ.get('GOOGLE_SHEETS_CREDS'),  # File path OR JSON
            'spreadsheet_id': os.environ.get('SPREADSHEET_ID', 'default-id'),
        }
    }
    # Validate and exit if missing required vars
    missing = [k for k, v in [('ERPNEXT_URL', config['erpnext']['url']),
                               ('ERPNEXT_PASSWORD', config['erpnext']['password'])] if not v]
    if missing:
        print(f"ERROR: Missing: {', '.join(missing)}")
        sys.exit(1)
    return config
```

## HTTP Client Setup

### Always Use Timeouts and Retry Logic

```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 30

def create_session_with_retry():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
```

### ERPNext Client Template

```python
class ERPNextClient:
    def __init__(self, url, username, password):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self._login(username, password)

    def _login(self, username, password):
        resp = self.session.post(f'{self.url}/api/method/login',
                                  data={'usr': username, 'pwd': password},
                                  timeout=REQUEST_TIMEOUT)
        # SECURITY: Never expose response.text in errors
        if resp.status_code != 200 or 'Logged In' not in resp.text:
            raise Exception(f'Login failed (status {resp.status_code})')

    def create(self, doctype, data):
        resp = self.session.post(f'{self.url}/api/resource/{doctype}',
                                  json=data, timeout=REQUEST_TIMEOUT)
        if resp.status_code not in (200, 201):
            return {'error': f'HTTP {resp.status_code}'}
        try:
            return resp.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON'}

    def exists(self, doctype, name):
        resp = self.session.get(f'{self.url}/api/resource/{doctype}/{name}',
                                 timeout=REQUEST_TIMEOUT)
        return resp.status_code == 200
```

## Error Handling

```python
# SECURITY: Never expose response body in errors
raise Exception(f'Failed (status {resp.status_code})')  # Good
raise Exception(f'Failed: {resp.text}')                  # Bad - may leak secrets

# Always check HTTP status before parsing JSON
if resp.status_code not in (200, 201):
    return {'error': f'HTTP {resp.status_code}'}
return resp.json()

# Catch specific exceptions
try:
    response = client.create('Item', item)
except requests.exceptions.Timeout:
    errors.append({'item': name, 'error': 'Timeout'})
except requests.exceptions.RequestException as e:
    errors.append({'item': name, 'error': f'Network: {type(e).__name__}'})
```

## Data Validation

```python
import re

# Email validation
EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
def is_valid_email(email):
    return bool(email and EMAIL_RE.match(email))

# Price parsing (handles "$1,234.56" and edge cases like "1.234.56")
def clean_price(value):
    if not value:
        return 0.0
    cleaned = re.sub(r'[^\d.]', '', str(value))
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

# Company detection with word boundaries (avoids false positives)
COMPANY_RE = re.compile(r'\b(ltd|limited|inc|plc|llc|gmbh|university|council)\b', re.I)
def is_company(name):
    return bool(name and COMPANY_RE.search(name))
```

## Batch Processing

```python
def import_items(client, items, batch_size=50):
    results = {'created': 0, 'skipped': 0, 'failed': 0, 'errors': []}
    total = len(items)

    for i, item in enumerate(items):
        # Skip existing (makes script idempotent)
        if client.exists('Item', item['item_code']):
            results['skipped'] += 1
            continue

        resp = client.create('Item', item)
        if resp.get('data', {}).get('name'):
            results['created'] += 1
            print(f'[{i+1}/{total}] Created: {item["item_code"]}')
        else:
            results['failed'] += 1
            results['errors'].append({'item': item['item_code'], 'error': resp.get('error')})

        # Rate limiting
        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total}, pausing...')
            time.sleep(1)

    return results
```

## Reports and Exit Codes

```python
import tempfile
from datetime import datetime

# Save report with unique timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
report_path = os.path.join(tempfile.gettempdir(), f'migration_{timestamp}.json')
with open(report_path, 'w') as f:
    json.dump(results, f, indent=2)
print(f'Report: {report_path}')

# Exit code for CI/CD
sys.exit(1 if results['failed'] > 0 else 0)
```

## Testing

```bash
# Local ERPNext
ERPNEXT_URL="http://100.65.0.28:8080" \
ERPNEXT_PASSWORD="soundbox-admin-2026" \
GOOGLE_SHEETS_CREDS="~/.config/gcloud/service-accounts/sheets-api-service.json" \
python scripts/migrate_xxx.py

# Production
ERPNEXT_URL="https://erp.soundboxstore.com" \
ERPNEXT_PASSWORD="$ERPNEXT_ADMIN_PASSWORD" \
GOOGLE_SHEETS_CREDS="$GOOGLE_SHEETS_CREDS_JSON" \
python scripts/migrate_xxx.py
```

## Pre-PR Checklist

- [ ] Environment variables for all credentials
- [ ] Request timeouts (30s) on all HTTP calls
- [ ] Retry logic for transient failures
- [ ] HTTP status check before `.json()`
- [ ] No sensitive data in error messages
- [ ] Input validation (emails, prices)
- [ ] Skips existing records (idempotent)
- [ ] Rate limiting between batches
- [ ] Reports with unique timestamps
- [ ] Proper exit codes (0/1)
- [ ] Tested on local ERPNext first
