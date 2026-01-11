#!/usr/bin/env python3
"""
SBS-51: Customer Migration Script
Imports customers from Google Sheets Despatched sheet into ERPNext

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)
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
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constants
REQUEST_TIMEOUT = 30  # seconds

# Company keywords with word boundary matching
COMPANY_KEYWORDS = [
    r'\bltd\b', r'\blimited\b', r'\binc\b', r'\bplc\b', r'\bllc\b',
    r'\bcorp\b', r'\bcorporation\b', r'\bacademy\b', r'\bschool\b',
    r'\buniversity\b', r'\bcollege\b', r'\bcouncil\b', r'\bgmbh\b',
    r'\bbv\b', r'\bab\b', r'\bsa\b', r'\bag\b', r'\bco\b', r'\bgroup\b',
    r'\bholdings?\b', r'\bpartners?\b', r'\bassociates?\b', r'\bconsulting\b',
    r'\bservices\b', r'\bsolutions\b', r'\benterprise\b', r'\bindustries\b',
    r'\btrust\b', r'\bfoundation\b', r'\bcharity\b', r'\bnhs\b', r'\bhospital\b'
]
COMPANY_PATTERN = re.compile('|'.join(COMPANY_KEYWORDS), re.IGNORECASE)

# Email validation pattern (RFC 5322 simplified)
EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)


def get_config():
    """Load configuration from environment variables"""
    config = {
        'erpnext': {
            'url': os.environ.get('ERPNEXT_URL'),
            'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
            'password': os.environ.get('ERPNEXT_PASSWORD'),
        },
        'google_sheets': {
            'scopes': ['https://www.googleapis.com/auth/spreadsheets.readonly'],
            'credentials': os.environ.get('GOOGLE_SHEETS_CREDS'),
            'spreadsheet_id': os.environ.get('SPREADSHEET_ID', '1NQA7DBzIryCjA0o0dxehLyGmxM8ZeOofpg3IENgtDmA'),
        }
    }

    missing = []
    if not config['erpnext']['url']:
        missing.append('ERPNEXT_URL')
    if not config['erpnext']['password']:
        missing.append('ERPNEXT_PASSWORD')
    if not config['google_sheets']['credentials']:
        missing.append('GOOGLE_SHEETS_CREDS')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL          - ERPNext server URL (e.g., https://erp.soundboxstore.com)")
        print("  ERPNEXT_PASSWORD     - ERPNext admin password")
        print("  GOOGLE_SHEETS_CREDS  - Path to service account JSON file OR JSON content")
        print("\nOptional:")
        print("  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)")
        print("  SPREADSHEET_ID       - Google Sheets ID (has default)")
        sys.exit(1)

    return config


def create_session_with_retry():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def is_valid_email(email):
    """Validate email format"""
    if not email:
        return False
    return bool(EMAIL_PATTERN.match(email))


def is_company(name):
    """Check if a name appears to be a company using word boundary matching"""
    if not name:
        return False
    return bool(COMPANY_PATTERN.search(name))


class ERPNextClient:
    """ERPNext API Client"""

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
        if response.status_code != 200:
            raise Exception(f'Login failed with status {response.status_code}')
        if 'Logged In' not in response.text:
            raise Exception('Login failed: Invalid credentials')
        print(f'Logged in to ERPNext at {self.url}')

    def create_customer(self, data):
        """Create a Customer in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Customer',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def create_address(self, data):
        """Create an Address in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Address',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_customer(self, customer_name):
        """Get customer by name, returns customer name (ID) if exists, None otherwise"""
        response = self.session.get(
            f'{self.url}/api/resource/Customer',
            params={'filters': json.dumps([['customer_name', '=', customer_name]])},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', [])
                if data:
                    return data[0].get('name')
            except json.JSONDecodeError:
                return None
        return None

    def update_customer(self, customer_id, data):
        """Update an existing Customer in ERPNext"""
        response = self.session.put(
            f'{self.url}/api/resource/Customer/{customer_id}',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}


def get_sheets_service(config):
    """Initialize Google Sheets API service"""
    creds_input = config['google_sheets']['credentials']

    if os.path.isfile(creds_input):
        creds = Credentials.from_service_account_file(
            creds_input,
            scopes=config['google_sheets']['scopes']
        )
    else:
        try:
            creds_info = json.loads(creds_input)
            creds = Credentials.from_service_account_info(
                creds_info,
                scopes=config['google_sheets']['scopes']
            )
        except json.JSONDecodeError:
            raise ValueError(
                "GOOGLE_SHEETS_CREDS must be either a valid file path or JSON content"
            )

    return build('sheets', 'v4', credentials=creds)


def clean_text(value):
    """Clean text field"""
    if not value:
        return ''
    return str(value).strip()


def clean_phone(value):
    """Clean phone number"""
    if not value:
        return ''
    cleaned = re.sub(r'[^\d+]', '', str(value))
    return cleaned if cleaned else ''


def read_customers(service, spreadsheet_id):
    """Read and parse unique customers from Despatched sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Despatched!A2:N10000'
    ).execute()

    rows = result.get('values', [])
    customers = {}
    invalid_emails = []

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        email = clean_text(get_col(8)).lower()
        name = clean_text(get_col(7))

        if not email or not name:
            continue

        # Validate email format
        if not is_valid_email(email):
            invalid_emails.append({'email': email, 'name': name})
            continue

        if email in customers:
            continue

        customer = {
            'customer_name': name,
            'email': email,
            'phone': clean_phone(get_col(9)),
            'address': clean_text(get_col(10)),
            'city': clean_text(get_col(11)),
            'pincode': clean_text(get_col(12)),
            'country': clean_text(get_col(13)) or 'United Kingdom',
        }

        customers[email] = customer

    return list(customers.values()), invalid_emails


def import_customers(client, customers, batch_size=50):
    """Import customers into ERPNext using upsert (update if exists, create if not)"""
    results = {
        'created': 0,
        'updated': 0,
        'failed': 0,
        'errors': []
    }

    total = len(customers)

    for i, cust in enumerate(customers):
        try:
            # Use word boundary matching for company detection
            customer_type = 'Company' if is_company(cust['customer_name']) else 'Individual'

            customer_data = {
                'customer_name': cust['customer_name'],
                'customer_type': customer_type,
                'customer_group': 'All Customer Groups',
                'territory': 'All Territories',
            }

            existing_id = client.get_customer(cust['customer_name'])

            if existing_id:
                # Update existing customer
                response = client.update_customer(existing_id, customer_data)
                if response.get('data', {}).get('name'):
                    results['updated'] += 1
                    print(f'[{i+1}/{total}] Updated: {cust["customer_name"]} ({customer_type})')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'customer': cust['customer_name'],
                        'error': f'Update failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Update failed: {cust["customer_name"]} - {str(error)[:80]}')
            else:
                # Create new customer
                response = client.create_customer(customer_data)

                if response.get('data', {}).get('name'):
                    customer_id = response['data']['name']

                    if cust['address'] or cust['city']:
                        address_data = {
                            'address_title': cust['customer_name'],
                            'address_type': 'Billing',
                            'address_line1': cust['address'] or cust['city'],
                            'city': cust['city'] or 'Not specified',
                            'pincode': cust['pincode'],
                            'country': cust['country'],
                            'phone': cust['phone'],
                            'email_id': cust['email'],
                            'links': [{'link_doctype': 'Customer', 'link_name': customer_id}]
                        }
                        client.create_address(address_data)

                    results['created'] += 1
                    print(f'[{i+1}/{total}] Created: {cust["customer_name"]} ({customer_type})')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'customer': cust['customer_name'],
                        'error': f'Create failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Create failed: {cust["customer_name"]} - {str(error)[:80]}')

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'customer': cust['customer_name'],
                'error': 'Request timeout'
            })
            print(f'[{i+1}/{total}] Timeout: {cust["customer_name"]}')

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'customer': cust['customer_name'],
                'error': f'Network error: {type(e).__name__}'
            })
            print(f'[{i+1}/{total}] Network error: {cust["customer_name"]} - {type(e).__name__}')

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'customer': cust['customer_name'],
                'error': str(e)
            })
            print(f'[{i+1}/{total}] Error: {cust["customer_name"]} - {str(e)[:80]}')

        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} customers, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-51: Customer Migration')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print('\n2. Connecting to ERPNext...')
    erpnext = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['username'],
        config['erpnext']['password']
    )

    print('\n3. Reading customers from Despatched sheet...')
    customers, invalid_emails = read_customers(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(customers)} unique customers')
    if invalid_emails:
        print(f'   Skipped {len(invalid_emails)} rows with invalid emails')

    print(f'\n4. Importing {len(customers)} customers to ERPNext...')
    results = import_customers(erpnext, customers)

    print('\n' + '=' * 60)
    print('CUSTOMER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Updated: {results["updated"]}')
    print(f'Failed:  {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["customer"]}: {err["error"][:60]}')

    # Use tempfile with timestamp for unique report path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'customer_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'total_customers': len(customers),
            'created': results['created'],
            'updated': results['updated'],
            'failed': results['failed'],
            'invalid_emails': invalid_emails[:50],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
