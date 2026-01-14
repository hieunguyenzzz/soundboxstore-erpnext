#!/usr/bin/env python3
"""
SBS-64: Customer Migration Script
Imports customers from ALL Google Sheets (Sales, For Despatch, Despatched) into ERPNext.

This script:
1. Reads customers from Sales, For Despatch, and Despatched sheets
2. De-duplicates by email address
3. Creates Customer records with proper customer_type detection
4. Creates Address records for EVERY customer (100% coverage)
5. Creates Contact records with email and phone

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_customers.py
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

# Sheets to read customers from (in order of priority for data quality)
CUSTOMER_SHEETS = [
    'Despatched',     # Most complete data (orders already delivered)
    'For Despatch',   # Ready to ship orders
    'Sales',          # New orders
]

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
            'api_key': os.environ.get('ERPNEXT_API_KEY'),
            'api_secret': os.environ.get('ERPNEXT_API_SECRET'),
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
    if not config['erpnext']['api_key']:
        missing.append('ERPNEXT_API_KEY')
    if not config['erpnext']['api_secret']:
        missing.append('ERPNEXT_API_SECRET')
    if not config['google_sheets']['credentials']:
        missing.append('GOOGLE_SHEETS_CREDS')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL          - ERPNext server URL")
        print("  ERPNEXT_API_KEY      - ERPNext API key")
        print("  ERPNEXT_API_SECRET   - ERPNext API secret")
        print("  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR JSON content")
        print("\nOptional:")
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
    """ERPNext API Client using token authentication"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }
        self._verify_connection()

    def _verify_connection(self):
        """Verify API connection works"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed: {response.status_code}')
        user = response.json().get('message', 'Unknown')
        print(f'Connected to ERPNext at {self.url} as {user}')

    def create_customer(self, data):
        """Create a Customer in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Customer',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', error_data.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
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
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', error_data.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def create_contact(self, data):
        """Create a Contact in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Contact',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', error_data.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
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
            headers=self.headers,
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

    def get_customer_data(self, customer_id):
        """Get full customer data by ID"""
        response = self.session.get(
            f'{self.url}/api/resource/Customer/{customer_id}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def update_customer(self, customer_id, data):
        """Update an existing Customer in ERPNext"""
        response = self.session.put(
            f'{self.url}/api/resource/Customer/{customer_id}',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', error_data.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_address_for_customer(self, customer_id):
        """Check if customer already has an address linked"""
        response = self.session.get(
            f'{self.url}/api/resource/Address',
            params={
                'filters': json.dumps([
                    ['Dynamic Link', 'link_doctype', '=', 'Customer'],
                    ['Dynamic Link', 'link_name', '=', customer_id]
                ]),
                'fields': json.dumps(['name'])
            },
            headers=self.headers,
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

    def get_contact_for_customer(self, customer_id):
        """Check if customer already has a contact linked"""
        response = self.session.get(
            f'{self.url}/api/resource/Contact',
            params={
                'filters': json.dumps([
                    ['Dynamic Link', 'link_doctype', '=', 'Customer'],
                    ['Dynamic Link', 'link_name', '=', customer_id]
                ]),
                'fields': json.dumps(['name'])
            },
            headers=self.headers,
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


def normalize_country(country):
    """Normalize country name to ERPNext format"""
    if not country:
        return 'United Kingdom'

    country_upper = country.upper().strip()

    # Common mappings
    country_map = {
        'UK': 'United Kingdom',
        'GB': 'United Kingdom',
        'GREAT BRITAIN': 'United Kingdom',
        'ENGLAND': 'United Kingdom',
        'SCOTLAND': 'United Kingdom',
        'WALES': 'United Kingdom',
        'NORTHERN IRELAND': 'United Kingdom',
        'USA': 'United States',
        'US': 'United States',
        'AMERICA': 'United States',
        'UAE': 'United Arab Emirates',
        'HOLLAND': 'Netherlands',
        'ESPAÑA': 'Spain',
        'DEUTSCHLAND': 'Germany',
        'ITALIA': 'Italy',
        'FRANCE': 'France',
    }

    if country_upper in country_map:
        return country_map[country_upper]

    # Return original with title case if no mapping found
    return country.title()


def read_customers_from_sheet(service, spreadsheet_id, sheet_name):
    """Read customers from a single sheet.

    Column mapping (for all sheets):
    H (7):  Customer Name
    I (8):  Email
    J (9):  Phone
    K (10): Address Line 1
    L (11): City
    M (12): Pincode
    N (13): Country
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A2:N10000'
        ).execute()
    except Exception as e:
        print(f'   Warning: Could not read sheet "{sheet_name}": {e}')
        return []

    rows = result.get('values', [])
    customers = []

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        name = clean_text(get_col(7))
        email = clean_text(get_col(8)).lower()

        if not name:
            continue

        customer = {
            'customer_name': name[:140] if name else name,  # Truncate to ERPNext limit
            'email': email,
            'phone': clean_phone(get_col(9)),
            'address_line1': clean_text(get_col(10)),
            'city': clean_text(get_col(11)),
            'pincode': clean_text(get_col(12)),
            'country': normalize_country(get_col(13)),
            'source_sheet': sheet_name,
        }

        customers.append(customer)

    return customers


def consolidate_customers(all_customers):
    """Consolidate customers by email, keeping the most complete data.

    Priority: Despatched > For Despatch > Sales (based on data quality)
    """
    by_email = {}
    by_name = {}
    invalid_emails = []

    for cust in all_customers:
        email = cust.get('email', '')
        name = cust.get('customer_name', '')

        # Validate email if present
        if email and not is_valid_email(email):
            invalid_emails.append({'email': email, 'name': name})
            continue

        # Use email as primary key if valid, otherwise use name
        if email:
            if email in by_email:
                # Merge data - prefer existing (earlier priority) but fill gaps
                existing = by_email[email]
                for field in ['address_line1', 'city', 'pincode', 'phone']:
                    if not existing.get(field) and cust.get(field):
                        existing[field] = cust[field]
            else:
                by_email[email] = cust.copy()
        else:
            # No email - use name as key
            if name in by_name:
                existing = by_name[name]
                for field in ['address_line1', 'city', 'pincode', 'phone']:
                    if not existing.get(field) and cust.get(field):
                        existing[field] = cust[field]
            else:
                by_name[name] = cust.copy()

    # Combine both dictionaries
    unique_customers = list(by_email.values()) + list(by_name.values())

    return unique_customers, invalid_emails


def read_all_customers(service, spreadsheet_id):
    """Read and consolidate customers from all sheets"""
    all_customers = []
    sheet_counts = {}

    for sheet_name in CUSTOMER_SHEETS:
        print(f'   Reading from {sheet_name}...')
        customers = read_customers_from_sheet(service, spreadsheet_id, sheet_name)
        sheet_counts[sheet_name] = len(customers)
        all_customers.extend(customers)
        print(f'      Found {len(customers)} customer records')

    print(f'\n   Total records from all sheets: {len(all_customers)}')

    # Consolidate by email/name
    unique_customers, invalid_emails = consolidate_customers(all_customers)

    print(f'   After de-duplication: {len(unique_customers)} unique customers')
    if invalid_emails:
        print(f'   Skipped {len(invalid_emails)} records with invalid emails')

    return unique_customers, invalid_emails, sheet_counts


def import_customers(client, customers, batch_size=50):
    """Import customers into ERPNext with addresses and contacts"""
    results = {
        'created': 0,
        'updated': 0,
        'unchanged': 0,
        'failed': 0,
        'addresses_created': 0,
        'contacts_created': 0,
        'errors': []
    }

    total = len(customers)

    for i, cust in enumerate(customers):
        try:
            customer_type = 'Company' if is_company(cust['customer_name']) else 'Individual'

            customer_data = {
                'customer_name': cust['customer_name'],
                'customer_type': customer_type,
                'customer_group': 'All Customer Groups',
                'territory': 'All Territories',
            }

            existing_id = client.get_customer(cust['customer_name'])

            if existing_id:
                # Customer exists - check for address and contact
                address_exists = client.get_address_for_customer(existing_id)
                contact_exists = client.get_contact_for_customer(existing_id)

                # Create address if missing
                if not address_exists:
                    address_result = create_address_for_customer(client, existing_id, cust)
                    if address_result.get('success'):
                        results['addresses_created'] += 1

                # Create contact if missing and we have email
                if not contact_exists and cust.get('email'):
                    contact_result = create_contact_for_customer(client, existing_id, cust)
                    if contact_result.get('success'):
                        results['contacts_created'] += 1

                results['unchanged'] += 1
                print(f'[{i+1}/{total}] Exists: {cust["customer_name"]}')

            else:
                # Create new customer
                response = client.create_customer(customer_data)

                if response.get('data', {}).get('name'):
                    customer_id = response['data']['name']

                    # Always create address
                    address_result = create_address_for_customer(client, customer_id, cust)
                    if address_result.get('success'):
                        results['addresses_created'] += 1

                    # Create contact if we have email
                    if cust.get('email'):
                        contact_result = create_contact_for_customer(client, customer_id, cust)
                        if contact_result.get('success'):
                            results['contacts_created'] += 1

                    results['created'] += 1
                    print(f'[{i+1}/{total}] Created: {cust["customer_name"]} ({customer_type})')
                else:
                    error = response.get('error', 'Unknown error')
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


def create_address_for_customer(client, customer_id, cust):
    """Create address for a customer"""
    # Ensure we have at least some address data
    address_line1 = cust.get('address_line1') or cust.get('city') or 'Address not provided'
    city = cust.get('city') or 'City not specified'
    country = cust.get('country') or 'United Kingdom'

    address_data = {
        'address_title': cust['customer_name'][:100],  # Limit title length
        'address_type': 'Billing',
        'address_line1': address_line1,
        'city': city,
        'pincode': cust.get('pincode', ''),
        'country': country,
        'phone': cust.get('phone', ''),
        'email_id': cust.get('email', ''),
        'links': [{'link_doctype': 'Customer', 'link_name': customer_id}]
    }

    response = client.create_address(address_data)
    if response.get('data', {}).get('name'):
        return {'success': True, 'name': response['data']['name']}
    return {'error': response.get('error', 'Unknown error')}


def create_contact_for_customer(client, customer_id, cust):
    """Create contact for a customer"""
    email = cust.get('email', '')
    phone = cust.get('phone', '')
    name = cust.get('customer_name', '')

    # Parse first/last name
    name_parts = name.split(' ', 1)
    first_name = name_parts[0] if name_parts else name
    last_name = name_parts[1] if len(name_parts) > 1 else ''

    contact_data = {
        'first_name': first_name[:100],
        'last_name': last_name[:100] if last_name else None,
        'is_primary_contact': 1,
        'links': [{'link_doctype': 'Customer', 'link_name': customer_id}]
    }

    # Add email if present
    if email:
        contact_data['email_ids'] = [{'email_id': email, 'is_primary': 1}]

    # Add phone if present
    if phone:
        contact_data['phone_nos'] = [{'phone': phone, 'is_primary_phone': 1}]

    response = client.create_contact(contact_data)
    if response.get('data', {}).get('name'):
        return {'success': True, 'name': response['data']['name']}
    return {'error': response.get('error', 'Unknown error')}


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-64: Customer Migration (All Sheets)')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print('\n2. Connecting to ERPNext...')
    erpnext = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['api_key'],
        config['erpnext']['api_secret']
    )

    print('\n3. Reading customers from all sheets...')
    customers, invalid_emails, sheet_counts = read_all_customers(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )

    print(f'\n4. Importing {len(customers)} customers to ERPNext...')
    results = import_customers(erpnext, customers)

    print('\n' + '=' * 60)
    print('CUSTOMER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Customers Created:   {results["created"]}')
    print(f'Customers Existing:  {results["unchanged"]}')
    print(f'Customers Failed:    {results["failed"]}')
    print(f'Addresses Created:   {results["addresses_created"]}')
    print(f'Contacts Created:    {results["contacts_created"]}')

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
            'sheet_counts': sheet_counts,
            'created': results['created'],
            'unchanged': results['unchanged'],
            'failed': results['failed'],
            'addresses_created': results['addresses_created'],
            'contacts_created': results['contacts_created'],
            'invalid_emails': invalid_emails[:50],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
