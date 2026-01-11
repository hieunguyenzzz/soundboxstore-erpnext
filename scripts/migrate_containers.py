#!/usr/bin/env python3
"""
SBS-51: Container Migration Script
Imports shipping containers from Google Sheets Container Status into ERPNext

This script:
1. Creates a custom Container doctype if it doesn't exist
2. Imports container data from Google Sheets using upsert logic

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

# Container DocType definition
CONTAINER_DOCTYPE = {
    "custom": 1,
    "name": "Container",
    "module": "Stock",
    "naming_rule": "By fieldname",
    "autoname": "field:container_name",
    "istable": 0,
    "editable_grid": 1,
    "track_changes": 1,
    "fields": [
        {"fieldname": "container_name", "fieldtype": "Data", "label": "Container Name", "reqd": 1, "unique": 1, "in_list_view": 1},
        {"fieldname": "container_no", "fieldtype": "Data", "label": "Container No.", "in_list_view": 1},
        {"fieldname": "capacity", "fieldtype": "Data", "label": "Capacity"},
        {"fieldname": "shipped_to", "fieldtype": "Link", "label": "Shipped To", "options": "Warehouse"},
        {"fieldname": "agent", "fieldtype": "Data", "label": "Agent"},
        {"fieldname": "provider", "fieldtype": "Data", "label": "Provider"},
        {"fieldname": "etd", "fieldtype": "Date", "label": "ETD"},
        {"fieldname": "eta", "fieldtype": "Date", "label": "ETA (Docks)"},
        {"fieldname": "status", "fieldtype": "Select", "label": "Status", "options": "\nIn Transit\nArrived\nCleared", "default": "In Transit"}
    ],
    "permissions": [
        {"role": "Stock Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
        {"role": "Stock User", "read": 1, "write": 1, "create": 1}
    ]
}


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

    def doctype_exists(self, doctype_name):
        """Check if a DocType exists"""
        response = self.session.get(
            f'{self.url}/api/resource/DocType/{doctype_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_doctype(self, doctype_def):
        """Create a custom DocType"""
        response = self.session.post(
            f'{self.url}/api/resource/DocType',
            json=doctype_def,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_container(self, container_name):
        """Get a Container by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Container/{container_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def create_container(self, data):
        """Create a Container in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Container',
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

    def update_container(self, container_name, data):
        """Update an existing Container in ERPNext"""
        response = self.session.put(
            f'{self.url}/api/resource/Container/{container_name}',
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

    def warehouse_exists(self, warehouse_name):
        """Check if warehouse exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200


def ensure_container_doctype(client):
    """Create Container custom doctype if it doesn't exist"""
    if client.doctype_exists('Container'):
        print('   Container doctype already exists')
        return True

    print('   Creating Container doctype...')
    response = client.create_doctype(CONTAINER_DOCTYPE)

    if response.get('data', {}).get('name'):
        print('   Container doctype created successfully')
        return True
    else:
        error = response.get('error', 'Unknown error')
        print(f'   ERROR: Failed to create Container doctype: {error}')
        return False


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


def parse_date(value):
    """Parse date from various formats to YYYY-MM-DD"""
    if not value:
        return None

    value = str(value).strip()
    if not value:
        return None

    formats = [
        '%d/%m/%Y',      # 25/12/2024
        '%d-%m-%Y',      # 25-12-2024
        '%Y-%m-%d',      # 2024-12-25
        '%d %b %Y',      # 25 Dec 2024
        '%d %B %Y',      # 25 December 2024
        '%m/%d/%Y',      # 12/25/2024 (US format)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def resolve_warehouse(client, warehouse_ref):
    """Try to resolve warehouse reference to actual warehouse name"""
    if not warehouse_ref:
        return None

    # Try with company suffix
    with_suffix = f"{warehouse_ref} - SBS"
    if client.warehouse_exists(with_suffix):
        return with_suffix

    # Try exact name
    if client.warehouse_exists(warehouse_ref):
        return warehouse_ref

    return None


def read_containers(service, spreadsheet_id):
    """Read and parse containers from Container Status sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Container Status!A2:V500'
    ).execute()

    rows = result.get('values', [])
    containers = []
    skipped = []

    for i, row in enumerate(rows):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        container_name = clean_text(get_col(0))

        if not container_name:
            continue

        # Skip header-like rows
        if container_name.upper() in ['CONTAINER NAME', 'NAME', 'CONTAINER']:
            continue

        container = {
            'container_name': container_name,
            'container_no': clean_text(get_col(1)),
            'capacity': clean_text(get_col(2)),
            'shipped_to_ref': clean_text(get_col(3)),  # Will be resolved later
            'agent': clean_text(get_col(4)),
            'provider': clean_text(get_col(5)),
            'etd': parse_date(get_col(6)),
            'eta': parse_date(get_col(7)),
        }

        containers.append(container)

    return containers, skipped


def import_containers(client, containers, batch_size=50):
    """Import containers into ERPNext using upsert (update if exists, create if not)"""
    results = {
        'created': 0,
        'updated': 0,
        'failed': 0,
        'warehouse_warnings': [],
        'errors': []
    }

    total = len(containers)

    for i, cont in enumerate(containers):
        try:
            # Resolve warehouse reference
            shipped_to = None
            if cont.get('shipped_to_ref'):
                shipped_to = resolve_warehouse(client, cont['shipped_to_ref'])
                if not shipped_to and cont['shipped_to_ref'] not in [w['ref'] for w in results['warehouse_warnings']]:
                    results['warehouse_warnings'].append({
                        'ref': cont['shipped_to_ref'],
                        'container': cont['container_name']
                    })

            container_data = {
                'container_name': cont['container_name'],
                'container_no': cont.get('container_no', ''),
                'capacity': cont.get('capacity', ''),
                'agent': cont.get('agent', ''),
                'provider': cont.get('provider', ''),
                'status': 'In Transit',
            }

            if shipped_to:
                container_data['shipped_to'] = shipped_to
            if cont.get('etd'):
                container_data['etd'] = cont['etd']
            if cont.get('eta'):
                container_data['eta'] = cont['eta']

            existing = client.get_container(cont['container_name'])

            if existing:
                # Update existing container
                response = client.update_container(cont['container_name'], container_data)
                if response.get('data', {}).get('name'):
                    results['updated'] += 1
                    print(f'[{i+1}/{total}] Updated: {cont["container_name"]}')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'container': cont['container_name'],
                        'error': f'Update failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Update failed: {cont["container_name"]} - {str(error)[:80]}')
            else:
                # Create new container
                response = client.create_container(container_data)
                if response.get('data', {}).get('name'):
                    results['created'] += 1
                    print(f'[{i+1}/{total}] Created: {cont["container_name"]}')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'container': cont['container_name'],
                        'error': f'Create failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Create failed: {cont["container_name"]} - {str(error)[:80]}')

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': 'Request timeout'
            })
            print(f'[{i+1}/{total}] Timeout: {cont["container_name"]}')

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': f'Network error: {type(e).__name__}'
            })
            print(f'[{i+1}/{total}] Network error: {cont["container_name"]} - {type(e).__name__}')

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': str(e)
            })
            print(f'[{i+1}/{total}] Error: {cont["container_name"]} - {str(e)[:80]}')

        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} containers, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-51: Container Migration')
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

    print('\n3. Ensuring Container doctype exists...')
    if not ensure_container_doctype(erpnext):
        print('ERROR: Cannot proceed without Container doctype')
        sys.exit(1)

    print('\n4. Reading containers from Container Status sheet...')
    containers, skipped = read_containers(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(containers)} containers')

    print(f'\n5. Importing {len(containers)} containers to ERPNext...')
    results = import_containers(erpnext, containers)

    print('\n' + '=' * 60)
    print('CONTAINER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Updated: {results["updated"]}')
    print(f'Failed:  {results["failed"]}')

    if results['warehouse_warnings']:
        print(f'\nWarning: {len(results["warehouse_warnings"])} warehouse references not found:')
        for w in results['warehouse_warnings'][:5]:
            print(f'  - "{w["ref"]}" (container: {w["container"]})')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["container"]}: {err["error"][:60]}')

    # Use tempfile with timestamp for unique report path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'container_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'total_containers': len(containers),
            'created': results['created'],
            'updated': results['updated'],
            'failed': results['failed'],
            'warehouse_warnings': results['warehouse_warnings'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
