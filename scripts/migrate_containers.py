#!/usr/bin/env python3
"""
SBS-64: Container Migration Script
Imports shipping containers from Google Sheets Container Status into ERPNext

This script:
1. Creates a custom Container doctype if it doesn't exist
2. Imports container data from Google Sheets with proper status logic
3. Determines status based on date fields (ETD, ETA, arrival notice, booked to warehouse)

Status Logic:
- Pre-Departure: No ETD set
- In Transit: ETD set, no arrival notice
- At Port: Arrival notice set, not yet booked to warehouse
- Arrived: Booked to warehouse date set

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_containers.py
  python scripts/migrate_containers.py --update-doctype  # Update DocType fields
  python scripts/migrate_containers.py --doctype-only    # Only create DocType
"""

import os
import re
import json
import time
import sys
import tempfile
import argparse
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constants
REQUEST_TIMEOUT = 30  # seconds

# Container DocType definition with proper status options
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
        # Basic Info Section
        {"fieldname": "container_name", "fieldtype": "Data", "label": "Container Name", "reqd": 1, "unique": 1, "in_list_view": 1},
        {"fieldname": "container_no", "fieldtype": "Data", "label": "Container No.", "in_list_view": 1},
        {"fieldname": "capacity", "fieldtype": "Data", "label": "Capacity"},
        {"fieldname": "shipped_to", "fieldtype": "Link", "label": "Shipped To", "options": "Warehouse"},
        {"fieldname": "agent", "fieldtype": "Data", "label": "Agent"},
        {"fieldname": "provider", "fieldtype": "Data", "label": "Provider"},
        {"fieldname": "location", "fieldtype": "Select", "label": "Location", "options": "\nUK\nSPAIN\nUS"},

        # Dates Section
        {"fieldname": "section_dates", "fieldtype": "Section Break", "label": "Dates"},
        {"fieldname": "etd", "fieldtype": "Date", "label": "ETD", "description": "Estimated Time of Departure"},
        {"fieldname": "eta", "fieldtype": "Date", "label": "ETA (Docks)", "description": "Initial estimated arrival at docks"},
        {"fieldname": "column_break_dates", "fieldtype": "Column Break"},
        {"fieldname": "actual_docking", "fieldtype": "Date", "label": "Actual Docking Date", "description": "Latest docking ETA or actual arrival"},
        {"fieldname": "arrival_notice_date", "fieldtype": "Date", "label": "Arrival Notice Date", "description": "When arrival notice was received"},
        {"fieldname": "warehouse_receipt_date", "fieldtype": "Date", "label": "Warehouse Receipt Date", "description": "BOOKED TO WAREHOUSE - when stock entered warehouse"},

        # Quantities Section
        {"fieldname": "section_quantities", "fieldtype": "Section Break", "label": "Quantities"},
        {"fieldname": "expected_qty", "fieldtype": "Float", "label": "Expected Qty", "description": "QTY DUE IN", "precision": 2},
        {"fieldname": "allocated_qty", "fieldtype": "Float", "label": "Allocated Qty", "description": "QTY SOLD", "precision": 2},
        {"fieldname": "column_break_qty", "fieldtype": "Column Break"},
        {"fieldname": "remaining_qty", "fieldtype": "Float", "label": "Remaining Qty", "description": "Expected - Allocated", "precision": 2, "read_only": 1},
        {"fieldname": "percent_sold", "fieldtype": "Percent", "label": "% Sold", "description": "Percentage of expected qty that is allocated", "read_only": 1},

        # Status Section
        {"fieldname": "section_status", "fieldtype": "Section Break", "label": "Status"},
        {"fieldname": "status", "fieldtype": "Select", "label": "Status",
         "options": "\nPre-Departure\nIn Transit\nAt Port\nArrived",
         "default": "Pre-Departure", "in_list_view": 1, "in_standard_filter": 1}
    ],
    "permissions": [
        {"role": "Stock Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
        {"role": "Stock User", "read": 1, "write": 1, "create": 1}
    ]
}


def get_config(require_google_sheets=True):
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
    if require_google_sheets and not config['google_sheets']['credentials']:
        missing.append('GOOGLE_SHEETS_CREDS')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL          - ERPNext server URL")
        print("  ERPNEXT_API_KEY      - ERPNext API key")
        print("  ERPNEXT_API_SECRET   - ERPNext API secret")
        if require_google_sheets:
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

    def doctype_exists(self, doctype_name):
        """Check if a DocType exists"""
        response = self.session.get(
            f'{self.url}/api/resource/DocType/{doctype_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def get_doctype(self, doctype_name):
        """Get a DocType definition"""
        response = self.session.get(
            f'{self.url}/api/resource/DocType/{doctype_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def create_doctype(self, doctype_def):
        """Create a custom DocType"""
        response = self.session.post(
            f'{self.url}/api/resource/DocType',
            json=doctype_def,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def update_doctype(self, doctype_name, doctype_def):
        """Update an existing DocType"""
        response = self.session.put(
            f'{self.url}/api/resource/DocType/{doctype_name}',
            json=doctype_def,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}', 'response': response.text}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_container(self, container_name):
        """Get a Container by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Container/{container_name}',
            headers=self.headers,
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
            headers=self.headers,
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
            headers=self.headers,
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
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200


def ensure_container_doctype(client, force_update=False):
    """Create or update Container custom doctype"""
    if client.doctype_exists('Container'):
        if not force_update:
            print('   Container doctype already exists (use --update-doctype to update)')
            return True

        print('   Updating Container doctype fields...')
        existing = client.get_doctype('Container')
        if not existing:
            print('   ERROR: Could not fetch existing Container doctype')
            return False

        existing_fieldnames = {f['fieldname'] for f in existing.get('fields', [])}
        new_fieldnames = {f['fieldname'] for f in CONTAINER_DOCTYPE['fields']}
        fields_to_add = new_fieldnames - existing_fieldnames

        if fields_to_add:
            print(f'   Adding new fields: {", ".join(sorted(fields_to_add))}')

        update_payload = {'fields': CONTAINER_DOCTYPE['fields']}
        response = client.update_doctype('Container', update_payload)

        if response.get('data', {}).get('name'):
            print('   Container doctype updated successfully')
            return True
        else:
            error = response.get('error', 'Unknown error')
            print(f'   ERROR: Failed to update Container doctype: {error}')
            return False

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
        '%d-%b-%Y',      # 25-Dec-2024
        '%d-%b-%y',      # 25-Dec-24
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def parse_float(value):
    """Parse float value"""
    if not value:
        return 0.0
    try:
        cleaned = re.sub(r'[,\s]', '', str(value).strip())
        return float(cleaned)
    except ValueError:
        return 0.0


def determine_status(etd, eta, arrival_notice, warehouse_receipt):
    """
    Determine container status based on date fields.

    Status Logic:
    - Arrived: warehouse_receipt_date is set (container has been booked to warehouse)
    - At Port: arrival_notice_date is set (arrived at port, awaiting warehouse)
    - In Transit: etd is set (departed from supplier)
    - Pre-Departure: no etd (still at supplier)
    """
    if warehouse_receipt:
        return 'Arrived'
    if arrival_notice:
        return 'At Port'
    if etd:
        return 'In Transit'
    return 'Pre-Departure'


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
    """
    Read and parse containers from Container Status sheet

    Column mapping (0-indexed):
    A (0): CONTAINER - container_name
    B (1): Container No. - container_no
    C (2): Capacity - capacity
    D (3): Shipped to - shipped_to
    E (4): Agent - agent
    F (5): Provider - provider
    G (6): ETD - etd
    H (7): ETA (docks) - eta
    I (8): Latest docking ETA - actual_docking
    J (9): ARRIVAL NOTICE - arrival_notice_date
    K (10): BOOKED TO WAREHOUSE - warehouse_receipt_date
    L (11): QTY DUE IN - expected_qty
    M (12): QTY SOLD - allocated_qty
    ...
    V (21): LOCATION - location (UK/SPAIN/US)
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Container Status!A2:W500'
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
        if container_name.upper() in ['CONTAINER NAME', 'NAME', 'CONTAINER', 'CONTAINER ']:
            continue

        # Parse all date fields
        etd = parse_date(get_col(6))
        eta = parse_date(get_col(7))
        actual_docking = parse_date(get_col(8))
        arrival_notice = parse_date(get_col(9))
        warehouse_receipt = parse_date(get_col(10))

        # Determine status based on dates
        status = determine_status(etd, eta, arrival_notice, warehouse_receipt)

        container = {
            'container_name': container_name,
            'container_no': clean_text(get_col(1)),
            'capacity': clean_text(get_col(2)),
            'shipped_to_ref': clean_text(get_col(3)),
            'agent': clean_text(get_col(4)),
            'provider': clean_text(get_col(5)),
            'etd': etd,
            'eta': eta,
            'actual_docking': actual_docking,
            'arrival_notice_date': arrival_notice,
            'warehouse_receipt_date': warehouse_receipt,
            'expected_qty': parse_float(get_col(11)),
            'allocated_qty': parse_float(get_col(12)),
            'location': clean_text(get_col(21)).upper() if get_col(21) else '',
            'status': status,
        }

        containers.append(container)

    return containers, skipped


def has_changes(existing, new_data, fields):
    """Check if any field has changed between existing record and new data"""
    for field in fields:
        existing_val = existing.get(field)
        new_val = new_data.get(field)
        # Normalize None and empty string
        if existing_val in (None, ''):
            existing_val = None
        if new_val in (None, ''):
            new_val = None
        if existing_val != new_val:
            return True
    return False


def import_containers(client, containers, batch_size=50):
    """Import containers into ERPNext using upsert"""
    results = {
        'created': 0,
        'updated': 0,
        'unchanged': 0,
        'failed': 0,
        'status_counts': {'Pre-Departure': 0, 'In Transit': 0, 'At Port': 0, 'Arrived': 0},
        'warehouse_warnings': [],
        'errors': []
    }

    compare_fields = [
        'container_name', 'container_no', 'capacity', 'shipped_to',
        'agent', 'provider', 'etd', 'eta', 'actual_docking',
        'arrival_notice_date', 'warehouse_receipt_date', 'status',
        'expected_qty', 'allocated_qty', 'location'
    ]

    total = len(containers)

    for i, cont in enumerate(containers):
        try:
            # Resolve warehouse reference
            shipped_to = None
            if cont.get('shipped_to_ref'):
                shipped_to = resolve_warehouse(client, cont['shipped_to_ref'])
                if not shipped_to:
                    existing_refs = [w['ref'] for w in results['warehouse_warnings']]
                    if cont['shipped_to_ref'] not in existing_refs:
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
                'status': cont['status'],
                'location': cont.get('location', ''),
            }

            if shipped_to:
                container_data['shipped_to'] = shipped_to
            if cont.get('etd'):
                container_data['etd'] = cont['etd']
            if cont.get('eta'):
                container_data['eta'] = cont['eta']
            if cont.get('actual_docking'):
                container_data['actual_docking'] = cont['actual_docking']
            if cont.get('arrival_notice_date'):
                container_data['arrival_notice_date'] = cont['arrival_notice_date']
            if cont.get('warehouse_receipt_date'):
                container_data['warehouse_receipt_date'] = cont['warehouse_receipt_date']
            if cont.get('expected_qty'):
                container_data['expected_qty'] = cont['expected_qty']
            if cont.get('allocated_qty'):
                container_data['allocated_qty'] = cont['allocated_qty']

            # Track status distribution
            results['status_counts'][cont['status']] += 1

            existing = client.get_container(cont['container_name'])

            if existing:
                if not has_changes(existing, container_data, compare_fields):
                    results['unchanged'] += 1
                    if (i + 1) % 20 == 0:
                        print(f'[{i+1}/{total}] Progress...')
                    continue

                response = client.update_container(cont['container_name'], container_data)
                if response.get('data', {}).get('name'):
                    results['updated'] += 1
                    print(f'[{i+1}/{total}] Updated: {cont["container_name"]} ({cont["status"]})')
                else:
                    error = response.get('error', 'Unknown error')
                    results['failed'] += 1
                    results['errors'].append({
                        'container': cont['container_name'],
                        'error': f'Update failed: {error}'
                    })
            else:
                response = client.create_container(container_data)
                if response.get('data', {}).get('name'):
                    results['created'] += 1
                    print(f'[{i+1}/{total}] Created: {cont["container_name"]} ({cont["status"]})')
                else:
                    error = response.get('error', 'Unknown error')
                    results['failed'] += 1
                    results['errors'].append({
                        'container': cont['container_name'],
                        'error': f'Create failed: {error}'
                    })

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': 'Request timeout'
            })

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': f'Network error: {type(e).__name__}'
            })

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'container': cont['container_name'],
                'error': str(e)
            })

        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} containers, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    parser = argparse.ArgumentParser(description='Container Migration Script')
    parser.add_argument('--update-doctype', action='store_true',
                       help='Update Container DocType with new field definitions')
    parser.add_argument('--doctype-only', action='store_true',
                       help='Only create/update DocType, skip data migration')
    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: Container Migration')
    print('=' * 60)

    config = get_config(require_google_sheets=not args.doctype_only)

    if not args.doctype_only:
        print('\n1. Connecting to Google Sheets...')
        sheets_service = get_sheets_service(config)
    else:
        sheets_service = None

    print('\n2. Connecting to ERPNext...')
    erpnext = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['api_key'],
        config['erpnext']['api_secret']
    )

    print('\n3. Ensuring Container doctype exists...')
    force_update = args.update_doctype or args.doctype_only
    if not ensure_container_doctype(erpnext, force_update=force_update):
        print('ERROR: Cannot proceed without Container doctype')
        sys.exit(1)

    if args.doctype_only:
        print('\n' + '=' * 60)
        print('DOCTYPE UPDATE COMPLETE')
        print('=' * 60)
        sys.exit(0)

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
    print(f'Created:   {results["created"]}')
    print(f'Updated:   {results["updated"]}')
    print(f'Unchanged: {results["unchanged"]}')
    print(f'Failed:    {results["failed"]}')

    print('\nStatus Distribution:')
    for status, count in results['status_counts'].items():
        print(f'  {status}: {count}')

    if results['warehouse_warnings']:
        print(f'\nWarning: {len(results["warehouse_warnings"])} warehouse refs not found')

    if results['errors']:
        print(f'\nFirst 5 errors:')
        for err in results['errors'][:5]:
            print(f'  - {err["container"]}: {err["error"][:60]}')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'container_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'total_containers': len(containers),
            'created': results['created'],
            'updated': results['updated'],
            'unchanged': results['unchanged'],
            'failed': results['failed'],
            'status_counts': results['status_counts'],
            'warehouse_warnings': results['warehouse_warnings'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nReport saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
