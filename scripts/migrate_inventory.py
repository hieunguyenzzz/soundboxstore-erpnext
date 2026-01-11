#!/usr/bin/env python3
"""
SBS-52: Inventory Migration Script
Imports opening stock from Google Sheets Inventory into ERPNext via Stock Entry

This script:
1. Reads inventory data from Google Sheets (REMAINING QTY column)
2. Groups items by warehouse location
3. Creates Stock Entry (Material Receipt) per warehouse
4. Uses valuation rates from existing Item master

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
from collections import defaultdict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constants
REQUEST_TIMEOUT = 30  # seconds
COMPANY = "Soundbox Store"

# Warehouse mapping: Google Sheets location -> ERPNext warehouse name
# ERPNext warehouses use " - SBS" suffix
WAREHOUSE_MAPPING = {
    # Existing warehouses from SBS-51
    'FOR MANUFACTURE': 'For Manufacture - SBS',
    'ON WATER': 'Goods on Water - SBS',
    # New warehouses to be created
    'BEACONSFIELD OFFICE': 'Beaconsfield Office - SBS',
    'BEACONSFIELD SHOWROOM': 'Beaconsfield Showroom - SBS',
    'GRAFANOLA SHOWROOM': 'Grafanola Showroom - SBS',
    'STOCK IN UBI - HODDESDON': 'Stock In UBI Hoddesdon - SBS',
    'STOCK IN UBI - WARRINGTON': 'Stock In UBI Warrington - SBS',
    'STOCK IN WAREHOUSE - ES': 'Stock In Warehouse ES - SBS',
    'STOCK IN WAREHOUSE - ES - GRADE A': 'Stock In Warehouse ES Grade A - SBS',
    'STOCK IN WAREHOUSE - UK - FSL': 'Stock In Warehouse UK FSL - SBS',
    'STOCK IN WAREHOUSE - UK - MAR': 'Stock In Warehouse UK MAR - SBS',
    'STOCK IN WAREHOUSE - UK - PRIM': 'Stock In Warehouse UK PRIM - SBS',
    'WAITING CLEARANCE': 'Waiting Clearance - SBS',
}

# Default warehouse for unmapped locations
DEFAULT_WAREHOUSE = 'Stores - SBS'


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

    def get_item(self, item_code):
        """Get an Item by code with valuation_rate"""
        response = self.session.get(
            f'{self.url}/api/resource/Item/{item_code}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def warehouse_exists(self, warehouse_name):
        """Check if warehouse exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_warehouse(self, warehouse_name):
        """Create a new warehouse"""
        # Extract parent warehouse from name
        data = {
            'warehouse_name': warehouse_name.replace(' - SBS', ''),
            'company': COMPANY,
            'is_group': 0,
        }
        response = self.session.post(
            f'{self.url}/api/resource/Warehouse',
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

    def stock_entry_type_exists(self, entry_type):
        """Check if Stock Entry Type exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock Entry Type/{entry_type}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_stock_entry_type(self, entry_type, purpose):
        """Create a Stock Entry Type"""
        data = {
            'name': entry_type,
            'purpose': purpose
        }
        response = self.session.post(
            f'{self.url}/api/resource/Stock Entry Type',
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

    def fiscal_year_exists(self, year):
        """Check if Fiscal Year exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Fiscal Year/{year}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_fiscal_year(self, year):
        """Create a Fiscal Year"""
        data = {
            'year': year,
            'year_start_date': f'{year}-01-01',
            'year_end_date': f'{year}-12-31',
            'companies': [{'company': COMPANY}]
        }
        response = self.session.post(
            f'{self.url}/api/resource/Fiscal Year',
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

    def create_stock_entry(self, items, warehouse, posting_date):
        """Create a Stock Entry (Material Receipt) with multiple items"""
        data = {
            'doctype': 'Stock Entry',
            'stock_entry_type': 'Material Receipt',
            'posting_date': posting_date,
            'company': COMPANY,
            'items': items
        }
        response = self.session.post(
            f'{self.url}/api/resource/Stock Entry',
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

    def submit_stock_entry(self, stock_entry_name):
        """Submit a Stock Entry to make it effective"""
        # First get the document fresh
        response = self.session.get(
            f'{self.url}/api/resource/Stock Entry/{stock_entry_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return {'error': f'Failed to get document: HTTP {response.status_code}'}

        try:
            doc = response.json().get('data')
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response on get'}

        # Submit using the proper API
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            result = response.json()
            # Check if submitted successfully
            if result.get('message', {}).get('docstatus') == 1:
                return {'data': result.get('message')}
            return {'error': 'Submit did not return docstatus=1'}
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


def clean_float(value):
    """Convert string to float"""
    if not value:
        return 0.0
    # Remove currency symbols and commas
    cleaned = re.sub(r'[£$€,]', '', str(value).strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def resolve_warehouse(location):
    """Map Google Sheets location to ERPNext warehouse name"""
    if not location:
        return DEFAULT_WAREHOUSE

    location = location.strip().upper()

    # Check direct mapping
    if location in WAREHOUSE_MAPPING:
        return WAREHOUSE_MAPPING[location]

    # Try to find partial match
    for sheet_loc, erp_wh in WAREHOUSE_MAPPING.items():
        if sheet_loc in location or location in sheet_loc:
            return erp_wh

    return DEFAULT_WAREHOUSE


def read_inventory(service, spreadsheet_id):
    """Read inventory data from Google Sheets

    Columns:
    - Col 2 (C): SBS SKU - item_code
    - Col 11 (L): REMAINING QTY - available stock
    - Col 13 (N): CURRENT LOCATION - warehouse
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Inventory!A2:O5000'  # Start from row 2 (skip header)
    ).execute()

    rows = result.get('values', [])
    inventory = []
    skipped = []

    for i, row in enumerate(rows):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))  # Col C: SBS SKU
        remaining_qty = clean_float(get_col(11))  # Col L: REMAINING QTY
        location = clean_text(get_col(13))  # Col N: CURRENT LOCATION

        # Skip rows without SKU or with zero/negative stock
        if not sku:
            continue

        if remaining_qty <= 0:
            skipped.append({
                'sku': sku,
                'reason': f'Zero or negative stock: {remaining_qty}'
            })
            continue

        inventory.append({
            'item_code': sku,
            'qty': remaining_qty,
            'location': location,
            'warehouse': resolve_warehouse(location)
        })

    return inventory, skipped


def ensure_fiscal_year(client, year):
    """Ensure Fiscal Year exists"""
    if client.fiscal_year_exists(year):
        print(f'   Fiscal Year {year} exists')
        return True

    print(f'   Creating Fiscal Year {year}...')
    response = client.create_fiscal_year(year)
    if response.get('data', {}).get('name'):
        print(f'   Created Fiscal Year {year}')
        return True
    else:
        error = response.get('error', 'Unknown error')
        print(f'   ERROR: Failed to create Fiscal Year: {error}')
        return False


def ensure_stock_entry_type(client):
    """Ensure Material Receipt Stock Entry Type exists"""
    entry_type = 'Material Receipt'
    if client.stock_entry_type_exists(entry_type):
        print(f'   Stock Entry Type "{entry_type}" exists')
        return True

    print(f'   Creating Stock Entry Type "{entry_type}"...')
    response = client.create_stock_entry_type(entry_type, entry_type)
    if response.get('data', {}).get('name'):
        print(f'   Created Stock Entry Type "{entry_type}"')
        return True
    else:
        error = response.get('error', 'Unknown error')
        print(f'   ERROR: Failed to create Stock Entry Type: {error}')
        return False


def ensure_warehouses(client, inventory):
    """Ensure all required warehouses exist in ERPNext"""
    warehouses_needed = set(item['warehouse'] for item in inventory)
    created = []
    existing = []
    failed = []

    for wh in sorted(warehouses_needed):
        if client.warehouse_exists(wh):
            existing.append(wh)
        else:
            print(f'   Creating warehouse: {wh}')
            response = client.create_warehouse(wh)
            if response.get('data', {}).get('name'):
                created.append(wh)
            else:
                error = response.get('error', 'Unknown error')
                failed.append({'warehouse': wh, 'error': error})
                print(f'   ERROR: Failed to create {wh}: {error}')

    return {
        'created': created,
        'existing': existing,
        'failed': failed
    }


def create_stock_entries(client, inventory, batch_size=100):
    """Create Stock Entries grouped by warehouse

    Creates one Stock Entry per warehouse with all items for that warehouse.
    """
    results = {
        'entries_created': 0,
        'entries_submitted': 0,
        'total_items': 0,
        'items_failed': 0,
        'items_missing': [],
        'errors': []
    }

    # Group items by warehouse
    by_warehouse = defaultdict(list)
    for item in inventory:
        by_warehouse[item['warehouse']].append(item)

    posting_date = datetime.now().strftime('%Y-%m-%d')
    total_warehouses = len(by_warehouse)

    for wh_idx, (warehouse, items) in enumerate(sorted(by_warehouse.items()), 1):
        print(f'\n[{wh_idx}/{total_warehouses}] Processing warehouse: {warehouse}')
        print(f'   Items to process: {len(items)}')

        # Prepare items with valuation rates
        stock_items = []
        for item in items:
            # Get valuation rate from Item master
            item_data = client.get_item(item['item_code'])
            if not item_data:
                results['items_missing'].append(item['item_code'])
                results['items_failed'] += 1
                continue

            valuation_rate = item_data.get('valuation_rate', 0) or 0
            if valuation_rate <= 0:
                valuation_rate = item_data.get('standard_rate', 0) or 0

            stock_item = {
                'item_code': item['item_code'],
                'qty': item['qty'],
                'basic_rate': valuation_rate,
                't_warehouse': warehouse
            }

            # Allow zero valuation rate if no rate is available
            if valuation_rate <= 0:
                stock_item['allow_zero_valuation_rate'] = 1

            stock_items.append(stock_item)

        if not stock_items:
            print(f'   No valid items for warehouse {warehouse}')
            continue

        # Split into batches if too many items
        for batch_start in range(0, len(stock_items), batch_size):
            batch_items = stock_items[batch_start:batch_start + batch_size]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(stock_items) + batch_size - 1) // batch_size

            if total_batches > 1:
                print(f'   Creating Stock Entry (batch {batch_num}/{total_batches}, {len(batch_items)} items)...')
            else:
                print(f'   Creating Stock Entry ({len(batch_items)} items)...')

            try:
                response = client.create_stock_entry(batch_items, warehouse, posting_date)

                if response.get('data', {}).get('name'):
                    entry_name = response['data']['name']
                    results['entries_created'] += 1
                    results['total_items'] += len(batch_items)
                    print(f'   Created: {entry_name}')

                    # Submit the Stock Entry
                    submit_response = client.submit_stock_entry(entry_name)
                    if submit_response.get('data', {}).get('docstatus') == 1:
                        results['entries_submitted'] += 1
                        print(f'   Submitted: {entry_name}')
                    else:
                        error = submit_response.get('error', 'Unknown error')
                        print(f'   WARNING: Created but failed to submit: {error}')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['errors'].append({
                        'warehouse': warehouse,
                        'error': str(error)[:200]
                    })
                    print(f'   ERROR: Failed to create Stock Entry: {str(error)[:100]}')

            except requests.exceptions.Timeout:
                results['errors'].append({
                    'warehouse': warehouse,
                    'error': 'Request timeout'
                })
                print(f'   ERROR: Timeout for warehouse {warehouse}')

            except requests.exceptions.RequestException as e:
                results['errors'].append({
                    'warehouse': warehouse,
                    'error': f'Network error: {type(e).__name__}'
                })
                print(f'   ERROR: Network error for {warehouse}: {type(e).__name__}')

            # Rate limiting
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-52: Inventory Migration')
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

    print('\n3. Reading Inventory sheet...')
    inventory, skipped = read_inventory(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(inventory)} items with stock')
    print(f'   Skipped {len(skipped)} items (zero/negative stock)')

    if not inventory:
        print('\nNo inventory items to import. Exiting.')
        sys.exit(0)

    # Show warehouse distribution
    warehouse_counts = defaultdict(int)
    for item in inventory:
        warehouse_counts[item['warehouse']] += 1
    print('\n   Warehouse distribution:')
    for wh, count in sorted(warehouse_counts.items()):
        print(f'      {wh}: {count} items')

    # Get current year for Fiscal Year
    current_year = datetime.now().strftime('%Y')

    print(f'\n4. Ensuring Fiscal Year {current_year} exists...')
    if not ensure_fiscal_year(erpnext, current_year):
        print('ERROR: Cannot proceed without Fiscal Year')
        sys.exit(1)

    print('\n5. Ensuring Stock Entry Type exists...')
    if not ensure_stock_entry_type(erpnext):
        print('ERROR: Cannot proceed without Stock Entry Type')
        sys.exit(1)

    print('\n6. Ensuring warehouses exist...')
    wh_results = ensure_warehouses(erpnext, inventory)
    print(f'   Existing: {len(wh_results["existing"])}')
    print(f'   Created: {len(wh_results["created"])}')
    if wh_results['failed']:
        print(f'   Failed: {len(wh_results["failed"])}')

    print('\n7. Creating Stock Entries...')
    results = create_stock_entries(erpnext, inventory)

    print('\n' + '=' * 60)
    print('INVENTORY MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Stock Entries Created:   {results["entries_created"]}')
    print(f'Stock Entries Submitted: {results["entries_submitted"]}')
    print(f'Total Items Imported:    {results["total_items"]}')
    print(f'Items Failed:            {results["items_failed"]}')

    if results['items_missing']:
        print(f'\nMissing Items (not in Item master): {len(results["items_missing"])}')
        for sku in results['items_missing'][:10]:
            print(f'  - {sku}')
        if len(results['items_missing']) > 10:
            print(f'  ... and {len(results["items_missing"]) - 10} more')

    if results['errors']:
        print(f'\nErrors ({len(results["errors"])}):')
        for err in results['errors'][:10]:
            print(f'  - {err["warehouse"]}: {err["error"][:80]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'inventory_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_inventory_items': len(inventory),
            'skipped_items': len(skipped),
            'warehouse_results': wh_results,
            'entries_created': results['entries_created'],
            'entries_submitted': results['entries_submitted'],
            'total_items_imported': results['total_items'],
            'items_failed': results['items_failed'],
            'items_missing': results['items_missing'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    has_errors = results['items_failed'] > 0 or len(results['errors']) > 0
    sys.exit(1 if has_errors else 0)


if __name__ == '__main__':
    main()
