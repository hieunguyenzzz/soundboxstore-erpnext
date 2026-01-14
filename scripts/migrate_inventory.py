#!/usr/bin/env python3
"""
SBS-64: Inventory Migration Script
Imports opening stock from Google Sheets Inventory into ERPNext via Stock Entry.

This script:
1. Reads inventory data from Google Sheets (REMAINING QTY column)
2. Groups items by warehouse location
3. Creates Stock Entry (Material Receipt) per warehouse
4. Uses valuation rates from existing Item master

Columns (0-indexed):
- C (2):  SBS SKU - item_code
- L (11): REMAINING QTY - available stock for sale
- N (13): CURRENT LOCATION - warehouse

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_inventory.py
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

# Warehouse mapping: Google Sheets location -> ERPNext warehouse base name
WAREHOUSE_BASE_MAPPING = {
    'FOR MANUFACTURE': 'For Manufacture',
    'ON WATER': 'Goods on Water',
    'BEACONSFIELD OFFICE': 'Beaconsfield Office',
    'BEACONSFIELD SHOWROOM': 'Beaconsfield Showroom',
    'GRAFANOLA SHOWROOM': 'Grafanola Showroom',
    'STOCK IN UBI - HODDESDON': 'Stock In UBI Hoddesdon',
    'STOCK IN UBI - WARRINGTON': 'Stock In UBI Warrington',
    'STOCK IN WAREHOUSE - ES': 'Stock In Warehouse ES',
    'STOCK IN WAREHOUSE - ES - GRADE A': 'Stock In Warehouse ES Grade A',
    'STOCK IN WAREHOUSE - UK - FSL': 'Stock In Warehouse UK FSL',
    'STOCK IN WAREHOUSE - UK - MAR': 'Stock In Warehouse UK MAR',
    'STOCK IN WAREHOUSE - UK - PRIM': 'Stock In Warehouse UK PRIM',
    'WAITING CLEARANCE': 'Waiting Clearance',
}

DEFAULT_WAREHOUSE_BASE = 'Stores'


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

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }
        self._verify_connection()
        self.company_name, self.company_abbr = self._get_company_info()
        print(f'Using company: {self.company_name} (abbr: {self.company_abbr})')

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

    def _get_company_info(self):
        """Get the first company's name and abbreviation"""
        response = self.session.get(
            f'{self.url}/api/resource/Company',
            params={'limit_page_length': 1},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception('Failed to fetch company list')

        companies = response.json().get('data', [])
        if not companies:
            raise Exception('No company found in ERPNext')

        company_name = companies[0]['name']

        response = self.session.get(
            f'{self.url}/api/resource/Company/{company_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Failed to fetch company details for {company_name}')

        company_data = response.json().get('data', {})
        return company_data.get('name'), company_data.get('abbr', 'SBS')

    def get_items_batch(self, item_codes, batch_size=100):
        """Fetch multiple items in batches and return a dict of {item_code: item_data}"""
        all_items = {}

        for i in range(0, len(item_codes), batch_size):
            batch = item_codes[i:i + batch_size]
            filters = json.dumps([['name', 'in', batch]])
            fields = json.dumps(['name', 'valuation_rate', 'standard_rate'])

            response = self.session.get(
                f'{self.url}/api/resource/Item',
                params={
                    'filters': filters,
                    'fields': fields,
                    'limit_page_length': batch_size
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                try:
                    items = response.json().get('data', [])
                    for item in items:
                        all_items[item['name']] = item
                except json.JSONDecodeError:
                    pass

        return all_items

    def warehouse_exists(self, warehouse_name):
        """Check if warehouse exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_warehouse(self, warehouse_name):
        """Create a new warehouse"""
        suffix = f' - {self.company_abbr}'
        base_name = warehouse_name.replace(suffix, '') if warehouse_name.endswith(suffix) else warehouse_name
        data = {
            'warehouse_name': base_name,
            'company': self.company_name,
            'is_group': 0,
        }
        response = self.session.post(
            f'{self.url}/api/resource/Warehouse',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', f'HTTP {response.status_code}')}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def stock_entry_type_exists(self, entry_type):
        """Check if Stock Entry Type exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock Entry Type/{entry_type}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def fiscal_year_exists(self, year):
        """Check if Fiscal Year exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Fiscal Year/{year}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_fiscal_year(self, year):
        """Create a Fiscal Year"""
        data = {
            'year': year,
            'year_start_date': f'{year}-01-01',
            'year_end_date': f'{year}-12-31',
            'companies': [{'company': self.company_name}]
        }
        response = self.session.post(
            f'{self.url}/api/resource/Fiscal Year',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', f'HTTP {response.status_code}')}
            except json.JSONDecodeError:
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
            'company': self.company_name,
            'items': items
        }
        response = self.session.post(
            f'{self.url}/api/resource/Stock Entry',
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

    def submit_stock_entry(self, stock_entry_name):
        """Submit a Stock Entry to make it effective"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock Entry/{stock_entry_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return {'error': f'Failed to get document: HTTP {response.status_code}'}

        try:
            doc = response.json().get('data')
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response on get'}

        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', f'HTTP {response.status_code}')}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        try:
            result = response.json()
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
    cleaned = re.sub(r'[£$€,]', '', str(value).strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def resolve_warehouse(location, company_abbr):
    """Map Google Sheets location to ERPNext warehouse name"""
    suffix = f' - {company_abbr}'

    if not location:
        return DEFAULT_WAREHOUSE_BASE + suffix

    location = location.strip().upper()

    if location in WAREHOUSE_BASE_MAPPING:
        return WAREHOUSE_BASE_MAPPING[location] + suffix

    for sheet_loc, erp_wh_base in WAREHOUSE_BASE_MAPPING.items():
        if sheet_loc in location or location in sheet_loc:
            return erp_wh_base + suffix

    return DEFAULT_WAREHOUSE_BASE + suffix


def read_inventory(service, spreadsheet_id, company_abbr):
    """Read inventory data from Google Sheets.

    Columns (0-indexed):
    - C (2):  SBS SKU - item_code
    - L (11): REMAINING QTY - available stock
    - N (13): CURRENT LOCATION - warehouse
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Inventory!A2:O5000'
    ).execute()

    rows = result.get('values', [])
    inventory = []
    skipped = []

    for i, row in enumerate(rows):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))           # Col C: SBS SKU
        qty = clean_float(get_col(11))         # Col L: REMAINING QTY
        location = clean_text(get_col(13))     # Col N: CURRENT LOCATION

        if not sku:
            continue

        if qty <= 0:
            skipped.append({
                'sku': sku,
                'qty': qty,
                'reason': f'Zero or negative remaining qty: {qty}'
            })
            continue

        inventory.append({
            'item_code': sku,
            'qty': qty,
            'location': location,
            'warehouse': resolve_warehouse(location, company_abbr),
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
    print(f'   ERROR: Stock Entry Type "{entry_type}" does not exist')
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

    return {'created': created, 'existing': existing, 'failed': failed}


def create_stock_entries(client, inventory, batch_size=100):
    """Create Stock Entries grouped by warehouse."""
    results = {
        'entries_created': 0,
        'entries_submitted': 0,
        'total_items': 0,
        'items_failed': 0,
        'items_missing': [],
        'errors': []
    }

    posting_date = datetime.now().strftime('%Y-%m-%d')

    by_warehouse = defaultdict(list)
    for item in inventory:
        by_warehouse[item['warehouse']].append(item)

    total_warehouses = len(by_warehouse)

    print('   Pre-fetching item valuation rates...')
    all_item_codes = list(set(item['item_code'] for item in inventory))
    item_data_map = client.get_items_batch(all_item_codes)
    print(f'   Fetched {len(item_data_map)} items from ERPNext')

    for wh_idx, (warehouse, items) in enumerate(sorted(by_warehouse.items()), 1):
        print(f'\n[{wh_idx}/{total_warehouses}] Processing warehouse: {warehouse}')
        print(f'   Items to process: {len(items)}')

        stock_items = []
        for item in items:
            item_data = item_data_map.get(item['item_code'])
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

            if valuation_rate <= 0:
                stock_item['allow_zero_valuation_rate'] = 1

            stock_items.append(stock_item)

        if not stock_items:
            print(f'   No valid items for warehouse {warehouse}')
            continue

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

                    submit_response = client.submit_stock_entry(entry_name)
                    if submit_response.get('data', {}).get('docstatus') == 1:
                        results['entries_submitted'] += 1
                        print(f'   Submitted: {entry_name}')
                    else:
                        error = submit_response.get('error', 'Unknown error')
                        print(f'   WARNING: Created but failed to submit: {error}')
                else:
                    error = response.get('error', 'Unknown error')
                    results['errors'].append({
                        'warehouse': warehouse,
                        'error': str(error)[:200]
                    })
                    print(f'   ERROR: Failed to create Stock Entry: {str(error)[:100]}')

            except requests.exceptions.Timeout:
                results['errors'].append({'warehouse': warehouse, 'error': 'Request timeout'})
                print(f'   ERROR: Timeout for warehouse {warehouse}')

            except requests.exceptions.RequestException as e:
                results['errors'].append({'warehouse': warehouse, 'error': f'Network error: {type(e).__name__}'})
                print(f'   ERROR: Network error for {warehouse}: {type(e).__name__}')

            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-64: Inventory Migration (REMAINING QTY)')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    erpnext = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['api_key'],
        config['erpnext']['api_secret']
    )

    print('\n2. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print('\n3. Reading Inventory sheet (REMAINING QTY column)...')
    inventory, skipped = read_inventory(
        sheets_service,
        config['google_sheets']['spreadsheet_id'],
        erpnext.company_abbr
    )
    print(f'   Found {len(inventory)} items with positive remaining qty')
    print(f'   Skipped {len(skipped)} items (zero/negative qty)')

    if not inventory:
        print('\nNo inventory items to import. Exiting.')
        sys.exit(0)

    warehouse_counts = defaultdict(int)
    for item in inventory:
        warehouse_counts[item['warehouse']] += 1
    print('\n   Warehouse distribution:')
    for wh, count in sorted(warehouse_counts.items()):
        print(f'      {wh}: {count} items')

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

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'inventory_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'source_column': 'REMAINING QTY (col L)',
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

    has_errors = results['items_failed'] > 0 or len(results['errors']) > 0
    sys.exit(1 if has_errors else 0)


if __name__ == '__main__':
    main()
