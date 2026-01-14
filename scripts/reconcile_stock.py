#!/usr/bin/env python3
"""
Stock Reconciliation Script
Adjusts stock levels to match Google Sheets Inventory (REMAINING QTY column).

This script:
1. Reads current inventory from Google Sheets
2. Creates Stock Reconciliation document in ERPNext
3. Sets quantities to match the spreadsheet

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/reconcile_stock.py
  python scripts/reconcile_stock.py --dry-run  # Preview without changes
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

REQUEST_TIMEOUT = 60

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

    def get_current_stock(self, item_code, warehouse):
        """Get current stock qty for an item in a warehouse"""
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.get_list',
            json={
                'doctype': 'Bin',
                'filters': [
                    ['item_code', '=', item_code],
                    ['warehouse', '=', warehouse]
                ],
                'fields': ['actual_qty'],
                'limit_page_length': 1
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            bins = response.json().get('message', [])
            if bins:
                return bins[0].get('actual_qty', 0)
        return 0

    def create_stock_reconciliation(self, items, posting_date, purpose='Stock Reconciliation'):
        """Create a Stock Reconciliation document"""
        data = {
            'doctype': 'Stock Reconciliation',
            'purpose': purpose,
            'posting_date': posting_date,
            'posting_time': '23:59:59',
            'company': self.company_name,
            'items': items
        }
        response = self.session.post(
            f'{self.url}/api/resource/Stock Reconciliation',
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

    def submit_document(self, doctype, name):
        """Submit a document"""
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
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
    inventory = {}  # Use dict to aggregate by (item_code, warehouse)

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))           # Col C: SBS SKU
        qty = clean_float(get_col(11))         # Col L: REMAINING QTY
        location = clean_text(get_col(13))     # Col N: CURRENT LOCATION

        if not sku:
            continue

        warehouse = resolve_warehouse(location, company_abbr)
        key = (sku, warehouse)

        # Aggregate quantities for same item in same warehouse
        if key in inventory:
            inventory[key]['qty'] += qty
        else:
            inventory[key] = {
                'item_code': sku,
                'warehouse': warehouse,
                'qty': qty
            }

    return list(inventory.values())


def main():
    """Main reconciliation function"""
    import argparse
    parser = argparse.ArgumentParser(description='Stock Reconciliation')
    parser.add_argument('--dry-run', action='store_true', help='Preview without changes')
    args = parser.parse_args()

    print('=' * 60)
    print('Stock Reconciliation')
    if args.dry_run:
        print('MODE: DRY RUN (no changes will be made)')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['api_key'],
        config['erpnext']['api_secret']
    )

    print('\n2. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print('\n3. Reading Inventory sheet...')
    inventory = read_inventory(
        sheets_service,
        config['google_sheets']['spreadsheet_id'],
        client.company_abbr
    )
    print(f'   Found {len(inventory)} unique item-warehouse combinations')

    if not inventory:
        print('\nNo inventory items found. Exiting.')
        sys.exit(0)

    # Show warehouse distribution
    warehouse_counts = defaultdict(int)
    for item in inventory:
        warehouse_counts[item['warehouse']] += 1
    print('\n   Warehouse distribution:')
    for wh, count in sorted(warehouse_counts.items()):
        print(f'      {wh}: {count} items')

    print('\n4. Validating items exist in ERPNext...')
    all_item_codes = list(set(item['item_code'] for item in inventory))
    item_data_map = client.get_items_batch(all_item_codes)
    print(f'   Found {len(item_data_map)} items in ERPNext')

    missing_items = []
    valid_items = []
    for item in inventory:
        if item['item_code'] in item_data_map:
            item_info = item_data_map[item['item_code']]
            valuation_rate = item_info.get('valuation_rate', 0) or item_info.get('standard_rate', 0) or 100
            valid_items.append({
                'item_code': item['item_code'],
                'warehouse': item['warehouse'],
                'qty': item['qty'],
                'valuation_rate': valuation_rate
            })
        else:
            missing_items.append(item['item_code'])

    print(f'   Valid items: {len(valid_items)}')
    print(f'   Missing items: {len(missing_items)}')

    if not valid_items:
        print('\nNo valid items to reconcile. Exiting.')
        sys.exit(1)

    print('\n5. Creating Stock Reconciliation...')
    posting_date = datetime.now().strftime('%Y-%m-%d')

    # Build reconciliation items
    recon_items = []
    for item in valid_items:
        recon_items.append({
            'item_code': item['item_code'],
            'warehouse': item['warehouse'],
            'qty': item['qty'],
            'valuation_rate': item['valuation_rate']
        })

    if args.dry_run:
        print(f'   Would create Stock Reconciliation with {len(recon_items)} items')
        print(f'\n   Sample items:')
        for item in recon_items[:10]:
            print(f'      {item["item_code"]} @ {item["warehouse"]}: {item["qty"]}')
        if len(recon_items) > 10:
            print(f'      ... and {len(recon_items) - 10} more')
    else:
        # Create in batches of 500 items (ERPNext has limits on document size)
        batch_size = 500
        total_batches = (len(recon_items) + batch_size - 1) // batch_size
        created = 0
        submitted = 0
        errors = []

        for batch_idx in range(0, len(recon_items), batch_size):
            batch = recon_items[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1

            print(f'\n   Batch {batch_num}/{total_batches} ({len(batch)} items)...')

            response = client.create_stock_reconciliation(batch, posting_date)

            if response.get('data', {}).get('name'):
                recon_name = response['data']['name']
                created += 1
                print(f'   Created: {recon_name}')

                # Submit the reconciliation
                submit_response = client.submit_document('Stock Reconciliation', recon_name)
                if submit_response.get('data', {}).get('docstatus') == 1:
                    submitted += 1
                    print(f'   Submitted: {recon_name}')
                else:
                    error = submit_response.get('error', 'Unknown error')
                    errors.append({'batch': batch_num, 'error': f'Submit failed: {error}'})
                    print(f'   WARNING: Failed to submit: {error}')
            else:
                error = response.get('error', 'Unknown error')
                errors.append({'batch': batch_num, 'error': str(error)[:200]})
                print(f'   ERROR: Failed to create: {str(error)[:100]}')

            time.sleep(1)

        print('\n' + '=' * 60)
        print('STOCK RECONCILIATION COMPLETE')
        print('=' * 60)
        print(f'Reconciliations Created:   {created}')
        print(f'Reconciliations Submitted: {submitted}')
        print(f'Total Items Reconciled:    {len(recon_items)}')
        print(f'Missing Items (skipped):   {len(missing_items)}')

        if errors:
            print(f'\nErrors ({len(errors)}):')
            for err in errors[:5]:
                print(f'  - Batch {err["batch"]}: {err["error"][:80]}')

    if missing_items:
        print(f'\nMissing items (first 10):')
        for sku in missing_items[:10]:
            print(f'  - {sku}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'stock_reconciliation_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'dry_run': args.dry_run,
            'total_items': len(recon_items),
            'missing_items': missing_items,
            'warehouse_distribution': dict(warehouse_counts)
        }, f, indent=2)
    print(f'\nReport saved to: {report_path}')


if __name__ == '__main__':
    main()
