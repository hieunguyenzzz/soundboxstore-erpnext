#!/usr/bin/env python3
"""
SBS-61: Stock Reservations Migration Script
Creates Stock Reservation entries from Google Sheets Allocation data

Data Sources:
- Allocation sheet: Permanent allocations linked to orders
- Temp Allocation sheet: Temporary allocations

Uses ERPNext v15 Stock Reservation Entry doctype.

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
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
COMPANY = "DWIR"
BATCH_SIZE = 50
DEFAULT_WAREHOUSE = 'Stores - D'


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
        print("  ERPNEXT_URL          - ERPNext server URL (e.g., https://erp.soundboxstore.com)")
        print("  ERPNEXT_API_KEY      - ERPNext API key")
        print("  ERPNEXT_API_SECRET   - ERPNext API secret")
        print("  GOOGLE_SHEETS_CREDS  - Path to service account JSON file OR JSON content")
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
    """ERPNext API Client"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.auth_header = f'token {api_key}:{api_secret}'
        self.session.headers.update({'Authorization': self.auth_header})
        self._verify_connection()

    def _verify_connection(self):
        """Verify API connection works"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed with status {response.status_code}')
        user = response.json().get('message', 'Unknown')
        print(f'Connected to ERPNext at {self.url} as {user}')

    def get_sales_order_by_po_no(self, po_no):
        """Find Sales Order by external order number (po_no field)"""
        filters = json.dumps([
            ['po_no', '=', po_no],
            ['docstatus', '=', 1]  # Only submitted SOs
        ])
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order',
            params={
                'filters': filters,
                'fields': json.dumps(['name', 'customer']),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', [])
                return data[0] if data else None
            except json.JSONDecodeError:
                return None
        return None

    def get_sales_order_items(self, so_name):
        """Get items from a Sales Order"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order/{so_name}',
            params={'fields': json.dumps(['items'])},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', {})
                return data.get('items', [])
            except json.JSONDecodeError:
                return []
        return []

    def get_item(self, item_code):
        """Get an Item by code"""
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

    def get_stock_balance(self, item_code, warehouse):
        """Get actual stock balance for item in warehouse"""
        filters = json.dumps([
            ['item_code', '=', item_code],
            ['warehouse', '=', warehouse]
        ])
        response = self.session.get(
            f'{self.url}/api/resource/Bin',
            params={
                'filters': filters,
                'fields': json.dumps(['actual_qty', 'reserved_qty']),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', [])
                if data:
                    return {
                        'actual_qty': data[0].get('actual_qty', 0),
                        'reserved_qty': data[0].get('reserved_qty', 0)
                    }
            except json.JSONDecodeError:
                pass
        return {'actual_qty': 0, 'reserved_qty': 0}

    def get_existing_reservation(self, so_name, item_code, warehouse):
        """Check if reservation already exists"""
        filters = json.dumps([
            ['voucher_type', '=', 'Sales Order'],
            ['voucher_no', '=', so_name],
            ['item_code', '=', item_code],
            ['warehouse', '=', warehouse],
            ['status', '!=', 'Cancelled']
        ])
        response = self.session.get(
            f'{self.url}/api/resource/Stock Reservation Entry',
            params={
                'filters': filters,
                'fields': json.dumps(['name', 'reserved_qty']),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', [])
                return data[0] if data else None
            except json.JSONDecodeError:
                return None
        return None

    def create_stock_reservation(self, data):
        """Create a Stock Reservation Entry"""
        response = self.session.post(
            f'{self.url}/api/resource/Stock Reservation Entry',
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

    def check_stock_reservation_enabled(self):
        """Check if Stock Reservation is enabled in Stock Settings"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock Settings/Stock Settings',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                data = response.json().get('data', {})
                return data.get('enable_stock_reservation', 0) == 1
            except json.JSONDecodeError:
                return False
        return False

    def enable_stock_reservation(self):
        """Enable Stock Reservation in Stock Settings"""
        response = self.session.put(
            f'{self.url}/api/resource/Stock Settings/Stock Settings',
            json={'enable_stock_reservation': 1},
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code in (200, 201)


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


def clean_int(value):
    """Convert string to int"""
    if not value:
        return 0
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return 0


def read_allocations(service, spreadsheet_id, sheet_name):
    """Read allocation data from sheet

    Expected columns (may vary by sheet):
    - Order No / Customer reference
    - SKU
    - Qty Allocated
    - Warehouse/Location
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A2:M5000'
        ).execute()
    except Exception as e:
        print(f'   Warning: Could not read {sheet_name}: {e}')
        return []

    rows = result.get('values', [])
    allocations = []

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        # Different column mappings for different sheets
        if sheet_name == 'Temp Allocation':
            # Temp Allocation columns:
            # A=ref, B=REF (order ref), C=Name, D=SBS SKU, E=Product, F=Batch, G=Location, H=Container, I=QTY
            order_ref = clean_text(get_col(1))  # Col B: REF (like PI-20240001)
            sku = clean_text(get_col(3))  # Col D: SBS SKU
            qty = clean_int(get_col(8)) or 1  # Col I: QTY (default to 1 if 0)
            warehouse = clean_text(get_col(6))  # Col G: CURRENT LOCATION
        else:
            # Allocation sheet columns:
            # A=row#, B=concat_ref, C=Order No, D=Name, E=Country, F=SKU, G=Product, H=Batch, I=Location, J=Container
            order_ref = clean_text(get_col(2))  # Col C: Order No.
            sku = clean_text(get_col(5))  # Col F: SKU
            qty = 1  # Each row represents 1 unit allocation
            warehouse = clean_text(get_col(8))  # Col I: CURRENT LOCATION

        # Skip if no meaningful data
        if not sku or not order_ref:
            continue

        allocations.append({
            'order_ref': order_ref,
            'item_code': sku,
            'qty': qty,
            'warehouse': warehouse if warehouse else DEFAULT_WAREHOUSE,
            'source_sheet': sheet_name
        })

    return allocations


def resolve_warehouse(location):
    """Map location to ERPNext warehouse name"""
    if not location:
        return DEFAULT_WAREHOUSE

    warehouse_mapping = {
        'UK - MAR': 'Stock In Warehouse UK MAR - D',
        'UK - FSL': 'Stock In Warehouse UK FSL - D',
        'UK - PRIM': 'Stock In Warehouse UK PRIM - D',
        'ES': 'Stock In Warehouse ES - D',
        'SPAIN': 'Stock In Warehouse ES - D',
        'ON WATER': 'Goods on Water - D',
    }

    location_upper = location.upper()
    for key, warehouse in warehouse_mapping.items():
        if key in location_upper:
            return warehouse

    return DEFAULT_WAREHOUSE


def create_stock_reservations(client, allocations):
    """Create Stock Reservation entries in ERPNext"""
    results = {
        'created': 0,
        'skipped': 0,
        'failed': 0,
        'no_so': 0,
        'no_stock': 0,
        'errors': []
    }

    # Group allocations by order reference
    by_order = defaultdict(list)
    for alloc in allocations:
        by_order[alloc['order_ref']].append(alloc)

    total = len(by_order)
    print(f'   Processing {total} order references...')

    for i, (order_ref, items) in enumerate(by_order.items(), 1):
        if not order_ref:
            continue

        print(f'[{i}/{total}] Processing order: {order_ref}')

        # Find the Sales Order
        so = client.get_sales_order_by_po_no(order_ref)
        if not so:
            results['no_so'] += 1
            print(f'   Skipped: No submitted SO found for {order_ref}')
            continue

        so_name = so['name']

        for alloc in items:
            item_code = alloc['item_code']
            qty = alloc['qty']
            warehouse = resolve_warehouse(alloc['warehouse'])

            try:
                # Check if reservation already exists
                existing = client.get_existing_reservation(so_name, item_code, warehouse)
                if existing:
                    results['skipped'] += 1
                    print(f'   Skipped: Reservation exists for {item_code}')
                    continue

                # Check stock availability
                stock = client.get_stock_balance(item_code, warehouse)
                available = stock['actual_qty'] - stock['reserved_qty']

                if available < qty:
                    # Try to reserve what's available
                    if available <= 0:
                        results['no_stock'] += 1
                        print(f'   No stock: {item_code} in {warehouse}')
                        continue
                    qty = available
                    print(f'   Partial: Only {qty} available for {item_code}')

                # Create the reservation
                reservation_data = {
                    'voucher_type': 'Sales Order',
                    'voucher_no': so_name,
                    'item_code': item_code,
                    'warehouse': warehouse,
                    'reserved_qty': qty,
                    'company': COMPANY,
                    'stock_uom': 'Nos',
                    'voucher_qty': qty,
                    'available_qty': stock['actual_qty'],
                    'reservation_based_on': 'Qty'
                }

                response = client.create_stock_reservation(reservation_data)
                if response.get('data', {}).get('name'):
                    results['created'] += 1
                    print(f'   Created: {response["data"]["name"]} for {item_code}')
                else:
                    error = response.get('exception') or response.get('message') or response.get('error', 'Unknown error')
                    results['failed'] += 1
                    results['errors'].append({
                        'order_ref': order_ref,
                        'item_code': item_code,
                        'error': str(error)[:150]
                    })
                    print(f'   ERROR: {str(error)[:80]}')

            except requests.exceptions.Timeout:
                results['failed'] += 1
                results['errors'].append({
                    'order_ref': order_ref,
                    'item_code': item_code,
                    'error': 'Request timeout'
                })
                print(f'   ERROR: Timeout')

            except requests.exceptions.RequestException as e:
                results['failed'] += 1
                results['errors'].append({
                    'order_ref': order_ref,
                    'item_code': item_code,
                    'error': f'Network error: {type(e).__name__}'
                })
                print(f'   ERROR: Network error')

        # Rate limiting
        if i % BATCH_SIZE == 0:
            print(f'   Processed {i}/{total}, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-61: Stock Reservations Migration')
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

    # Check if Stock Reservation is enabled
    print('\n3. Checking Stock Reservation settings...')
    if not erpnext.check_stock_reservation_enabled():
        print('   Stock Reservation not enabled. Enabling...')
        if erpnext.enable_stock_reservation():
            print('   Stock Reservation enabled successfully')
        else:
            print('   WARNING: Could not enable Stock Reservation')
            print('   Please enable it manually in Stock Settings')
    else:
        print('   Stock Reservation is enabled')

    # Read allocations from both sheets
    all_allocations = []
    sheets_to_read = ['Allocation', 'Temp Allocation']

    for sheet_name in sheets_to_read:
        print(f'\n4. Reading {sheet_name} sheet...')
        allocations = read_allocations(
            sheets_service,
            config['google_sheets']['spreadsheet_id'],
            sheet_name
        )
        print(f'   Found {len(allocations)} allocations')
        all_allocations.extend(allocations)

    print(f'\n   Total allocations: {len(all_allocations)}')

    if not all_allocations:
        print('\nNo allocations to process. Exiting.')
        sys.exit(0)

    # Show distribution by source
    by_source = defaultdict(int)
    for alloc in all_allocations:
        by_source[alloc['source_sheet']] += 1
    print('   Distribution by source:')
    for source, count in sorted(by_source.items()):
        print(f'      {source}: {count}')

    print(f'\n5. Creating Stock Reservations...')
    results = create_stock_reservations(erpnext, all_allocations)

    print('\n' + '=' * 60)
    print('STOCK RESERVATIONS MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created:         {results["created"]}')
    print(f'Skipped:         {results["skipped"]} (already exist)')
    print(f'No SO Found:     {results["no_so"]}')
    print(f'No Stock:        {results["no_stock"]}')
    print(f'Failed:          {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["order_ref"]} / {err["item_code"]}: {err["error"][:60]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'reservation_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_allocations': len(all_allocations),
            'by_source': dict(by_source),
            'created': results['created'],
            'skipped': results['skipped'],
            'no_so_found': results['no_so'],
            'no_stock': results['no_stock'],
            'failed': results['failed'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
