#!/usr/bin/env python3
"""
SBS-64: Purchase Order Migration Script
Imports Purchase Orders from Google Sheets Order Status into ERPNext.

This script:
1. Reads order data from Order Status sheet
2. Groups items by Container (one PO per container shipment)
3. Creates Purchase Orders with Container linkage
4. Sets expected delivery dates from ETD/ETA

The Order Status sheet tracks factory orders that become Purchase Orders in ERPNext.
Each container shipment becomes one Purchase Order.

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_purchase_orders.py
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
DEFAULT_SUPPLIER = 'Default Supplier'


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

    def get_supplier(self, supplier_name):
        """Get supplier by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Supplier',
            params={'filters': json.dumps([['supplier_name', '=', supplier_name]])},
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

    def create_supplier(self, supplier_name):
        """Create a new supplier"""
        data = {
            'supplier_name': supplier_name,
            'supplier_group': 'All Supplier Groups',
            'supplier_type': 'Company',
        }
        response = self.session.post(
            f'{self.url}/api/resource/Supplier',
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

    def get_item(self, item_code):
        """Get an Item by code"""
        response = self.session.get(
            f'{self.url}/api/resource/Item/{item_code}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def get_items_batch(self, item_codes, batch_size=100):
        """Fetch multiple items in batches"""
        all_items = {}

        for i in range(0, len(item_codes), batch_size):
            batch = item_codes[i:i + batch_size]
            filters = json.dumps([['name', 'in', batch]])
            fields = json.dumps(['name', 'item_name', 'stock_uom', 'valuation_rate', 'standard_rate'])

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

    def get_container(self, container_name):
        """Get Container by name"""
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

    def get_existing_purchase_order(self, container_name):
        """Check if PO already exists for this container"""
        response = self.session.get(
            f'{self.url}/api/resource/Purchase Order',
            params={
                'filters': json.dumps([['custom_container', '=', container_name]]),
                'fields': json.dumps(['name']),
                'limit_page_length': 1
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

    def create_purchase_order(self, data):
        """Create a Purchase Order"""
        response = self.session.post(
            f'{self.url}/api/resource/Purchase Order',
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

    def submit_purchase_order(self, po_name):
        """Submit a Purchase Order"""
        response = self.session.get(
            f'{self.url}/api/resource/Purchase Order/{po_name}',
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


def clean_int(value):
    """Convert string to int"""
    if not value:
        return 0
    try:
        return int(float(str(value).replace(',', '').strip()))
    except ValueError:
        return 0


def clean_float(value):
    """Convert string to float"""
    if not value:
        return 0.0
    cleaned = re.sub(r'[£$€,]', '', str(value).strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(value):
    """Parse date from various formats"""
    if not value:
        return None

    value = str(value).strip()

    # Try common formats
    formats = [
        '%Y-%m-%d',      # 2025-01-15
        '%d/%m/%Y',      # 15/01/2025
        '%m/%d/%Y',      # 01/15/2025
        '%d-%m-%Y',      # 15-01-2025
        '%d %b %Y',      # 15 Jan 2025
        '%d %B %Y',      # 15 January 2025
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def read_order_status(service, spreadsheet_id):
    """Read order data from Order Status sheet.

    Column mapping (0-indexed):
    A (0):  Order Date
    B (1):  Container Name
    C (2):  SKU
    D (3):  Item Name
    E (4):  Qty Ordered
    F (5):  Unit Cost
    G (6):  Total Cost
    H (7):  Supplier
    I (8):  ETD (Estimated Time of Departure)
    J (9):  ETA (Estimated Time of Arrival)
    K (10): Status
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='Order Status!A2:K5000'
        ).execute()
    except Exception as e:
        print(f'   Warning: Could not read Order Status sheet: {e}')
        return [], []

    rows = result.get('values', [])
    orders = []
    skipped = []

    for i, row in enumerate(rows):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        container = clean_text(get_col(1))
        sku = clean_text(get_col(2))
        qty = clean_int(get_col(4))

        # Skip rows without essential data
        if not sku:
            continue

        if qty <= 0:
            skipped.append({
                'row': i + 2,
                'sku': sku,
                'reason': f'Zero or negative quantity: {qty}'
            })
            continue

        order = {
            'order_date': parse_date(get_col(0)),
            'container': container,
            'item_code': sku,
            'item_name': clean_text(get_col(3)),
            'qty': qty,
            'rate': clean_float(get_col(5)),
            'supplier': clean_text(get_col(7)) or DEFAULT_SUPPLIER,
            'etd': parse_date(get_col(8)),
            'eta': parse_date(get_col(9)),
            'status': clean_text(get_col(10)),
        }

        orders.append(order)

    return orders, skipped


def group_by_container(orders):
    """Group orders by container for PO creation"""
    by_container = defaultdict(list)
    for order in orders:
        container = order['container'] or 'NO_CONTAINER'
        by_container[container].append(order)
    return dict(by_container)


def ensure_supplier(client, supplier_name):
    """Ensure supplier exists, create if not"""
    existing = client.get_supplier(supplier_name)
    if existing:
        return existing

    print(f'   Creating supplier: {supplier_name}')
    response = client.create_supplier(supplier_name)
    if response.get('data', {}).get('name'):
        return response['data']['name']
    else:
        print(f'   WARNING: Failed to create supplier {supplier_name}: {response.get("error")}')
        return None


def create_purchase_orders(client, orders_by_container, item_data_map):
    """Create Purchase Orders grouped by container"""
    results = {
        'created': 0,
        'submitted': 0,
        'skipped': 0,
        'failed': 0,
        'items_missing': [],
        'errors': []
    }

    total_containers = len(orders_by_container)
    suppliers_cache = {}

    for idx, (container, items) in enumerate(sorted(orders_by_container.items()), 1):
        print(f'\n[{idx}/{total_containers}] Processing container: {container}')

        # Skip if PO already exists for this container
        if container != 'NO_CONTAINER':
            existing_po = client.get_existing_purchase_order(container)
            if existing_po:
                print(f'   SKIPPED: PO already exists ({existing_po})')
                results['skipped'] += 1
                continue

        # Get supplier from first item
        supplier_name = items[0]['supplier']
        if supplier_name not in suppliers_cache:
            supplier_id = ensure_supplier(client, supplier_name)
            suppliers_cache[supplier_name] = supplier_id
        supplier_id = suppliers_cache.get(supplier_name)

        if not supplier_id:
            results['errors'].append({
                'container': container,
                'error': f'Could not get/create supplier: {supplier_name}'
            })
            results['failed'] += 1
            continue

        # Build PO items
        po_items = []
        for item in items:
            item_data = item_data_map.get(item['item_code'])
            if not item_data:
                results['items_missing'].append(item['item_code'])
                continue

            rate = item['rate']
            if rate <= 0:
                rate = item_data.get('valuation_rate', 0) or item_data.get('standard_rate', 0) or 0

            po_item = {
                'item_code': item['item_code'],
                'item_name': item_data.get('item_name', item['item_name']),
                'qty': item['qty'],
                'rate': rate,
                'uom': item_data.get('stock_uom', 'Nos'),
                'stock_uom': item_data.get('stock_uom', 'Nos'),
                'conversion_factor': 1,
            }

            # Set schedule date from ETA if available
            if item.get('eta'):
                po_item['schedule_date'] = item['eta']

            po_items.append(po_item)

        if not po_items:
            print(f'   No valid items for container {container}')
            results['failed'] += 1
            continue

        # Determine dates
        order_date = items[0].get('order_date') or datetime.now().strftime('%Y-%m-%d')
        schedule_date = items[0].get('eta') or items[0].get('etd') or order_date

        # Build PO data
        po_data = {
            'doctype': 'Purchase Order',
            'supplier': supplier_id,
            'company': client.company_name,
            'transaction_date': order_date,
            'schedule_date': schedule_date,
            'items': po_items,
        }

        # Link to container if exists
        if container != 'NO_CONTAINER':
            container_exists = client.get_container(container)
            if container_exists:
                po_data['custom_container'] = container

        # Set ETD/ETA in remarks
        etd = items[0].get('etd', '')
        eta = items[0].get('eta', '')
        if etd or eta:
            remarks = []
            if etd:
                remarks.append(f'ETD: {etd}')
            if eta:
                remarks.append(f'ETA: {eta}')
            po_data['remarks'] = ' | '.join(remarks)

        try:
            print(f'   Creating PO with {len(po_items)} items...')
            response = client.create_purchase_order(po_data)

            if response.get('data', {}).get('name'):
                po_name = response['data']['name']
                results['created'] += 1
                print(f'   Created: {po_name}')

                # Submit the PO
                submit_response = client.submit_purchase_order(po_name)
                if submit_response.get('data', {}).get('docstatus') == 1:
                    results['submitted'] += 1
                    print(f'   Submitted: {po_name}')
                else:
                    error = submit_response.get('error', 'Unknown error')
                    print(f'   WARNING: Created but failed to submit: {error}')
            else:
                error = response.get('error', 'Unknown error')
                results['errors'].append({
                    'container': container,
                    'error': str(error)[:200]
                })
                results['failed'] += 1
                print(f'   ERROR: Failed to create PO: {str(error)[:100]}')

        except requests.exceptions.Timeout:
            results['errors'].append({
                'container': container,
                'error': 'Request timeout'
            })
            results['failed'] += 1
            print(f'   ERROR: Timeout for container {container}')

        except requests.exceptions.RequestException as e:
            results['errors'].append({
                'container': container,
                'error': f'Network error: {type(e).__name__}'
            })
            results['failed'] += 1
            print(f'   ERROR: Network error for {container}: {type(e).__name__}')

        # Rate limiting
        time.sleep(0.5)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-64: Purchase Order Migration')
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

    print('\n3. Reading Order Status sheet...')
    orders, skipped = read_order_status(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(orders)} order lines')
    print(f'   Skipped {len(skipped)} lines (zero/negative qty)')

    if not orders:
        print('\nNo orders to import. Exiting.')
        sys.exit(0)

    # Group by container
    orders_by_container = group_by_container(orders)
    print(f'\n   Grouped into {len(orders_by_container)} containers/POs')

    # Show container distribution
    print('\n   Container distribution:')
    for container, items in sorted(orders_by_container.items())[:10]:
        print(f'      {container}: {len(items)} items')
    if len(orders_by_container) > 10:
        print(f'      ... and {len(orders_by_container) - 10} more')

    # Pre-fetch all items
    print('\n4. Pre-fetching item data...')
    all_item_codes = list(set(order['item_code'] for order in orders))
    item_data_map = erpnext.get_items_batch(all_item_codes)
    print(f'   Fetched {len(item_data_map)} items from ERPNext')

    print('\n5. Creating Purchase Orders...')
    results = create_purchase_orders(erpnext, orders_by_container, item_data_map)

    print('\n' + '=' * 60)
    print('PURCHASE ORDER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'POs Created:   {results["created"]}')
    print(f'POs Submitted: {results["submitted"]}')
    print(f'POs Skipped:   {results["skipped"]} (already exist)')
    print(f'POs Failed:    {results["failed"]}')

    if results['items_missing']:
        unique_missing = list(set(results['items_missing']))
        print(f'\nMissing Items (not in Item master): {len(unique_missing)}')
        for sku in unique_missing[:10]:
            print(f'  - {sku}')
        if len(unique_missing) > 10:
            print(f'  ... and {len(unique_missing) - 10} more')

    if results['errors']:
        print(f'\nErrors ({len(results["errors"])}):')
        for err in results['errors'][:10]:
            print(f'  - {err["container"]}: {err["error"][:80]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'purchase_order_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_order_lines': len(orders),
            'containers': len(orders_by_container),
            'created': results['created'],
            'submitted': results['submitted'],
            'skipped': results['skipped'],
            'failed': results['failed'],
            'items_missing': list(set(results['items_missing'])),
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    has_errors = results['failed'] > 0
    sys.exit(1 if has_errors else 0)


if __name__ == '__main__':
    main()
