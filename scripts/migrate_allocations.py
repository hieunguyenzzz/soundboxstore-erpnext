#!/usr/bin/env python3
"""
SBS-64: Allocation Migration Script
Migrates Temp Allocation and Allocation data from Google Sheets to ERPNext.

Data Sources:
- Temp Allocation sheet: Temporary reservations (pending invoice) → status="Reserved"
- Allocation sheet: Confirmed allocations (invoice paid) → status="Fulfilled"

Target:
- Container Pre-Allocation DocType in ERPNext

Columns (Allocation sheet, 0-indexed):
- C (2):  Order No. - matches Sales Order po_no field
- D (3):  NAME - customer name
- F (5):  SKU - item_code
- H (7):  BATCH - batch identifier
- J (9):  CONTAINER - container reference
- K (10): QTY - allocated quantity

Columns (Temp Allocation sheet, 0-indexed):
- B (1):  REF - quote/invoice reference
- C (2):  NAME - customer name
- D (3):  SBS SKU - item_code
- F (5):  BATCH - batch identifier
- H (7):  CONTAINER - container reference
- I (8):  QTY - allocated quantity

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS - Path to service account JSON file (required)
  SPREADSHEET_ID     - Google Sheets ID (default: SoundboxStore spreadsheet)

Usage:
  python scripts/migrate_allocations.py
"""

import os
import sys
import json
import tempfile
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Google Sheets API imports
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except ImportError:
    print("ERROR: Google API packages not installed")
    print("Run: pip install google-api-python-client google-auth")
    sys.exit(1)

# Constants
REQUEST_TIMEOUT = 30
DEFAULT_SPREADSHEET_ID = '1NQA7DBzIryCjA0o0dxehLyGmxM8ZeOofpg3IENgtDmA'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def get_config():
    """Load configuration from environment variables"""
    config = {
        'url': os.environ.get('ERPNEXT_URL'),
        'api_key': os.environ.get('ERPNEXT_API_KEY'),
        'api_secret': os.environ.get('ERPNEXT_API_SECRET'),
        'sheets_creds': os.environ.get('GOOGLE_SHEETS_CREDS'),
        'spreadsheet_id': os.environ.get('SPREADSHEET_ID', DEFAULT_SPREADSHEET_ID),
    }

    missing = []
    if not config['url']:
        missing.append('ERPNEXT_URL')
    if not config['api_key']:
        missing.append('ERPNEXT_API_KEY')
    if not config['api_secret']:
        missing.append('ERPNEXT_API_SECRET')
    if not config['sheets_creds']:
        missing.append('GOOGLE_SHEETS_CREDS')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL        - ERPNext server URL (e.g., https://erp.soundboxstore.com)")
        print("  ERPNEXT_API_KEY    - ERPNext API key")
        print("  ERPNEXT_API_SECRET - ERPNext API secret")
        print("  GOOGLE_SHEETS_CREDS - Path to service account JSON file")
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
        self._cache = {
            'sales_orders': {},  # po_no -> name
            'containers': {},    # container_name -> name
            'items': set()       # item_codes that exist
        }

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

    def load_sales_orders(self):
        """Load all Sales Orders and build po_no -> name mapping"""
        print('  Loading Sales Orders...')
        offset = 0
        limit = 500
        while True:
            response = self.session.get(
                f'{self.url}/api/resource/Sales Order',
                params={
                    'fields': '["name","po_no"]',
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code != 200:
                break
            data = response.json().get('data', [])
            if not data:
                break
            for so in data:
                if so.get('po_no'):
                    self._cache['sales_orders'][so['po_no']] = so['name']
            offset += limit
            if len(data) < limit:
                break
        print(f'    Loaded {len(self._cache["sales_orders"])} Sales Orders with po_no')

    def load_containers(self):
        """Load all Containers"""
        print('  Loading Containers...')
        offset = 0
        limit = 500
        while True:
            response = self.session.get(
                f'{self.url}/api/resource/Container',
                params={
                    'fields': '["name"]',
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code != 200:
                break
            data = response.json().get('data', [])
            if not data:
                break
            for c in data:
                self._cache['containers'][c['name']] = c['name']
            offset += limit
            if len(data) < limit:
                break
        print(f'    Loaded {len(self._cache["containers"])} Containers')

    def load_items(self):
        """Load all Items"""
        print('  Loading Items...')
        offset = 0
        limit = 1000
        while True:
            response = self.session.get(
                f'{self.url}/api/resource/Item',
                params={
                    'fields': '["name"]',
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code != 200:
                break
            data = response.json().get('data', [])
            if not data:
                break
            for item in data:
                self._cache['items'].add(item['name'])
            offset += limit
            if len(data) < limit:
                break
        print(f'    Loaded {len(self._cache["items"])} Items')

    def get_sales_order_by_po_no(self, po_no):
        """Get Sales Order name by po_no"""
        return self._cache['sales_orders'].get(po_no)

    def get_container(self, container_name):
        """Get Container name if exists"""
        return self._cache['containers'].get(container_name)

    def item_exists(self, item_code):
        """Check if Item exists"""
        return item_code in self._cache['items']

    def create_preallocation(self, data):
        """Create a Container Pre-Allocation record"""
        response = self.session.post(
            f'{self.url}/api/resource/Container Pre-Allocation',
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

    def preallocation_exists(self, sales_order, item_code, container):
        """Check if preallocation already exists"""
        filters = [
            ["sales_order", "=", sales_order],
            ["item_code", "=", item_code]
        ]
        if container:
            filters.append(["container", "=", container])

        response = self.session.get(
            f'{self.url}/api/resource/Container Pre-Allocation',
            params={
                'filters': json.dumps(filters),
                'limit_page_length': 1
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            return len(data) > 0
        return False


def get_sheets_service(creds_path):
    """Create Google Sheets API service"""
    if os.path.isfile(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    else:
        creds_data = json.loads(creds_path)
        creds = Credentials.from_service_account_info(creds_data, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)


def clean_text(value):
    """Clean and normalize text values"""
    if value is None:
        return ''
    return str(value).strip()


def clean_float(value):
    """Clean and parse float values"""
    if not value:
        return 0.0
    try:
        cleaned = str(value).replace(',', '').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def read_allocations(service, spreadsheet_id):
    """Read confirmed allocations from Allocation sheet.

    Columns (0-indexed):
    - C (2):  Order No. - matches Sales Order po_no field
    - D (3):  NAME - customer name
    - F (5):  SKU - item_code
    - H (7):  BATCH - batch identifier
    - J (9):  CONTAINER - container reference
    - K (10): QTY - allocated quantity
    """
    print('  Reading Allocation sheet...')
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Allocation!A2:L10000'
    ).execute()

    rows = result.get('values', [])
    allocations = []
    skipped = []

    for i, row in enumerate(rows, 2):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        order_no = clean_text(get_col(2))    # Col C: Order No.
        customer = clean_text(get_col(3))    # Col D: NAME
        sku = clean_text(get_col(5))         # Col F: SKU
        batch = clean_text(get_col(7))       # Col H: BATCH
        container = clean_text(get_col(9))   # Col J: CONTAINER
        qty = clean_float(get_col(10))       # Col K: QTY

        if not sku:
            continue

        if qty <= 0:
            skipped.append({
                'row': i,
                'order_no': order_no,
                'sku': sku,
                'reason': f'Zero or negative qty: {qty}'
            })
            continue

        # Skip internal showroom allocations (no real Sales Order)
        if order_no.lower() == 'showroom':
            skipped.append({
                'row': i,
                'order_no': order_no,
                'sku': sku,
                'reason': 'Showroom allocation (internal use)'
            })
            continue

        allocations.append({
            'order_no': order_no,
            'customer': customer,
            'sku': sku,
            'batch': batch,
            'container': container,
            'qty': qty,
            'row': i,
            'source': 'Allocation',
            'status': 'Fulfilled'  # Confirmed allocations
        })

    print(f'    Found {len(allocations)} confirmed allocations (non-zero QTY, non-showroom)')
    print(f'    Skipped {len(skipped)} rows')
    return allocations, skipped


def read_temp_allocations(service, spreadsheet_id):
    """Read temporary allocations from Temp Allocation sheet.

    Columns (0-indexed):
    - B (1):  REF - quote/invoice reference
    - C (2):  NAME - customer name
    - D (3):  SBS SKU - item_code
    - F (5):  BATCH - batch identifier
    - H (7):  CONTAINER - container reference
    - I (8):  QTY - allocated quantity
    """
    print('  Reading Temp Allocation sheet...')
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Temp Allocation!A2:J5000'
    ).execute()

    rows = result.get('values', [])
    allocations = []
    skipped = []

    for i, row in enumerate(rows, 2):
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        ref = clean_text(get_col(1))         # Col B: REF
        customer = clean_text(get_col(2))    # Col C: NAME
        sku = clean_text(get_col(3))         # Col D: SBS SKU
        batch = clean_text(get_col(5))       # Col F: BATCH
        container = clean_text(get_col(7))   # Col H: CONTAINER
        qty = clean_float(get_col(8))        # Col I: QTY

        if not sku:
            continue

        if qty <= 0:
            skipped.append({
                'row': i,
                'ref': ref,
                'sku': sku,
                'reason': f'Zero or negative qty: {qty}'
            })
            continue

        allocations.append({
            'order_no': ref,  # Use REF as order reference for temp allocations
            'customer': customer,
            'sku': sku,
            'batch': batch,
            'container': container,
            'qty': qty,
            'row': i,
            'source': 'Temp Allocation',
            'status': 'Reserved'  # Temporary reservations
        })

    print(f'    Found {len(allocations)} temp allocations (non-zero QTY)')
    print(f'    Skipped {len(skipped)} rows')
    return allocations, skipped


def migrate_allocations(client, allocations):
    """Migrate allocations to ERPNext Container Pre-Allocation"""
    results = {
        'created': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }

    total = len(allocations)
    for i, alloc in enumerate(allocations, 1):
        if i % 100 == 0:
            print(f'    Processing {i}/{total}...')

        order_no = alloc['order_no']
        sku = alloc['sku']
        container = alloc['container']
        qty = alloc['qty']
        status = alloc['status']
        source = alloc['source']

        # Find Sales Order by po_no
        sales_order = client.get_sales_order_by_po_no(order_no)
        if not sales_order:
            results['failed'] += 1
            results['errors'].append({
                'row': alloc['row'],
                'source': source,
                'order_no': order_no,
                'sku': sku,
                'error': f'Sales Order not found for po_no: {order_no}'
            })
            continue

        # Check if item exists
        if not client.item_exists(sku):
            results['failed'] += 1
            results['errors'].append({
                'row': alloc['row'],
                'source': source,
                'order_no': order_no,
                'sku': sku,
                'error': f'Item not found: {sku}'
            })
            continue

        # Find Container (optional - some allocations are FOR MANUFACTURE)
        container_name = None
        if container and container not in ('FOR MANUFACTURE', ''):
            container_name = client.get_container(container)
            if not container_name:
                # Try without space (CONTAINER232 vs CONTAINER 232)
                container_name = client.get_container(container.replace(' ', ''))
            # If still not found, don't fail - just skip container link
            if not container_name:
                container_name = None  # Proceed without container

        # Check if already exists
        if client.preallocation_exists(sales_order, sku, container_name):
            results['skipped'] += 1
            continue

        # Create Container Pre-Allocation
        prealloc_data = {
            'sales_order': sales_order,
            'item_code': sku,
            'qty': qty,
            'status': status,
            'allocation_date': datetime.now().strftime('%Y-%m-%d'),
            'notes': f'Migrated from {source} sheet. Original order: {order_no}'
        }

        if container_name:
            prealloc_data['container'] = container_name

        response = client.create_preallocation(prealloc_data)

        if response.get('data', {}).get('name'):
            results['created'] += 1
        else:
            error = response.get('error', 'Unknown error')
            results['failed'] += 1
            results['errors'].append({
                'row': alloc['row'],
                'source': source,
                'order_no': order_no,
                'sku': sku,
                'error': str(error)[:100]
            })

    return results


def save_report(report_data):
    """Save migration report to temp directory"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'allocation_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
    return report_path


def main():
    """Main function"""
    print('=' * 60)
    print('SBS-64: Allocation Migration')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    print('\n2. Loading reference data from ERPNext...')
    client.load_sales_orders()
    client.load_containers()
    client.load_items()

    print('\n3. Connecting to Google Sheets...')
    service = get_sheets_service(config['sheets_creds'])

    print('\n4. Reading allocation data...')
    allocations, alloc_skipped = read_allocations(service, config['spreadsheet_id'])
    temp_allocations, temp_skipped = read_temp_allocations(service, config['spreadsheet_id'])

    # Combine all allocations
    all_allocations = allocations + temp_allocations
    print(f'\n   Total allocations to migrate: {len(all_allocations)}')
    print(f'   - Confirmed (Allocation sheet): {len(allocations)}')
    print(f'   - Temporary (Temp Allocation sheet): {len(temp_allocations)}')

    print('\n5. Migrating allocations to ERPNext...')
    results = migrate_allocations(client, all_allocations)

    # Summary
    print('\n' + '=' * 60)
    print('ALLOCATION MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created:  {results["created"]}')
    print(f'Skipped:  {results["skipped"]} (already exist)')
    print(f'Failed:   {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - Row {err["row"]} ({err["source"]}): {err["sku"]} - {err["error"][:60]}')

    # Save report
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_allocations': len(all_allocations),
            'confirmed': len(allocations),
            'temporary': len(temp_allocations),
            'created': results['created'],
            'skipped': results['skipped'],
            'failed': results['failed']
        },
        'errors': results['errors'],
        'skipped_rows': {
            'allocation': alloc_skipped,
            'temp_allocation': temp_skipped
        }
    }
    report_path = save_report(report_data)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
