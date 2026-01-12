#!/usr/bin/env python3
"""
SBS-61: Purchase Orders Migration Script
Imports Purchase Orders from Google Sheets into ERPNext

Creates one PO per Supplier + Container combination.

Data Sources:
- Inventory sheet: Column C (SBS SKU), Column O (CONTAINER)
- 4orm4 sheet: Column C (SBS SKU), Column R (SUPPLIER), Column F (COST)
- Container Status sheet: ETA, destination warehouse

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
BATCH_SIZE = 50

# Default warehouse for containers without mapping
DEFAULT_WAREHOUSE = 'Stores - SBS'

# Warehouse mapping for container destinations
WAREHOUSE_MAPPING = {
    'Marone Solutions Ltd': 'Stock In Warehouse UK MAR - SBS',
    'Edward\'s Furniture Solutions Ltd': 'Stock In Warehouse UK FSL - SBS',
    'Primary OFS': 'Stock In Warehouse UK PRIM - SBS',
    'Transportes Grau': 'Stock In Warehouse ES - SBS',
    'FSL': 'Stock In Warehouse UK FSL - SBS',
    'MAR': 'Stock In Warehouse UK MAR - SBS',
    'PRIM': 'Stock In Warehouse UK PRIM - SBS',
    'ES': 'Stock In Warehouse ES - SBS',
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

    def get_supplier(self, supplier_name):
        """Get a Supplier by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Supplier/{supplier_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def create_supplier(self, supplier_name):
        """Create a new Supplier"""
        data = {
            'supplier_name': supplier_name,
            'supplier_group': 'All Supplier Groups',
            'supplier_type': 'Company'
        }
        response = self.session.post(
            f'{self.url}/api/resource/Supplier',
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

    def get_purchase_order_by_container_supplier(self, container_name, supplier_name):
        """Find existing PO by container and supplier"""
        filters = json.dumps([
            ['custom_container', '=', container_name],
            ['supplier', '=', supplier_name],
            ['docstatus', '!=', 2]  # Not cancelled
        ])
        response = self.session.get(
            f'{self.url}/api/resource/Purchase Order',
            params={
                'filters': filters,
                'fields': json.dumps(['name', 'docstatus']),
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

    def create_purchase_order(self, data):
        """Create a Purchase Order"""
        response = self.session.post(
            f'{self.url}/api/resource/Purchase Order',
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

    def update_purchase_order(self, po_name, data):
        """Update an existing Purchase Order"""
        response = self.session.put(
            f'{self.url}/api/resource/Purchase Order/{po_name}',
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


def parse_date(value):
    """Parse date from various formats"""
    if not value:
        return None

    date_formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%d.%m.%Y',
    ]

    cleaned = str(value).strip()
    for fmt in date_formats:
        try:
            return datetime.strptime(cleaned, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def resolve_warehouse(shipped_to, location=None):
    """Map container destination to ERPNext warehouse"""
    if not shipped_to:
        return DEFAULT_WAREHOUSE

    shipped_to = shipped_to.strip()

    # Check direct mapping
    if shipped_to in WAREHOUSE_MAPPING:
        return WAREHOUSE_MAPPING[shipped_to]

    # Try partial match
    for key, warehouse in WAREHOUSE_MAPPING.items():
        if key.lower() in shipped_to.lower():
            return warehouse

    # Use location-based default
    if location:
        location = location.upper()
        if 'SPAIN' in location or 'ES' in location:
            return 'Stock In Warehouse ES - SBS'
        if 'UK' in location:
            return 'Stock In Warehouse UK MAR - SBS'

    return DEFAULT_WAREHOUSE


def read_inventory_items(service, spreadsheet_id):
    """Read inventory items with container assignments

    Returns dict: {sku: {container: container_name, qty: qty}}
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Inventory!A2:O5000'
    ).execute()

    rows = result.get('values', [])
    items = {}

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))  # Col C: SBS SKU
        container = clean_text(get_col(14))  # Col O: CONTAINER
        qty = clean_float(get_col(6))  # Col G: QTY

        if not sku or not container:
            continue

        if qty <= 0:
            continue

        # Store item with container reference
        key = (sku, container)
        if key not in items:
            items[key] = {
                'sku': sku,
                'container': container,
                'qty': qty
            }
        else:
            items[key]['qty'] += qty

    return list(items.values())


def read_4orm4_suppliers(service, spreadsheet_id):
    """Read supplier and pricing info from 4orm4 sheet

    Returns dict: {sku: {supplier: name, cost: price}}
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='4orm4!A2:T5000'
    ).execute()

    rows = result.get('values', [])
    suppliers = {}

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))  # Col C: SBS SKU
        supplier = clean_text(get_col(17))  # Col R: SUPPLIER
        cost = clean_float(get_col(5))  # Col F: COST

        if not sku:
            continue

        suppliers[sku] = {
            'supplier': supplier if supplier else 'Unknown Supplier',
            'cost': cost
        }

    return suppliers


def read_container_status(service, spreadsheet_id):
    """Read container info from Container Status sheet

    Returns dict: {container_name: {eta, shipped_to, location}}
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Container Status!A2:W500'
    ).execute()

    rows = result.get('values', [])
    containers = {}

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        container_name = clean_text(get_col(0))  # Col A: CONTAINER
        shipped_to = clean_text(get_col(3))  # Col D: Shipped to
        eta = parse_date(get_col(7))  # Col H: ETA
        location = clean_text(get_col(21))  # Col V: LOCATION

        if not container_name:
            continue

        containers[container_name] = {
            'shipped_to': shipped_to,
            'eta': eta,
            'location': location,
            'warehouse': resolve_warehouse(shipped_to, location)
        }

    return containers


def group_by_supplier_container(inventory_items, supplier_map):
    """Group items by (supplier, container) tuple"""
    groups = defaultdict(list)
    unknown_skus = []

    for item in inventory_items:
        sku = item['sku']
        container = item['container']

        supplier_info = supplier_map.get(sku)
        if not supplier_info:
            unknown_skus.append(sku)
            supplier_name = 'Unknown Supplier'
            cost = 0.0
        else:
            supplier_name = supplier_info['supplier']
            cost = supplier_info['cost']

        key = (supplier_name, container)
        groups[key].append({
            'item_code': sku,
            'qty': item['qty'],
            'rate': cost
        })

    return dict(groups), unknown_skus


def ensure_supplier(client, supplier_name):
    """Ensure supplier exists, create if not"""
    existing = client.get_supplier(supplier_name)
    if existing:
        return supplier_name

    response = client.create_supplier(supplier_name)
    if response.get('data', {}).get('name'):
        return response['data']['name']

    return None


def create_purchase_orders(client, grouped_items, container_info):
    """Create Purchase Orders for each supplier+container group"""
    results = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }

    total = len(grouped_items)
    today = datetime.now().strftime('%Y-%m-%d')

    for i, ((supplier_name, container_name), items) in enumerate(grouped_items.items(), 1):
        po_identifier = f"{supplier_name[:20]}|{container_name}"
        print(f'[{i}/{total}] Processing PO: {po_identifier}')

        # Get container details
        container = container_info.get(container_name, {})
        schedule_date = container.get('eta') or today
        warehouse = container.get('warehouse', DEFAULT_WAREHOUSE)

        try:
            # Ensure supplier exists
            actual_supplier = ensure_supplier(client, supplier_name)
            if not actual_supplier:
                results['failed'] += 1
                results['errors'].append({
                    'identifier': po_identifier,
                    'error': f'Failed to create supplier: {supplier_name}'
                })
                print(f'   ERROR: Failed to create supplier {supplier_name}')
                continue

            # Check for existing PO
            existing_po = client.get_purchase_order_by_container_supplier(
                container_name, actual_supplier
            )

            # Prepare PO items
            po_items = []
            for item in items:
                po_items.append({
                    'item_code': item['item_code'],
                    'qty': item['qty'],
                    'rate': item['rate'],
                    'warehouse': warehouse,
                    'schedule_date': schedule_date
                })

            po_data = {
                'supplier': actual_supplier,
                'transaction_date': today,
                'schedule_date': schedule_date,
                'company': COMPANY,
                'custom_container': container_name,
                'custom_destination_warehouse': warehouse,
                'items': po_items
            }

            if existing_po:
                # Update existing (only if draft)
                if existing_po.get('docstatus') == 0:
                    response = client.update_purchase_order(existing_po['name'], po_data)
                    if response.get('data', {}).get('name'):
                        results['updated'] += 1
                        print(f'   Updated: {existing_po["name"]}')
                    else:
                        error = response.get('error', 'Unknown error')
                        results['failed'] += 1
                        results['errors'].append({
                            'identifier': po_identifier,
                            'error': f'Update failed: {error}'
                        })
                        print(f'   ERROR: Update failed: {error}')
                else:
                    results['skipped'] += 1
                    print(f'   Skipped: {existing_po["name"]} (already submitted)')
            else:
                # Create new PO
                response = client.create_purchase_order(po_data)
                if response.get('data', {}).get('name'):
                    results['created'] += 1
                    print(f'   Created: {response["data"]["name"]}')
                else:
                    error = response.get('exception') or response.get('message') or response.get('error', 'Unknown error')
                    results['failed'] += 1
                    results['errors'].append({
                        'identifier': po_identifier,
                        'error': f'Create failed: {str(error)[:150]}'
                    })
                    print(f'   ERROR: {str(error)[:100]}')

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'identifier': po_identifier,
                'error': 'Request timeout'
            })
            print(f'   ERROR: Timeout')

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'identifier': po_identifier,
                'error': f'Network error: {type(e).__name__}'
            })
            print(f'   ERROR: Network error: {type(e).__name__}')

        # Rate limiting
        if i % BATCH_SIZE == 0:
            print(f'   Processed {i}/{total}, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-61: Purchase Orders Migration')
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

    print('\n3. Reading Inventory items with containers...')
    inventory_items = read_inventory_items(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(inventory_items)} item-container pairs')

    print('\n4. Reading supplier info from 4orm4...')
    supplier_map = read_4orm4_suppliers(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(supplier_map)} SKUs with supplier info')

    print('\n5. Reading Container Status...')
    container_info = read_container_status(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(container_info)} containers')

    print('\n6. Grouping items by Supplier + Container...')
    grouped_items, unknown_skus = group_by_supplier_container(inventory_items, supplier_map)
    print(f'   Created {len(grouped_items)} supplier-container groups')
    if unknown_skus:
        print(f'   Warning: {len(unknown_skus)} SKUs without supplier info')

    if not grouped_items:
        print('\nNo items to process. Exiting.')
        sys.exit(0)

    print(f'\n7. Creating {len(grouped_items)} Purchase Orders...')
    results = create_purchase_orders(erpnext, grouped_items, container_info)

    print('\n' + '=' * 60)
    print('PURCHASE ORDERS MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created:  {results["created"]}')
    print(f'Updated:  {results["updated"]}')
    print(f'Skipped:  {results["skipped"]}')
    print(f'Failed:   {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["identifier"]}: {err["error"][:80]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'po_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'inventory_items': len(inventory_items),
            'supplier_map_entries': len(supplier_map),
            'containers': len(container_info),
            'po_groups': len(grouped_items),
            'unknown_skus': unknown_skus[:50],
            'created': results['created'],
            'updated': results['updated'],
            'skipped': results['skipped'],
            'failed': results['failed'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
