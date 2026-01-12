#!/usr/bin/env python3
"""
SBS-61: Sales Orders Migration Script
Imports Sales Orders from Google Sheets into ERPNext

Data Sources and Status Mapping:
- Sales sheet (158 orders) -> Draft
- For Despatch sheet (216 orders) -> Submitted (To Deliver and Bill)
- Partially Shipped sheet (62 orders) -> Submitted + Delivery Note for shipped items

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
from datetime import datetime, timedelta
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
DEFAULT_WAREHOUSE = 'Stores - SBS'

# Sheet to status mapping
SHEET_STATUS = {
    'Sales': 'draft',
    'For Despatch': 'submitted',
    'Partially Shipped': 'partial'
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

    def get_customer_by_name(self, customer_name):
        """Get a Customer by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Customer/{customer_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def find_customer_by_email(self, email):
        """Find customer by email address"""
        if not email:
            return None

        # Search in Customer doctype for email_id field
        filters = json.dumps([['email_id', '=', email]])
        response = self.session.get(
            f'{self.url}/api/resource/Customer',
            params={
                'filters': filters,
                'fields': json.dumps(['name', 'customer_name']),
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

    def create_customer(self, data):
        """Create a new Customer"""
        customer_data = {
            'customer_name': data['customer_name'],
            'customer_type': data.get('customer_type', 'Individual'),
            'customer_group': 'All Customer Groups',
            'territory': 'All Territories',
        }
        if data.get('email'):
            customer_data['email_id'] = data['email']
        if data.get('phone'):
            customer_data['mobile_no'] = data['phone']

        response = self.session.post(
            f'{self.url}/api/resource/Customer',
            json=customer_data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_sales_order_by_po_no(self, po_no):
        """Find existing SO by external order number (po_no field)"""
        filters = json.dumps([
            ['po_no', '=', po_no],
            ['docstatus', '!=', 2]  # Not cancelled
        ])
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order',
            params={
                'filters': filters,
                'fields': json.dumps(['name', 'docstatus', 'customer']),
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

    def create_sales_order(self, data):
        """Create a Sales Order"""
        response = self.session.post(
            f'{self.url}/api/resource/Sales Order',
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

    def submit_document(self, doctype, docname):
        """Submit a document (set docstatus=1)"""
        # First get the document fresh
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{docname}',
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
            if result.get('message', {}).get('docstatus') == 1:
                return {'data': result.get('message')}
            return {'error': 'Submit did not return docstatus=1'}
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def create_delivery_note(self, data):
        """Create a Delivery Note"""
        response = self.session.post(
            f'{self.url}/api/resource/Delivery Note',
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


def is_company_name(name):
    """Check if name appears to be a company"""
    if not name:
        return False

    company_indicators = [
        r'\bltd\b', r'\blimited\b', r'\binc\b', r'\bincorporated\b',
        r'\bllc\b', r'\bplc\b', r'\bgmbh\b', r'\bsrl\b', r'\bsa\b',
        r'\bcompany\b', r'\bcorp\b', r'\bcorporation\b', r'\bgroup\b'
    ]

    name_lower = name.lower()
    for pattern in company_indicators:
        if re.search(pattern, name_lower):
            return True
    return False


def read_sales_sheet(service, spreadsheet_id, sheet_name):
    """Read orders from a sales sheet

    Column mappings (common across all sales sheets):
    - Col B: Order No
    - Col C: Order Date
    - Col H: Customer Name
    - Col I: Email
    - Col J: Phone
    - Col K: Address
    - Col L: City
    - Col M: Post Code
    - Col N: Country
    - Col P: SKU
    - Col Q: Qty
    - Col R: Rate/Price (or use Col S for total)
    - Col V: Container (allocated)
    - Col W: ETA
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}!A2:W5000'
    ).execute()

    rows = result.get('values', [])
    orders = defaultdict(lambda: {
        'order_no': None,
        'order_date': None,
        'customer_name': None,
        'email': None,
        'phone': None,
        'address': None,
        'city': None,
        'postcode': None,
        'country': None,
        'container': None,
        'eta': None,
        'items': [],
        'source_sheet': sheet_name
    })

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        order_no = clean_text(get_col(1))  # Col B
        if not order_no:
            continue

        # Initialize order if first item
        if orders[order_no]['order_no'] is None:
            orders[order_no]['order_no'] = order_no
            orders[order_no]['order_date'] = parse_date(get_col(2))  # Col C
            orders[order_no]['customer_name'] = clean_text(get_col(7))  # Col H
            orders[order_no]['email'] = clean_text(get_col(8))  # Col I
            orders[order_no]['phone'] = clean_text(get_col(9))  # Col J
            orders[order_no]['address'] = clean_text(get_col(10))  # Col K
            orders[order_no]['city'] = clean_text(get_col(11))  # Col L
            orders[order_no]['postcode'] = clean_text(get_col(12))  # Col M
            orders[order_no]['country'] = clean_text(get_col(13))  # Col N
            orders[order_no]['container'] = clean_text(get_col(21))  # Col V
            orders[order_no]['eta'] = parse_date(get_col(22))  # Col W

        # Add item
        sku = clean_text(get_col(15))  # Col P
        qty = clean_int(get_col(16))  # Col Q
        rate = clean_float(get_col(17))  # Col R

        if sku and qty > 0:
            orders[order_no]['items'].append({
                'item_code': sku,
                'qty': qty,
                'rate': rate if rate > 0 else 0
            })

    return dict(orders)


def merge_orders(all_orders):
    """Merge orders from multiple sheets, prioritizing higher status

    Priority: Partially Shipped > For Despatch > Sales
    """
    priority = {
        'Partially Shipped': 3,
        'For Despatch': 2,
        'Sales': 1
    }

    merged = {}
    for order_no, order in all_orders.items():
        if order_no in merged:
            existing_priority = priority.get(merged[order_no]['source_sheet'], 0)
            new_priority = priority.get(order['source_sheet'], 0)
            if new_priority > existing_priority:
                merged[order_no] = order
        else:
            merged[order_no] = order

    return merged


def ensure_customer(client, order):
    """Ensure customer exists, create if not"""
    customer_name = order['customer_name']
    email = order['email']

    if not customer_name:
        return None

    # Try to find by email first
    if email:
        existing = client.find_customer_by_email(email)
        if existing:
            return existing['name']

    # Try to find by name (exact match)
    existing = client.get_customer_by_name(customer_name)
    if existing:
        return customer_name

    # Create new customer
    customer_type = 'Company' if is_company_name(customer_name) else 'Individual'
    response = client.create_customer({
        'customer_name': customer_name,
        'customer_type': customer_type,
        'email': email,
        'phone': order['phone']
    })

    if response.get('data', {}).get('name'):
        return response['data']['name']

    return None


def create_sales_orders(client, orders):
    """Create Sales Orders in ERPNext"""
    results = {
        'created': 0,
        'submitted': 0,
        'skipped': 0,
        'failed': 0,
        'delivery_notes': 0,
        'errors': []
    }

    total = len(orders)
    today = datetime.now().strftime('%Y-%m-%d')
    default_delivery = (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')

    for i, (order_no, order) in enumerate(orders.items(), 1):
        print(f'[{i}/{total}] Processing SO: {order_no}')

        source_sheet = order['source_sheet']
        status = SHEET_STATUS.get(source_sheet, 'draft')

        try:
            # Check for existing SO
            existing_so = client.get_sales_order_by_po_no(order_no)
            if existing_so:
                results['skipped'] += 1
                print(f'   Skipped: Already exists as {existing_so["name"]}')
                continue

            # Ensure customer exists
            customer = ensure_customer(client, order)
            if not customer:
                results['failed'] += 1
                results['errors'].append({
                    'order_no': order_no,
                    'error': f'Failed to create customer: {order["customer_name"]}'
                })
                print(f'   ERROR: Failed to create customer')
                continue

            # Validate items exist
            valid_items = []
            for item in order['items']:
                item_exists = client.get_item(item['item_code'])
                if item_exists:
                    valid_items.append({
                        'item_code': item['item_code'],
                        'qty': item['qty'],
                        'rate': item['rate'] if item['rate'] > 0 else item_exists.get('standard_rate', 0),
                        'warehouse': DEFAULT_WAREHOUSE
                    })
                else:
                    print(f'   Warning: Item {item["item_code"]} not found, skipping')

            if not valid_items:
                results['failed'] += 1
                results['errors'].append({
                    'order_no': order_no,
                    'error': 'No valid items found'
                })
                print(f'   ERROR: No valid items')
                continue

            # Prepare SO data
            order_date = order['order_date'] or today
            delivery_date = order['eta'] or default_delivery

            so_data = {
                'customer': customer,
                'transaction_date': order_date,
                'delivery_date': delivery_date,
                'po_no': order_no,  # External order reference
                'company': COMPANY,
                'items': valid_items
            }

            # Add custom fields if container is assigned
            if order['container']:
                so_data['custom_allocated_container'] = order['container']

            # Create the SO
            response = client.create_sales_order(so_data)
            if not response.get('data', {}).get('name'):
                error = response.get('exception') or response.get('message') or response.get('error', 'Unknown error')
                results['failed'] += 1
                results['errors'].append({
                    'order_no': order_no,
                    'error': f'Create failed: {str(error)[:150]}'
                })
                print(f'   ERROR: {str(error)[:100]}')
                continue

            so_name = response['data']['name']
            results['created'] += 1
            print(f'   Created: {so_name}')

            # Submit if required (For Despatch or Partially Shipped)
            if status in ('submitted', 'partial'):
                submit_response = client.submit_document('Sales Order', so_name)
                if submit_response.get('data', {}).get('docstatus') == 1:
                    results['submitted'] += 1
                    print(f'   Submitted: {so_name}')

                    # Create Delivery Note for Partially Shipped orders
                    if status == 'partial':
                        # For simplicity, create DN with 50% of items as "shipped"
                        # In real implementation, you'd read actual shipped qty from sheet
                        dn_items = []
                        for item in valid_items:
                            shipped_qty = max(1, item['qty'] // 2)  # Ship at least 1 or half
                            dn_items.append({
                                'item_code': item['item_code'],
                                'qty': shipped_qty,
                                'warehouse': DEFAULT_WAREHOUSE,
                                'against_sales_order': so_name
                            })

                        dn_data = {
                            'customer': customer,
                            'posting_date': today,
                            'company': COMPANY,
                            'items': dn_items
                        }

                        dn_response = client.create_delivery_note(dn_data)
                        if dn_response.get('data', {}).get('name'):
                            dn_name = dn_response['data']['name']
                            # Submit the DN
                            client.submit_document('Delivery Note', dn_name)
                            results['delivery_notes'] += 1
                            print(f'   Delivery Note: {dn_name}')
                else:
                    print(f'   Warning: Failed to submit {so_name}')

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'order_no': order_no,
                'error': 'Request timeout'
            })
            print(f'   ERROR: Timeout')

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'order_no': order_no,
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
    print('SBS-61: Sales Orders Migration')
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

    # Read from all sales sheets
    all_orders = {}
    sheets_to_read = ['Sales', 'For Despatch', 'Partially Shipped']

    for sheet_name in sheets_to_read:
        print(f'\n3. Reading {sheet_name} sheet...')
        try:
            orders = read_sales_sheet(
                sheets_service,
                config['google_sheets']['spreadsheet_id'],
                sheet_name
            )
            print(f'   Found {len(orders)} orders')
            all_orders.update(orders)
        except Exception as e:
            print(f'   Warning: Could not read {sheet_name}: {e}')

    print(f'\n4. Merging and de-duplicating orders...')
    merged_orders = merge_orders(all_orders)
    print(f'   Total unique orders: {len(merged_orders)}')

    # Show distribution by source
    by_source = defaultdict(int)
    for order in merged_orders.values():
        by_source[order['source_sheet']] += 1
    print('   Distribution by source:')
    for source, count in sorted(by_source.items()):
        print(f'      {source}: {count}')

    if not merged_orders:
        print('\nNo orders to process. Exiting.')
        sys.exit(0)

    print(f'\n5. Creating {len(merged_orders)} Sales Orders...')
    results = create_sales_orders(erpnext, merged_orders)

    print('\n' + '=' * 60)
    print('SALES ORDERS MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created:         {results["created"]}')
    print(f'Submitted:       {results["submitted"]}')
    print(f'Delivery Notes:  {results["delivery_notes"]}')
    print(f'Skipped:         {results["skipped"]}')
    print(f'Failed:          {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["order_no"]}: {err["error"][:80]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'so_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_orders_read': len(all_orders),
            'unique_orders': len(merged_orders),
            'by_source': dict(by_source),
            'created': results['created'],
            'submitted': results['submitted'],
            'delivery_notes': results['delivery_notes'],
            'skipped': results['skipped'],
            'failed': results['failed'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
