#!/usr/bin/env python3
"""
SBS-64: Sales Order Migration Script
Imports Sales Orders from Google Sheets (Despatched, Sales, For Despatch) into ERPNext.

This script:
1. Reads order data from Google Sheets
2. Groups line items by Order No
3. Creates/finds Customer records
4. Creates Sales Orders with proper delivery dates
5. Maps custom fields (container, batch, stock_status, etc.)

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (preferred auth method)
  ERPNEXT_API_SECRET   - ERPNext API secret (preferred auth method)
  ERPNEXT_USERNAME     - ERPNext username (fallback auth, default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (fallback auth)
  ERPNEXT_COMPANY_ABBR - Company abbreviation (auto-detected if not set)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_sales_orders.py [--sheet=despatched|sales|for_despatch] [--limit=N]
"""

import os
import re
import json
import time
import sys
import argparse
import tempfile
from datetime import datetime
from collections import defaultdict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constants
REQUEST_TIMEOUT = 30

# Sheet configurations
SHEET_CONFIG = {
    'despatched': {
        'range': 'Despatched!A2:AQ6000',
        'order_no_col': 1,
        'order_date_col': 2,
        'stock_status_col': 6,
        'customer_name_col': 7,
        'email_col': 8,
        'phone_col': 9,
        'address_col': 10,
        'city_col': 11,
        'postcode_col': 12,
        'country_col': 13,
        'product_name_col': 14,
        'sku_col': 15,
        'qty_col': 16,
        'amount_col': 18,
        'batch_col': 19,
        'container_col': 21,
        'eta_col': 22,
        'warehouse_notes_col': 26,
        'date_delivered_col': 33,
    },
    'sales': {
        'range': 'Sales!A2:AK500',
        'order_no_col': 1,
        'order_date_col': 2,
        'stock_status_col': 6,
        'customer_name_col': 7,
        'email_col': 8,
        'phone_col': 9,
        'address_col': 10,
        'city_col': 11,
        'postcode_col': 12,
        'country_col': 13,
        'product_name_col': 14,
        'sku_col': 15,
        'qty_col': 16,
        'amount_col': 18,
        'batch_col': 19,
        'container_col': 21,
        'eta_col': 22,
        'warehouse_notes_col': 26,
        'date_delivered_col': 35,
    },
    'for_despatch': {
        'range': 'For Despatch!A2:AO500',
        'order_no_col': 1,
        'order_date_col': 2,
        'stock_status_col': 6,
        'customer_name_col': 7,
        'email_col': 8,
        'phone_col': 9,
        'address_col': 10,
        'city_col': 11,
        'postcode_col': 12,
        'country_col': 13,
        'product_name_col': 14,
        'sku_col': 15,
        'qty_col': 16,
        'amount_col': 18,
        'batch_col': 19,
        'container_col': 21,
        'eta_col': 22,
        'warehouse_notes_col': 26,
        'date_delivered_col': None,  # For Despatch doesn't have Date Delivered
    },
}


def get_config():
    """Load configuration from environment variables"""
    config = {
        'erpnext': {
            'url': os.environ.get('ERPNEXT_URL'),
            'api_key': os.environ.get('ERPNEXT_API_KEY'),
            'api_secret': os.environ.get('ERPNEXT_API_SECRET'),
            'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
            'password': os.environ.get('ERPNEXT_PASSWORD'),
            'company_abbr': os.environ.get('ERPNEXT_COMPANY_ABBR'),
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
    # Require either API key/secret pair OR password
    has_api_key = config['erpnext']['api_key'] and config['erpnext']['api_secret']
    has_password = config['erpnext']['password']
    if not has_api_key and not has_password:
        missing.append('ERPNEXT_API_KEY+ERPNEXT_API_SECRET or ERPNEXT_PASSWORD')
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

    def __init__(self, url, api_key=None, api_secret=None, username=None, password=None, company_abbr=None):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.api_key = api_key
        self.api_secret = api_secret

        # Prefer API token auth, fallback to password auth
        if api_key and api_secret:
            self.session.headers['Authorization'] = f'token {api_key}:{api_secret}'
            print(f'Using API token authentication')
        elif username and password:
            self.login(username, password)
        else:
            raise Exception('Either API key/secret or username/password required')

        # Use provided abbreviation or auto-detect
        if company_abbr:
            self.company_abbr = company_abbr
            self.company_name = self._get_company_name_by_abbr(company_abbr)
        else:
            self.company_name, self.company_abbr = self._get_company_info()
        print(f'Using company: {self.company_name} (abbr: {self.company_abbr})')

    def _get_company_name_by_abbr(self, abbr):
        """Get company name by abbreviation"""
        response = self.session.get(
            f'{self.url}/api/resource/Company',
            params={
                'filters': json.dumps([['abbr', '=', abbr]]),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            companies = response.json().get('data', [])
            if companies:
                return companies[0]['name']
        return self._get_company_info()[0]

    def _get_company_info(self):
        """Get the first company's name and abbreviation"""
        response = self.session.get(
            f'{self.url}/api/resource/Company',
            params={'limit_page_length': 1},
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
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Failed to fetch company details for {company_name}')

        company_data = response.json().get('data', {})
        return company_data.get('name'), company_data.get('abbr', 'SBS')

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

    def get_customer(self, customer_name):
        """Get customer by name"""
        # URL encode the customer name
        encoded_name = requests.utils.quote(customer_name, safe='')
        response = self.session.get(
            f'{self.url}/api/resource/Customer/{encoded_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None

    def find_customer_by_email(self, email):
        """Find customer by email"""
        if not email:
            return None
        response = self.session.get(
            f'{self.url}/api/resource/Customer',
            params={
                'filters': json.dumps([['email_id', '=', email]]),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            customers = response.json().get('data', [])
            if customers:
                return customers[0]['name']
        return None

    def create_customer(self, customer_data):
        """Create a new customer"""
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

    def get_item(self, item_code):
        """Get item by code"""
        encoded_code = requests.utils.quote(item_code, safe='')
        response = self.session.get(
            f'{self.url}/api/resource/Item/{encoded_code}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None

    def sales_order_exists(self, order_name):
        """Check if Sales Order exists"""
        encoded_name = requests.utils.quote(order_name, safe='')
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order/{encoded_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_sales_order(self, order_data):
        """Create a Sales Order"""
        response = self.session.post(
            f'{self.url}/api/resource/Sales Order',
            json=order_data,
            headers={'Content-Type': 'application/json'},
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

    def get_container(self, container_name):
        """Get container by name"""
        if not container_name:
            return None
        encoded_name = requests.utils.quote(container_name, safe='')
        response = self.session.get(
            f'{self.url}/api/resource/Container/{encoded_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None


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


def parse_date(value):
    """Parse date from various formats"""
    if not value:
        return None
    value = str(value).strip()

    # Try various date formats
    formats = [
        '%d-%b-%Y',      # 03-Dec-2025
        '%d-%b-%y',      # 03-Dec-25
        '%d/%m/%Y',      # 03/12/2025
        '%d/%m/%y',      # 03/12/25
        '%Y-%m-%d',      # 2025-12-03
        '%d-%m-%Y',      # 03-12-2025
        '%d %b %Y',      # 03 Dec 2025
        '%B %d, %Y',     # December 03, 2025
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def normalize_order_no(order_no):
    """Normalize order number to consistent format"""
    if not order_no:
        return None
    order_no = str(order_no).strip()
    # Remove common prefixes
    order_no = re.sub(r'^(R|SBS-)', '', order_no, flags=re.IGNORECASE)
    # Remove slashes
    order_no = order_no.replace('/', '')
    return order_no


def read_orders_from_sheet(service, spreadsheet_id, sheet_config):
    """Read orders from a Google Sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_config['range']
    ).execute()

    rows = result.get('values', [])
    orders = defaultdict(lambda: {
        'customer': {},
        'items': [],
        'meta': {}
    })

    for row in rows:
        def get_col(idx):
            return row[idx] if idx is not None and idx < len(row) else ''

        order_no = clean_text(get_col(sheet_config['order_no_col']))
        if not order_no:
            continue

        normalized_order_no = normalize_order_no(order_no)
        if not normalized_order_no:
            continue

        # Customer data
        customer_name = clean_text(get_col(sheet_config['customer_name_col']))
        if customer_name and not orders[normalized_order_no]['customer'].get('name'):
            orders[normalized_order_no]['customer'] = {
                'name': customer_name,
                'email': clean_text(get_col(sheet_config['email_col'])),
                'phone': clean_text(get_col(sheet_config['phone_col'])),
                'address': clean_text(get_col(sheet_config['address_col'])),
                'city': clean_text(get_col(sheet_config['city_col'])),
                'postcode': clean_text(get_col(sheet_config['postcode_col'])),
                'country': clean_text(get_col(sheet_config['country_col'])),
            }

        # Order metadata
        if not orders[normalized_order_no]['meta'].get('order_date'):
            orders[normalized_order_no]['meta'] = {
                'original_order_no': order_no,
                'order_date': parse_date(get_col(sheet_config['order_date_col'])),
                'stock_status': clean_text(get_col(sheet_config['stock_status_col'])),
                'container': clean_text(get_col(sheet_config['container_col'])),
                'eta': parse_date(get_col(sheet_config['eta_col'])),
                'warehouse_notes': clean_text(get_col(sheet_config['warehouse_notes_col'])),
                'date_delivered': parse_date(get_col(sheet_config['date_delivered_col'])) if sheet_config['date_delivered_col'] else None,
                'batch': clean_text(get_col(sheet_config['batch_col'])),
            }

        # Line item
        sku = clean_text(get_col(sheet_config['sku_col']))
        if sku:
            orders[normalized_order_no]['items'].append({
                'item_code': sku,
                'item_name': clean_text(get_col(sheet_config['product_name_col'])),
                'qty': clean_float(get_col(sheet_config['qty_col'])) or 1,
                'rate': clean_float(get_col(sheet_config['amount_col'])),
                'batch': clean_text(get_col(sheet_config['batch_col'])),
            })

    return dict(orders)


def ensure_customer(client, customer_data):
    """Ensure customer exists, create if not"""
    if not customer_data.get('name'):
        return None

    # Try to find by email first
    if customer_data.get('email'):
        existing = client.find_customer_by_email(customer_data['email'])
        if existing:
            return existing

    # Try to find by name
    existing = client.get_customer(customer_data['name'])
    if existing:
        return existing['name']

    # Create new customer
    customer_type = 'Company' if any(kw in customer_data['name'].lower() for kw in
        ['ltd', 'limited', 'inc', 'corp', 'gmbh', 'bv', 'llc', 'c/o']) else 'Individual'

    new_customer = {
        'customer_name': customer_data['name'],
        'customer_type': customer_type,
        'customer_group': 'All Customer Groups',
        'territory': 'All Territories',
    }
    if customer_data.get('email'):
        new_customer['email_id'] = customer_data['email']
    if customer_data.get('phone'):
        new_customer['mobile_no'] = customer_data['phone']

    result = client.create_customer(new_customer)
    if result.get('data', {}).get('name'):
        print(f"    Created customer: {result['data']['name']}")
        return result['data']['name']
    else:
        print(f"    Warning: Failed to create customer {customer_data['name']}: {result.get('error')}")
        return None


def create_sales_order(client, order_no, order_data):
    """Create a Sales Order in ERPNext"""
    # Check if already exists
    if client.sales_order_exists(f"SO-{order_no}"):
        return {'skipped': True, 'reason': 'Already exists'}

    customer_name = ensure_customer(client, order_data['customer'])
    if not customer_name:
        return {'error': 'No customer'}

    meta = order_data['meta']

    # Prepare items
    items = []
    for item in order_data['items']:
        # Check if item exists
        existing_item = client.get_item(item['item_code'])
        if not existing_item:
            print(f"    Warning: Item {item['item_code']} not found, skipping line")
            continue

        item_data = {
            'item_code': item['item_code'],
            'qty': item['qty'],
            'rate': item['rate'] if item['rate'] > 0 else existing_item.get('standard_rate', 0),
            'delivery_date': meta.get('date_delivered') or meta.get('order_date') or datetime.now().strftime('%Y-%m-%d'),
        }
        items.append(item_data)

    if not items:
        return {'error': 'No valid items'}

    # Determine delivery date
    delivery_date = meta.get('date_delivered')
    if not delivery_date:
        delivery_date = meta.get('eta')
    if not delivery_date:
        delivery_date = meta.get('order_date')
    if not delivery_date:
        delivery_date = datetime.now().strftime('%Y-%m-%d')

    # Build Sales Order
    so_data = {
        'naming_series': 'SAL-ORD-.YYYY.-',
        'customer': customer_name,
        'company': client.company_name,
        'transaction_date': meta.get('order_date') or datetime.now().strftime('%Y-%m-%d'),
        'delivery_date': delivery_date,
        'items': items,
    }

    # Add custom fields if available
    if meta.get('stock_status'):
        so_data['custom_stock_status'] = meta['stock_status']
    if meta.get('container'):
        # Check if container exists
        container = client.get_container(meta['container'])
        if container:
            so_data['custom_allocated_container'] = meta['container']
    if meta.get('eta'):
        so_data['custom_container_eta'] = meta['eta']
    if meta.get('warehouse_notes'):
        so_data['custom_warehouse_notes'] = meta['warehouse_notes']

    result = client.create_sales_order(so_data)
    return result


def main():
    """Main migration function"""
    parser = argparse.ArgumentParser(description='Migrate Sales Orders from Google Sheets to ERPNext')
    parser.add_argument('--sheet', choices=['despatched', 'sales', 'for_despatch'], default='despatched',
                        help='Which sheet to import from (default: despatched)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of orders to import (0=all)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be imported without making changes')
    args = parser.parse_args()

    print('=' * 60)
    print(f'SBS-64: Sales Order Migration ({args.sheet})')
    print('=' * 60)

    config = get_config()
    sheet_config = SHEET_CONFIG[args.sheet]

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        url=config['erpnext']['url'],
        api_key=config['erpnext']['api_key'],
        api_secret=config['erpnext']['api_secret'],
        username=config['erpnext']['username'],
        password=config['erpnext']['password'],
        company_abbr=config['erpnext']['company_abbr']
    )

    print('\n2. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print(f'\n3. Reading {args.sheet} sheet...')
    orders = read_orders_from_sheet(
        sheets_service,
        config['google_sheets']['spreadsheet_id'],
        sheet_config
    )
    print(f'   Found {len(orders)} unique orders')

    if args.limit > 0:
        order_keys = list(orders.keys())[:args.limit]
        orders = {k: orders[k] for k in order_keys}
        print(f'   Limited to {len(orders)} orders')

    if args.dry_run:
        print('\n4. DRY RUN - Would create these orders:')
        for order_no, order_data in list(orders.items())[:10]:
            print(f'   - {order_no}: {order_data["customer"].get("name", "Unknown")} ({len(order_data["items"])} items)')
            if order_data['meta'].get('date_delivered'):
                print(f'     Delivery Date: {order_data["meta"]["date_delivered"]}')
        if len(orders) > 10:
            print(f'   ... and {len(orders) - 10} more')
        print('\nRun without --dry-run to create orders.')
        sys.exit(0)

    print('\n4. Creating Sales Orders...')
    results = {
        'created': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }

    for idx, (order_no, order_data) in enumerate(orders.items(), 1):
        customer_name = order_data['customer'].get('name', 'Unknown')
        print(f'\n[{idx}/{len(orders)}] Order {order_no} - {customer_name}')
        print(f'   Items: {len(order_data["items"])}, Date Delivered: {order_data["meta"].get("date_delivered", "Not set")}')

        try:
            result = create_sales_order(client, order_no, order_data)

            if result.get('skipped'):
                print(f'   SKIPPED: {result.get("reason")}')
                results['skipped'] += 1
            elif result.get('data', {}).get('name'):
                print(f'   CREATED: {result["data"]["name"]}')
                results['created'] += 1
            else:
                error = result.get('error', 'Unknown error')
                print(f'   FAILED: {error[:100]}')
                results['failed'] += 1
                results['errors'].append({
                    'order_no': order_no,
                    'error': str(error)[:200]
                })

        except Exception as e:
            print(f'   ERROR: {str(e)[:100]}')
            results['failed'] += 1
            results['errors'].append({
                'order_no': order_no,
                'error': str(e)[:200]
            })

        # Rate limiting
        time.sleep(0.5)

    print('\n' + '=' * 60)
    print('SALES ORDER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Skipped: {results["skipped"]} (already exist)')
    print(f'Failed:  {results["failed"]}')

    if results['errors']:
        print(f'\nErrors ({len(results["errors"])}):')
        for err in results['errors'][:10]:
            print(f'  - {err["order_no"]}: {err["error"][:80]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'sales_order_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'sheet': args.sheet,
            'total_orders': len(orders),
            'results': results
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
