#!/usr/bin/env python3
"""
SBS-64: Sales Order Migration Script
Imports Sales Orders from Google Sheets (Sales, For Despatch, Despatched) into ERPNext.

This script:
1. Reads order data from 3 Google Sheets (removed Partially Shipped)
2. Groups line items by Order No.
3. Creates Sales Orders with proper custom_stock_status
4. Creates and SUBMITS Delivery Notes for completed orders (Despatched sheet)
5. Preserves all dates from the spreadsheet

Stock Status Values:
- FOR MANUFACTURE: Items need to be ordered from supplier
- STOCK COMING: Items on the way (in container)
- FOR DESPATCH: Items in stock, ready to ship
- DESPATCHED: Items have been delivered

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)

Usage:
  python scripts/migrate_sales_orders.py
  python scripts/migrate_sales_orders.py --sheet despatched --limit 10
"""

import os
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
REQUEST_TIMEOUT = 30
BATCH_SIZE = 50

# Sheet configurations with proper stock status mapping
SHEETS = {
    'sales': {
        'name': 'Sales',
        'range': 'Sales!A2:AM5000',  # Extended to include Column AG (Payment Terms)
        'submit_so': True,
        'stock_status': 'FOR MANUFACTURE',  # New orders, items not ordered yet
        'create_delivery_note': False,
        # Column mapping
        'col_payment_terms': 32,  # Column AG - PAYMENT TERMS
        'col_order_amount': 18,   # Column S - Amount (different meaning in Sales)
        'default_payment_terms': None  # No default, use sheet value
    },
    'for_despatch': {
        'name': 'For Despatch',
        'range': 'For Despatch!A2:AM5000',  # Extended to include Column AL (DPS DATE)
        'submit_so': True,
        'stock_status': 'FOR DESPATCH',  # Items in stock, ready to ship
        'create_delivery_note': False,
        # Column mapping
        'col_payment_terms': 32,  # Column AG - PAYMENT TERMS
        'col_order_amount': 18,   # Column S - Amount
        'default_payment_terms': None
    },
    'despatched': {
        'name': 'Despatched',
        'range': 'Despatched!A2:AM10000',
        'submit_so': True,
        'stock_status': 'DESPATCHED',  # Already delivered
        'create_delivery_note': True,
        'submit_delivery_note': True,
        # Column mapping - Despatched has different structure
        'col_payment_terms': None,  # No Payment Terms column in Despatched sheet
        'col_order_amount': 18,     # Column S - Amount
        'default_payment_terms': 'FULLY PAID'  # Assume all despatched orders are paid
    }
}


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
    """ERPNext API Client with token authentication"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }

        # Cache for lookups
        self._customer_cache = {}
        self._item_cache = {}
        self._container_cache = {}
        self._warehouse_cache = {}

        # Verify connection
        self._verify_connection()

        # Get company info
        self.company_name, self.company_abbr = self._get_company_info()
        print(f'Using company: {self.company_name} (abbr: {self.company_abbr})')

    def _verify_connection(self):
        """Verify API connection"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed with status {response.status_code}')
        user = response.json().get('message', 'Unknown')
        print(f'Connected to ERPNext at {self.url} as {user}')

    def _get_company_info(self):
        """Get first company's name and abbreviation"""
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
            raise Exception(f'Failed to fetch company details')

        company_data = response.json().get('data', {})
        return company_data.get('name'), company_data.get('abbr', 'SBS')

    def get_customer(self, customer_name):
        """Get customer by name, returns customer ID if exists"""
        if customer_name in self._customer_cache:
            return self._customer_cache[customer_name]

        response = self.session.get(
            f'{self.url}/api/resource/Customer',
            params={'filters': json.dumps([['customer_name', '=', customer_name]]), 'limit_page_length': 1},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                customer_id = data[0].get('name')
                self._customer_cache[customer_name] = customer_id
                return customer_id

        self._customer_cache[customer_name] = None
        return None

    def get_item(self, item_code):
        """Get item by code, returns item data if exists"""
        if item_code in self._item_cache:
            return self._item_cache[item_code]

        response = self.session.get(
            f'{self.url}/api/resource/Item/{item_code}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            item = response.json().get('data', {})
            self._item_cache[item_code] = item
            return item

        self._item_cache[item_code] = None
        return None

    def get_container(self, container_name):
        """Get container by name"""
        if container_name in self._container_cache:
            return self._container_cache[container_name]

        response = self.session.get(
            f'{self.url}/api/resource/Container/{container_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            self._container_cache[container_name] = container_name
            return container_name

        self._container_cache[container_name] = None
        return None

    def get_warehouse(self, warehouse_name):
        """Get warehouse by name, trying different formats"""
        if warehouse_name in self._warehouse_cache:
            return self._warehouse_cache[warehouse_name]

        # Try with company abbr
        with_abbr = f"{warehouse_name} - {self.company_abbr}"
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{with_abbr}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            self._warehouse_cache[warehouse_name] = with_abbr
            return with_abbr

        # Try exact name
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            self._warehouse_cache[warehouse_name] = warehouse_name
            return warehouse_name

        self._warehouse_cache[warehouse_name] = None
        return None

    def get_default_warehouse(self):
        """Get default warehouse for the company"""
        for name in ['Stores', 'Stock In Warehouse UK FSL', 'Finished Goods']:
            wh = self.get_warehouse(name)
            if wh:
                return wh
        return f"Stores - {self.company_abbr}"

    def find_sales_order_by_po_no(self, po_no):
        """Find Sales Order by po_no (customer's PO number)"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order',
            params={
                'filters': json.dumps([['po_no', '=', po_no]]),
                'limit_page_length': 1
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                return data[0].get('name')
        return None

    def create_sales_order(self, data):
        """Create a Sales Order in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Sales Order',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', err.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}: {response.text[:200]}'}
        return response.json()

    def submit_document(self, doctype, name):
        """Submit a document (change docstatus to 1)"""
        get_response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if get_response.status_code != 200:
            return {'error': f'Could not fetch {doctype} {name} for submission'}

        doc = get_response.json().get('data', {})
        if not doc:
            return {'error': f'Empty document returned for {doctype} {name}'}

        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', err.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        return response.json()

    def create_delivery_note(self, data):
        """Create a Delivery Note in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Delivery Note',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', err.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}: {response.text[:200]}'}
        return response.json()

    def get_sales_order(self, name):
        """Get a Sales Order by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None

    def get_item_rate(self, item_code):
        """Get item standard_rate from cache or fetch it"""
        item = self.get_item(item_code)
        if item:
            return item.get('standard_rate') or item.get('valuation_rate') or 0
        return 0

    def get_default_mode_of_payment(self):
        """Get the first available Mode of Payment"""
        response = self.session.get(
            f'{self.url}/api/resource/Mode of Payment',
            params={'limit_page_length': 1},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                return data[0]['name']
        # Default to Bank Transfer if none found
        return 'Bank Transfer'

    def create_payment_entry(self, data):
        """Create a Payment Entry in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Payment Entry',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', err.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}: {response.text[:200]}'}
        return response.json()

    def get_company_default_account(self, account_type):
        """Get default account for the company"""
        response = self.session.get(
            f'{self.url}/api/resource/Company/{self.company_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            company = response.json().get('data', {})
            if account_type == 'receivable':
                return company.get('default_receivable_account')
            elif account_type == 'bank':
                bank = company.get('default_bank_account')
                if bank:
                    return bank
                # Fallback: find a Cash or Bank account
                return self._find_bank_or_cash_account()
        return None

    def _find_bank_or_cash_account(self):
        """Find first available Bank or Cash leaf account (not a group)"""
        # Find a Cash or Bank account that is NOT a group
        response = self.session.get(
            f'{self.url}/api/resource/Account',
            params={
                'filters': json.dumps([
                    ['account_type', 'in', ['Cash', 'Bank']],
                    ['company', '=', self.company_name],
                    ['is_group', '=', 0]  # Must be a leaf account, not a group
                ]),
                'limit_page_length': 1
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                return data[0]['name']
        return f'Cash - {self.company_abbr}'


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


def parse_qty(value):
    """Parse quantity from string"""
    if not value:
        return 0
    try:
        return float(str(value).strip().replace(',', ''))
    except (ValueError, TypeError):
        return 0


def clean_price(value):
    """Parse price/currency from string like '£1,234.56' or '1234.56'"""
    if not value:
        return 0.0
    # Remove currency symbols and whitespace
    cleaned = str(value).strip()
    cleaned = cleaned.replace('£', '').replace('$', '').replace('€', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def read_orders_from_sheet(service, spreadsheet_id, sheet_config):
    """Read and parse orders from a Google Sheet.

    Column mapping:
    B (1): Order No.
    C (2): Order Date
    H (7): Customer Name
    I (8): Email
    J (9): Phone
    K (10): Address
    L (11): City
    M (12): Pincode
    N (13): Country
    O (14): Product Name
    P (15): SKU
    Q (16): Qty
    V (21): Container
    W (22): ETA
    X (23): Date Delivered (for Despatched sheet)
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_config['range']
        ).execute()
    except Exception as e:
        print(f'   Warning: Could not read sheet: {e}')
        return {}

    rows = result.get('values', [])
    orders = defaultdict(lambda: {
        'items': [],
        'customer_name': '',
        'customer_email': '',
        'order_date': None,
        'delivery_date': None,
        'date_delivered': None,
        'phone': '',
        'address': '',
        'city': '',
        'pincode': '',
        'country': '',
        'container': '',
        'eta': None,
        'dps_date': None,  # DPS DATE from Column AL - target dispatch date for For Despatch orders
        'payment_terms': '',  # Column AG - FULLY PAID, BEFORE DELIVERY, AFTER DELIVERY
        'order_amount': 0.0,  # Column S - Order Amount (currency)
        'source_sheet': sheet_config['name']
    })

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        order_no = clean_text(get_col(1))  # Column B
        if not order_no:
            continue

        # Skip header-like rows
        if order_no.upper() in ['ORDER NO', 'ORDER NO.', 'ORDER NUMBER', 'ORDERNUM']:
            continue

        order = orders[order_no]

        # Customer info (take first non-empty)
        if not order['customer_name']:
            order['customer_name'] = clean_text(get_col(7))  # Column H
        if not order['customer_email']:
            order['customer_email'] = clean_text(get_col(8))  # Column I
        if not order['order_date']:
            order['order_date'] = parse_date(get_col(2))  # Column C
        if not order['phone']:
            order['phone'] = clean_text(get_col(9))  # Column J
        if not order['address']:
            order['address'] = clean_text(get_col(10))  # Column K
        if not order['city']:
            order['city'] = clean_text(get_col(11))  # Column L
        if not order['pincode']:
            order['pincode'] = clean_text(get_col(12))  # Column M
        if not order['country']:
            order['country'] = clean_text(get_col(13)) or 'United Kingdom'  # Column N
        if not order['container']:
            order['container'] = clean_text(get_col(21))  # Column V
        if not order['eta']:
            order['eta'] = parse_date(get_col(22))  # Column W
        # Date delivered - Column X for Sales/For Despatch (index 23), Column AH for Despatched (index 33)
        if not order['date_delivered']:
            # For Despatched sheet, date_delivered is in Column AH (index 33)
            if sheet_config['name'] == 'Despatched':
                order['date_delivered'] = parse_date(get_col(33))  # Column AH - Date Delivered
            else:
                order['date_delivered'] = parse_date(get_col(23))  # Column X
        if not order['dps_date']:
            order['dps_date'] = parse_date(get_col(37))  # Column AL - DPS DATE (target dispatch date)

        # Payment terms - use sheet-specific column or default
        if not order['payment_terms']:
            col_payment_terms = sheet_config.get('col_payment_terms')
            if col_payment_terms is not None:
                order['payment_terms'] = clean_text(get_col(col_payment_terms)).upper()
            elif sheet_config.get('default_payment_terms'):
                order['payment_terms'] = sheet_config.get('default_payment_terms')

        # Order amount - use sheet-specific column
        if order['order_amount'] == 0.0:
            col_order_amount = sheet_config.get('col_order_amount', 18)
            order['order_amount'] = clean_price(get_col(col_order_amount))

        # Line item
        sku = clean_text(get_col(15))  # Column P - SKU
        qty = parse_qty(get_col(16))  # Column Q - Qty
        product_name = clean_text(get_col(14))  # Column O - Product Name

        if sku and qty > 0:
            order['items'].append({
                'item_code': sku,
                'item_name': product_name,
                'qty': qty
            })

    return dict(orders)


def calculate_order_amount(items, client):
    """Calculate order amount from item quantities and rates"""
    total = 0.0
    for item in items:
        rate = client.get_item_rate(item['item_code'])
        total += item['qty'] * rate
    return total


def get_payment_posting_date(payment_terms, order_date, delivery_date):
    """
    Calculate the posting date for Payment Entry based on payment terms.

    Returns:
        str: YYYY-MM-DD format date, or None if can't determine
    """
    if not payment_terms:
        return None

    payment_terms = payment_terms.upper().strip()

    if payment_terms == 'FULLY PAID':
        # Payment made at order time
        return order_date

    elif payment_terms == 'BEFORE DELIVERY':
        # Payment made 1 day before delivery
        if not delivery_date:
            return None
        try:
            dt = datetime.strptime(delivery_date, '%Y-%m-%d')
            return (dt - timedelta(days=1)).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    elif payment_terms == 'AFTER DELIVERY':
        # Payment made 1 day after delivery
        if not delivery_date:
            return None
        try:
            dt = datetime.strptime(delivery_date, '%Y-%m-%d')
            return (dt + timedelta(days=1)).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    return None


def create_payment_entry_for_order(client, so_name, customer_id, amount, posting_date, results):
    """Create a Payment Entry for a Sales Order.

    Uses the Sales Order's actual grand_total from ERPNext to ensure the payment
    amount matches the outstanding amount exactly.
    """
    if not posting_date:
        return None

    # Get the actual Sales Order total from ERPNext
    so_data = client.get_sales_order(so_name)
    if not so_data:
        results['pe_skipped'].append({
            'so_name': so_name,
            'reason': 'Could not fetch Sales Order'
        })
        return None

    # Use rounded_total (preferred) or grand_total from ERPNext
    # ERPNext Payment Entry uses rounded_total for outstanding calculations
    payment_amount = so_data.get('rounded_total') or so_data.get('grand_total') or 0
    if payment_amount <= 0:
        results['pe_skipped'].append({
            'so_name': so_name,
            'reason': f'Sales Order amount is {payment_amount}'
        })
        return None

    # Get required accounts
    receivable_account = client.get_company_default_account('receivable')
    bank_account = client.get_company_default_account('bank')
    mode_of_payment = client.get_default_mode_of_payment()

    if not receivable_account:
        results['pe_skipped'].append({
            'so_name': so_name,
            'reason': 'No receivable account configured'
        })
        return None

    pe_data = {
        'doctype': 'Payment Entry',
        'naming_series': 'ACC-PAY-.YYYY.-',
        'payment_type': 'Receive',
        'company': client.company_name,
        'mode_of_payment': mode_of_payment,
        'party_type': 'Customer',
        'party': customer_id,
        'paid_amount': payment_amount,
        'received_amount': payment_amount,
        'posting_date': posting_date,
        'reference_no': so_name,
        'reference_date': posting_date,
        'paid_to': bank_account or receivable_account,
        'paid_from': receivable_account,
        'references': [{
            'reference_doctype': 'Sales Order',
            'reference_name': so_name,
            'allocated_amount': payment_amount
        }]
    }

    response = client.create_payment_entry(pe_data)

    if response.get('data', {}).get('name'):
        pe_name = response['data']['name']
        results['pe_created'] += 1

        # Submit the Payment Entry
        submit_response = client.submit_document('Payment Entry', pe_name)
        if submit_response.get('error'):
            results['pe_submit_failed'].append({
                'pe_name': pe_name,
                'error': submit_response['error'][:100]
            })
        else:
            results['pe_submitted'] += 1

        return pe_name
    else:
        error = response.get('error', 'Unknown error')
        results['pe_create_failed'].append({
            'so_name': so_name,
            'error': str(error)[:150]
        })
        return None


def create_sales_order_in_erpnext(client, order_no, order_data, sheet_config, default_warehouse, results):
    """Create a single Sales Order in ERPNext.

    Returns:
        dict: {
            'so_name': str,
            'customer_id': str,
            'amount': float,
            'order_date': str,
            'delivery_date': str,
            'payment_terms': str,
            'skipped': bool
        }
    """
    result_data = {
        'so_name': None,
        'customer_id': None,
        'amount': 0.0,
        'order_date': None,
        'delivery_date': None,
        'payment_terms': order_data.get('payment_terms', ''),
        'skipped': False
    }

    # Check if SO already exists
    existing_so = client.find_sales_order_by_po_no(order_no)
    if existing_so:
        results['skipped'] += 1
        result_data['so_name'] = existing_so
        result_data['skipped'] = True
        return result_data

    # Find customer
    customer_id = client.get_customer(order_data['customer_name'])
    if not customer_id:
        results['customer_not_found'].append({
            'order_no': order_no,
            'customer': order_data['customer_name']
        })
        return result_data

    result_data['customer_id'] = customer_id

    # Verify all items exist
    items_data = []
    calculated_amount = 0.0
    for item in order_data['items']:
        item_data = client.get_item(item['item_code'])
        if not item_data:
            results['item_not_found'].append({
                'order_no': order_no,
                'item_code': item['item_code']
            })
            continue

        rate = item_data.get('standard_rate') or item_data.get('valuation_rate') or 0
        items_data.append({
            'item_code': item_data['name'],
            'item_name': item['item_name'] or item_data.get('item_name', item_data['name']),
            'qty': item['qty'],
            'rate': rate,
            'warehouse': default_warehouse
        })
        calculated_amount += item['qty'] * rate

    if not items_data:
        results['no_valid_items'].append(order_no)
        return result_data

    # Determine order amount: use Column S if available, otherwise calculated
    order_amount = order_data.get('order_amount', 0.0)
    if order_amount <= 0:
        order_amount = calculated_amount
    result_data['amount'] = order_amount

    # Resolve container
    container_link = None
    if order_data['container']:
        container_link = client.get_container(order_data['container'])

    # Calculate dates
    transaction_date = order_data['order_date'] or datetime.now().strftime('%Y-%m-%d')
    result_data['order_date'] = transaction_date

    # Delivery date priority:
    # 1. date_delivered (for DESPATCHED orders - actual delivery)
    # 2. dps_date (for FOR DESPATCH orders - target dispatch date from Column AL)
    # 3. eta (container ETA)
    # 4. Leave empty if no date available (no fallback - delivery_date is not required)
    delivery_date = order_data['date_delivered'] or order_data['dps_date'] or order_data['eta']

    # Only validate if delivery_date exists - ensure it's after transaction_date
    if delivery_date and delivery_date <= transaction_date:
        trans_dt = datetime.strptime(transaction_date, '%Y-%m-%d')
        delivery_date = (trans_dt + timedelta(days=1)).strftime('%Y-%m-%d')

    result_data['delivery_date'] = delivery_date

    # Build Sales Order data
    so_data = {
        'doctype': 'Sales Order',
        'naming_series': 'SAL-ORD-.YYYY.-',
        'company': client.company_name,
        'customer': customer_id,
        'po_no': order_no,
        'transaction_date': transaction_date,
        'items': items_data,
        # Custom stock status
        'custom_stock_status': sheet_config['stock_status']
    }

    # Add delivery_date only if available (not required field)
    if delivery_date:
        so_data['delivery_date'] = delivery_date

    # Add container linkage if exists
    if container_link:
        so_data['custom_allocated_container'] = container_link
    if order_data['eta']:
        so_data['custom_container_eta'] = order_data['eta']

    # Add date delivered for despatched orders
    if order_data['date_delivered']:
        so_data['custom_date_delivered'] = order_data['date_delivered']

    # Create the Sales Order
    response = client.create_sales_order(so_data)

    if response.get('data', {}).get('name'):
        so_name = response['data']['name']
        result_data['so_name'] = so_name
        results['created'] += 1

        # Submit if configured
        if sheet_config.get('submit_so', True):
            submit_response = client.submit_document('Sales Order', so_name)
            if submit_response.get('error'):
                results['submit_failed'].append({
                    'order_no': order_no,
                    'so_name': so_name,
                    'error': submit_response['error'][:100]
                })
            else:
                results['submitted'] += 1

        return result_data
    else:
        error = response.get('error', 'Unknown error')
        results['create_failed'].append({
            'order_no': order_no,
            'error': str(error)[:150]
        })
        return result_data


def create_delivery_note_for_order(client, so_name, order_data, default_warehouse, sheet_config, results):
    """Create and optionally submit a Delivery Note for a completed Sales Order"""
    so = client.get_sales_order(so_name)
    if not so:
        return None

    # Check if SO is submitted
    if so.get('docstatus') != 1:
        results['dn_create_failed'].append({
            'so_name': so_name,
            'error': 'Sales Order not submitted'
        })
        return None

    dn_items = []
    for item in so.get('items', []):
        dn_items.append({
            'item_code': item['item_code'],
            'item_name': item['item_name'],
            'qty': item['qty'],
            'warehouse': item.get('warehouse') or default_warehouse,
            'against_sales_order': so_name,
            'so_detail': item['name']
        })

    if not dn_items:
        return None

    # Use Sales Order's custom_date_delivered (set during SO creation from Google Sheets)
    # This ensures DN posting_date matches the actual historical delivery date
    posting_date = so.get('custom_date_delivered') or so.get('transaction_date')
    if not posting_date:
        results['dn_create_failed'].append({
            'so_name': so_name,
            'error': 'No delivery date available'
        })
        return None

    dn_data = {
        'doctype': 'Delivery Note',
        'naming_series': 'MAT-DN-.YYYY.-',
        'company': client.company_name,
        'customer': so['customer'],
        'posting_date': posting_date,
        'items': dn_items
    }

    response = client.create_delivery_note(dn_data)

    if response.get('data', {}).get('name'):
        dn_name = response['data']['name']
        results['dn_created'] += 1

        # Submit if configured (user chose to submit DNs)
        if sheet_config.get('submit_delivery_note', False):
            submit_response = client.submit_document('Delivery Note', dn_name)
            if submit_response.get('error'):
                results['dn_submit_failed'].append({
                    'dn_name': dn_name,
                    'error': submit_response['error'][:100]
                })
            else:
                results['dn_submitted'] += 1

        return dn_name
    else:
        error = response.get('error', 'Unknown error')
        results['dn_create_failed'].append({
            'so_name': so_name,
            'error': str(error)[:150]
        })
        return None


def process_sheet(client, service, spreadsheet_id, sheet_key, sheet_config, default_warehouse, limit=None):
    """Process a single sheet and create Sales Orders"""
    results = {
        'sheet': sheet_config['name'],
        'orders_read': 0,
        'created': 0,
        'submitted': 0,
        'skipped': 0,
        'customer_not_found': [],
        'item_not_found': [],
        'no_valid_items': [],
        'create_failed': [],
        'submit_failed': [],
        'dn_created': 0,
        'dn_submitted': 0,
        'dn_create_failed': [],
        'dn_submit_failed': [],
        # Payment Entry tracking
        'pe_created': 0,
        'pe_submitted': 0,
        'pe_create_failed': [],
        'pe_submit_failed': [],
        'pe_skipped': []
    }

    print(f'\n   Reading from {sheet_config["name"]} sheet...')
    orders = read_orders_from_sheet(service, spreadsheet_id, sheet_config)
    results['orders_read'] = len(orders)
    print(f'   Found {len(orders)} orders')

    if limit:
        order_items = list(orders.items())[:limit]
        orders = dict(order_items)
        print(f'   Limited to {len(orders)} orders for testing')

    total = len(orders)
    for i, (order_no, order_data) in enumerate(orders.items()):
        try:
            so_result = create_sales_order_in_erpnext(
                client, order_no, order_data, sheet_config, default_warehouse, results
            )

            so_name = so_result.get('so_name') if so_result else None

            if so_name and sheet_config.get('create_delivery_note', False):
                create_delivery_note_for_order(
                    client, so_name, order_data, default_warehouse, sheet_config, results
                )

            # Create Payment Entry if conditions are met
            # Only for newly created orders (not skipped), with valid payment terms
            if so_result and so_result.get('so_name') and not so_result.get('skipped'):
                payment_terms = so_result.get('payment_terms', '')
                order_date = so_result.get('order_date')
                delivery_date = so_result.get('delivery_date')
                customer_id = so_result.get('customer_id')

                # Calculate payment posting date
                payment_posting_date = get_payment_posting_date(
                    payment_terms, order_date, delivery_date
                )

                if payment_posting_date and customer_id:
                    create_payment_entry_for_order(
                        client,
                        so_result['so_name'],
                        customer_id,
                        None,  # Amount will be fetched from SO grand_total
                        payment_posting_date,
                        results
                    )
                elif payment_terms:
                    # Log skipped payment entries
                    results['pe_skipped'].append({
                        'so_name': so_result['so_name'],
                        'reason': f'Cannot calculate posting date for {payment_terms}'
                    })

            if (i + 1) % 10 == 0:
                print(f'   Processed {i+1}/{total} orders from {sheet_config["name"]}')

            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(1)

        except requests.exceptions.RequestException as e:
            results['create_failed'].append({
                'order_no': order_no,
                'error': f'Network error: {type(e).__name__}'
            })
        except Exception as e:
            results['create_failed'].append({
                'order_no': order_no,
                'error': str(e)[:100]
            })

    return results


def main():
    """Main migration function"""
    import argparse

    parser = argparse.ArgumentParser(description='Sales Order Migration Script')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of orders per sheet (for testing)')
    parser.add_argument('--sheet', type=str, default=None,
                       choices=['sales', 'for_despatch', 'despatched'],
                       help='Only process a specific sheet')
    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: Sales Order Migration')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to Google Sheets...')
    sheets_service = get_sheets_service(config)

    print('\n2. Connecting to ERPNext...')
    client = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['api_key'],
        config['erpnext']['api_secret']
    )

    print('\n3. Getting default warehouse...')
    default_warehouse = client.get_default_warehouse()
    print(f'   Using warehouse: {default_warehouse}')

    print('\n4. Processing order sheets...')
    all_results = []

    sheets_to_process = SHEETS.items()
    if args.sheet:
        sheets_to_process = [(args.sheet, SHEETS[args.sheet])]

    for sheet_key, sheet_config in sheets_to_process:
        print(f'\n   === Processing {sheet_config["name"]} ===')
        print(f'   Stock Status: {sheet_config["stock_status"]}')
        results = process_sheet(
            client, sheets_service, config['google_sheets']['spreadsheet_id'],
            sheet_key, sheet_config, default_warehouse, limit=args.limit
        )
        all_results.append(results)

    # Summary
    print('\n' + '=' * 60)
    print('SALES ORDER MIGRATION COMPLETE')
    print('=' * 60)

    total_read = sum(r['orders_read'] for r in all_results)
    total_created = sum(r['created'] for r in all_results)
    total_submitted = sum(r['submitted'] for r in all_results)
    total_skipped = sum(r['skipped'] for r in all_results)
    total_dn_created = sum(r['dn_created'] for r in all_results)
    total_dn_submitted = sum(r['dn_submitted'] for r in all_results)
    total_pe_created = sum(r['pe_created'] for r in all_results)
    total_pe_submitted = sum(r['pe_submitted'] for r in all_results)

    print(f'\nOrders Read:      {total_read}')
    print(f'SO Created:       {total_created}')
    print(f'SO Submitted:     {total_submitted}')
    print(f'SO Skipped:       {total_skipped} (already exist)')
    print(f'DN Created:       {total_dn_created}')
    print(f'DN Submitted:     {total_dn_submitted}')
    print(f'PE Created:       {total_pe_created}')
    print(f'PE Submitted:     {total_pe_submitted}')

    # Errors summary
    all_customer_not_found = []
    all_item_not_found = []
    all_create_failed = []
    all_pe_create_failed = []
    all_pe_skipped = []

    for r in all_results:
        all_customer_not_found.extend(r['customer_not_found'])
        all_item_not_found.extend(r['item_not_found'])
        all_create_failed.extend(r['create_failed'])
        all_pe_create_failed.extend(r['pe_create_failed'])
        all_pe_skipped.extend(r['pe_skipped'])

    if all_customer_not_found:
        unique_customers = set(c['customer'] for c in all_customer_not_found)
        print(f'\nCustomers not found: {len(unique_customers)} unique')
        for c in list(unique_customers)[:5]:
            print(f'  - {c}')

    if all_item_not_found:
        unique_items = set(i['item_code'] for i in all_item_not_found)
        print(f'\nItems not found: {len(unique_items)} unique')
        for i in list(unique_items)[:5]:
            print(f'  - {i}')

    if all_create_failed:
        print(f'\nSO Create failures: {len(all_create_failed)}')
        for f in all_create_failed[:5]:
            print(f'  - {f["order_no"]}: {f["error"][:60]}')

    if all_pe_create_failed:
        print(f'\nPE Create failures: {len(all_pe_create_failed)}')
        for f in all_pe_create_failed[:5]:
            print(f'  - {f["so_name"]}: {f["error"][:60]}')

    if all_pe_skipped:
        print(f'\nPE Skipped: {len(all_pe_skipped)}')
        reasons = {}
        for s in all_pe_skipped:
            reason = s.get('reason', 'Unknown')
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in list(reasons.items())[:5]:
            print(f'  - {reason}: {count}')

    # Save detailed report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'sales_order_migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_orders_read': total_read,
            'total_so_created': total_created,
            'total_so_submitted': total_submitted,
            'total_so_skipped': total_skipped,
            'total_dn_created': total_dn_created,
            'total_dn_submitted': total_dn_submitted,
            'total_pe_created': total_pe_created,
            'total_pe_submitted': total_pe_submitted,
            'sheet_results': all_results
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit code
    has_errors = len(all_create_failed) > 0
    sys.exit(1 if has_errors else 0)


if __name__ == '__main__':
    main()
