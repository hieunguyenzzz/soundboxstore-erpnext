#!/usr/bin/env python3
"""
SBS-61: Stock Reconciliation Report
Compares Google Sheets Inventory with ERPNext stock levels

Generates a detailed discrepancy report showing:
- Items in GSheets but not in ERPNext
- Items in ERPNext but not in GSheets
- Quantity mismatches per item/warehouse
- Summary statistics

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

# Warehouse mapping for comparison
WAREHOUSE_MAPPING = {
    'FOR MANUFACTURE': 'For Manufacture - SBS',
    'ON WATER': 'Goods on Water - SBS',
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

    def get_all_stock_levels(self):
        """Get all stock levels from Bin doctype"""
        all_bins = []
        offset = 0
        limit = 500

        while True:
            response = self.session.get(
                f'{self.url}/api/resource/Bin',
                params={
                    'fields': json.dumps(['item_code', 'warehouse', 'actual_qty', 'reserved_qty']),
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                break

            try:
                data = response.json().get('data', [])
                if not data:
                    break
                all_bins.extend(data)
                offset += limit
                print(f'   Fetched {len(all_bins)} bin records...')
            except json.JSONDecodeError:
                break

        return all_bins

    def get_all_items(self):
        """Get all items with their codes"""
        all_items = []
        offset = 0
        limit = 500

        while True:
            response = self.session.get(
                f'{self.url}/api/resource/Item',
                params={
                    'fields': json.dumps(['name', 'item_name', 'custom_sku']),
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                break

            try:
                data = response.json().get('data', [])
                if not data:
                    break
                all_items.extend(data)
                offset += limit
            except json.JSONDecodeError:
                break

        return all_items


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


def resolve_warehouse(location):
    """Map Google Sheets location to ERPNext warehouse name"""
    if not location:
        return DEFAULT_WAREHOUSE

    location = location.strip().upper()

    if location in WAREHOUSE_MAPPING:
        return WAREHOUSE_MAPPING[location]

    for sheet_loc, erp_wh in WAREHOUSE_MAPPING.items():
        if sheet_loc in location or location in sheet_loc:
            return erp_wh

    return DEFAULT_WAREHOUSE


def read_gsheets_inventory(service, spreadsheet_id):
    """Read inventory data from Google Sheets

    Returns dict: {(item_code, warehouse): qty}
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Inventory!A2:O5000'
    ).execute()

    rows = result.get('values', [])
    inventory = defaultdict(float)
    item_totals = defaultdict(float)

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(2))  # Col C: SBS SKU
        remaining_qty = clean_float(get_col(11))  # Col L: REMAINING QTY
        location = clean_text(get_col(13))  # Col N: CURRENT LOCATION

        if not sku:
            continue

        warehouse = resolve_warehouse(location)
        key = (sku, warehouse)
        inventory[key] += remaining_qty
        item_totals[sku] += remaining_qty

    return dict(inventory), dict(item_totals)


def process_erpnext_stock(bins):
    """Process ERPNext bin data into comparable format

    Returns dict: {(item_code, warehouse): qty}
    """
    inventory = defaultdict(float)
    item_totals = defaultdict(float)

    for bin_record in bins:
        item_code = bin_record.get('item_code', '')
        warehouse = bin_record.get('warehouse', '')
        actual_qty = bin_record.get('actual_qty', 0) or 0

        if not item_code or not warehouse:
            continue

        key = (item_code, warehouse)
        inventory[key] = actual_qty
        item_totals[item_code] += actual_qty

    return dict(inventory), dict(item_totals)


def calculate_discrepancies(gsheets_data, gsheets_totals, erpnext_data, erpnext_totals):
    """Compare inventories and identify discrepancies"""

    all_keys = set(gsheets_data.keys()) | set(erpnext_data.keys())
    all_items = set(gsheets_totals.keys()) | set(erpnext_totals.keys())

    discrepancies = {
        'by_location': [],  # Per item+warehouse discrepancies
        'by_item': [],  # Per item total discrepancies
        'missing_in_erpnext': [],  # Items in GSheets but not ERPNext
        'extra_in_erpnext': [],  # Items in ERPNext but not GSheets
    }

    # Compare by item + warehouse
    for key in sorted(all_keys):
        item_code, warehouse = key
        gsheets_qty = gsheets_data.get(key, 0)
        erpnext_qty = erpnext_data.get(key, 0)

        if abs(gsheets_qty - erpnext_qty) > 0.01:  # Allow tiny float differences
            discrepancies['by_location'].append({
                'item_code': item_code,
                'warehouse': warehouse,
                'gsheets_qty': gsheets_qty,
                'erpnext_qty': erpnext_qty,
                'difference': gsheets_qty - erpnext_qty
            })

    # Compare by item total
    for item_code in sorted(all_items):
        gsheets_total = gsheets_totals.get(item_code, 0)
        erpnext_total = erpnext_totals.get(item_code, 0)

        if abs(gsheets_total - erpnext_total) > 0.01:
            discrepancies['by_item'].append({
                'item_code': item_code,
                'gsheets_total': gsheets_total,
                'erpnext_total': erpnext_total,
                'difference': gsheets_total - erpnext_total
            })

    # Items only in GSheets
    gsheets_only = set(gsheets_totals.keys()) - set(erpnext_totals.keys())
    for item_code in sorted(gsheets_only):
        if gsheets_totals[item_code] > 0:  # Only non-zero stock
            discrepancies['missing_in_erpnext'].append({
                'item_code': item_code,
                'gsheets_qty': gsheets_totals[item_code]
            })

    # Items only in ERPNext
    erpnext_only = set(erpnext_totals.keys()) - set(gsheets_totals.keys())
    for item_code in sorted(erpnext_only):
        if erpnext_totals[item_code] > 0:  # Only non-zero stock
            discrepancies['extra_in_erpnext'].append({
                'item_code': item_code,
                'erpnext_qty': erpnext_totals[item_code]
            })

    return discrepancies


def generate_summary(gsheets_totals, erpnext_totals, discrepancies):
    """Generate summary statistics"""

    gsheets_items = set(k for k, v in gsheets_totals.items() if v > 0)
    erpnext_items = set(k for k, v in erpnext_totals.items() if v > 0)

    matched_items = gsheets_items & erpnext_items
    qty_matched = sum(1 for d in discrepancies['by_item'] if abs(d['difference']) < 0.01)

    return {
        'gsheets_unique_items': len(gsheets_items),
        'gsheets_total_qty': sum(gsheets_totals.values()),
        'erpnext_unique_items': len(erpnext_items),
        'erpnext_total_qty': sum(erpnext_totals.values()),
        'items_in_both': len(matched_items),
        'items_qty_matched': qty_matched,
        'items_qty_mismatch': len(discrepancies['by_item']),
        'items_missing_erpnext': len(discrepancies['missing_in_erpnext']),
        'items_extra_erpnext': len(discrepancies['extra_in_erpnext']),
        'location_discrepancies': len(discrepancies['by_location']),
    }


def print_report(summary, discrepancies):
    """Print report to console"""

    print('\n' + '=' * 60)
    print('STOCK RECONCILIATION SUMMARY')
    print('=' * 60)

    print('\n--- INVENTORY TOTALS ---')
    print(f'Google Sheets: {summary["gsheets_unique_items"]} unique items, '
          f'{summary["gsheets_total_qty"]:.0f} total units')
    print(f'ERPNext:       {summary["erpnext_unique_items"]} unique items, '
          f'{summary["erpnext_total_qty"]:.0f} total units')

    print('\n--- COMPARISON ---')
    print(f'Items in both systems:     {summary["items_in_both"]}')
    print(f'Items with qty match:      {summary["items_qty_matched"]}')
    print(f'Items with qty mismatch:   {summary["items_qty_mismatch"]}')
    print(f'Items missing in ERPNext:  {summary["items_missing_erpnext"]}')
    print(f'Items extra in ERPNext:    {summary["items_extra_erpnext"]}')
    print(f'Location-level mismatches: {summary["location_discrepancies"]}')

    # Show top discrepancies
    if discrepancies['by_item']:
        print('\n--- TOP 10 QUANTITY DISCREPANCIES (by item) ---')
        sorted_disc = sorted(discrepancies['by_item'],
                            key=lambda x: abs(x['difference']), reverse=True)
        for d in sorted_disc[:10]:
            print(f'  {d["item_code"]}: GSheets={d["gsheets_total"]:.0f}, '
                  f'ERPNext={d["erpnext_total"]:.0f}, Diff={d["difference"]:+.0f}')

    if discrepancies['missing_in_erpnext']:
        print('\n--- ITEMS MISSING IN ERPNEXT (first 10) ---')
        for d in discrepancies['missing_in_erpnext'][:10]:
            print(f'  {d["item_code"]}: {d["gsheets_qty"]:.0f} units in GSheets')

    if discrepancies['extra_in_erpnext']:
        print('\n--- EXTRA ITEMS IN ERPNEXT (first 10) ---')
        for d in discrepancies['extra_in_erpnext'][:10]:
            print(f'  {d["item_code"]}: {d["erpnext_qty"]:.0f} units in ERPNext')


def main():
    """Main function"""
    print('=' * 60)
    print('SBS-61: Stock Reconciliation Report')
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

    print('\n3. Reading Google Sheets Inventory...')
    gsheets_data, gsheets_totals = read_gsheets_inventory(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(gsheets_totals)} unique items in {len(gsheets_data)} item-warehouse combinations')

    print('\n4. Reading ERPNext stock levels...')
    bins = erpnext.get_all_stock_levels()
    erpnext_data, erpnext_totals = process_erpnext_stock(bins)
    print(f'   Found {len(erpnext_totals)} unique items in {len(erpnext_data)} item-warehouse combinations')

    print('\n5. Calculating discrepancies...')
    discrepancies = calculate_discrepancies(
        gsheets_data, gsheets_totals,
        erpnext_data, erpnext_totals
    )

    summary = generate_summary(gsheets_totals, erpnext_totals, discrepancies)

    # Print report
    print_report(summary, discrepancies)

    # Save detailed report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'stock_reconciliation_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'summary': summary,
            'discrepancies': {
                'by_item': discrepancies['by_item'][:100],  # Limit for readability
                'by_location': discrepancies['by_location'][:100],
                'missing_in_erpnext': discrepancies['missing_in_erpnext'],
                'extra_in_erpnext': discrepancies['extra_in_erpnext']
            },
            'totals': {
                'gsheets_by_item_count': len(gsheets_totals),
                'erpnext_by_item_count': len(erpnext_totals)
            }
        }, f, indent=2)
    print(f'\n\nDetailed report saved to: {report_path}')

    # Determine exit code based on discrepancy count
    # Non-blocking: always exit 0 unless there's a critical error
    # This is informational only
    print('\n' + '=' * 60)
    if summary['items_qty_mismatch'] > 0:
        print(f'NOTE: {summary["items_qty_mismatch"]} items have quantity discrepancies')
        print('Review the report and reconcile as needed.')
    else:
        print('All item quantities match between Google Sheets and ERPNext.')
    print('=' * 60)

    sys.exit(0)


if __name__ == '__main__':
    main()
