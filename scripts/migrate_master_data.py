#!/usr/bin/env python3
"""
SBS-51: Master Data Migration Script
Imports products from Google Sheets Masterfile into ERPNext
"""

import os
import re
import json
import time
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Configuration
GOOGLE_SHEETS_CONFIG = {
    'scopes': ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    'service_account_file': os.path.expanduser('~/.config/gcloud/service-accounts/sheets-api-service.json'),
    'spreadsheet_id': '1NQA7DBzIryCjA0o0dxehLyGmxM8ZeOofpg3IENgtDmA',
}

ERPNEXT_CONFIG = {
    'url': os.environ.get('ERPNEXT_URL', 'http://100.65.0.28:8080'),
    'username': 'Administrator',
    'password': os.environ.get('ERPNEXT_PASSWORD', 'soundbox-admin-2026'),
}

# Column mapping: Google Sheet column index -> ERPNext field
COLUMN_MAPPING = {
    0: 'item_code',           # SKU (A)
    2: 'item_name',           # NAME (C)
    3: 'description',         # MANUFACTURER DESCRIPTION (D)
    4: 'brand',               # MODEL (E)
    5: 'custom_finish',       # FINISH (F)
    6: 'valuation_rate',      # PURCHASE PRICE (G)
    7: 'standard_rate',       # RESELLER PRICE (H)
    8: 'custom_cbm',          # UNIT CBM (I)
    33: 'custom_packing_size', # PACKING SIZE (AH)
    37: 'weight_per_unit',    # WEIGHT (AL)
    45: 'supplier_part_no',   # Supplier SKU (AT) - not directly on Item
    46: 'item_group',         # Category (AU)
}

# Valid Item Groups
VALID_ITEM_GROUPS = [
    'Booth', 'Acoustic Panel', 'Acoustic Slat', 'Furniture',
    'Accessory', 'Moss', 'Spare Glass', 'Spare Packaging'
]


class ERPNextClient:
    """ERPNext API Client"""

    def __init__(self, url, username, password):
        self.url = url.rstrip('/')
        self.session = requests.Session()
        self.login(username, password)

    def login(self, username, password):
        """Login and get session cookie"""
        response = self.session.post(
            f'{self.url}/api/method/login',
            data={'usr': username, 'pwd': password}
        )
        if response.status_code != 200 or 'Logged In' not in response.text:
            raise Exception(f'Login failed: {response.text}')
        print(f'Logged in to ERPNext at {self.url}')

    def create_item(self, data):
        """Create an Item in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Item',
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return response.json()

    def get_item(self, item_code):
        """Get an Item by code"""
        response = self.session.get(
            f'{self.url}/api/resource/Item/{item_code}'
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None


def get_sheets_service():
    """Initialize Google Sheets API service"""
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['service_account_file'],
        scopes=GOOGLE_SHEETS_CONFIG['scopes']
    )
    return build('sheets', 'v4', credentials=creds)


def clean_price(value):
    """Convert price string to float: '$1,486.00' -> 1486.00"""
    if not value:
        return 0.0
    # Remove currency symbols and commas
    cleaned = re.sub(r'[^\d.]', '', str(value))
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def clean_float(value):
    """Convert string to float"""
    if not value:
        return 0.0
    try:
        return float(str(value).replace(',', ''))
    except ValueError:
        return 0.0


def clean_text(value):
    """Clean text field"""
    if not value:
        return ''
    return str(value).strip()


def read_masterfile(service):
    """Read and parse Masterfile sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
        range='Masterfile!A9:AU5000'  # Data starts at row 9
    ).execute()

    rows = result.get('values', [])
    items = []
    skipped = []

    for i, row in enumerate(rows):
        # Skip empty rows
        if not row or not row[0] or not row[0].strip():
            continue

        # Get values with safe indexing
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(0))
        name = clean_text(get_col(2))

        # Skip rows without SKU or name
        if not sku or not name:
            skipped.append(f'Row {i+9}: Missing SKU or name')
            continue

        # Get category, default to 'Booth' if not found
        category = clean_text(get_col(46))
        if category not in VALID_ITEM_GROUPS:
            category = 'Booth'  # Default

        # Get weight, only include weight_uom if weight exists
        weight = clean_float(get_col(37))

        item = {
            'item_code': sku,
            'item_name': name[:140] if name else sku,  # ERPNext limit
            'description': clean_text(get_col(3)),
            'item_group': category,
            'stock_uom': 'Nos',  # Default UOM
            'is_stock_item': 1,
            'include_item_in_manufacturing': 0,
            'valuation_rate': clean_price(get_col(6)),
            'standard_rate': clean_price(get_col(7)),
            'custom_cbm': clean_float(get_col(8)),
            'custom_finish': clean_text(get_col(5)),
            'custom_packing_size': clean_text(get_col(33)),
        }

        # Only add weight if it exists
        if weight > 0:
            item['weight_per_unit'] = weight
            item['weight_uom'] = 'Kg'

        # Skip brand to avoid Link validation errors (brands need to be pre-created)
        # brand = clean_text(get_col(4))
        # if brand:
        #     item['brand'] = brand

        # Store supplier SKU for later (will be used as barcode)
        supplier_sku = clean_text(get_col(45))
        if supplier_sku:
            item['_supplier_sku'] = supplier_sku

        items.append(item)

    return items, skipped


def import_items(client, items, batch_size=50):
    """Import items into ERPNext in batches"""
    results = {
        'created': 0,
        'failed': 0,
        'errors': []
    }

    total = len(items)

    for i, item in enumerate(items):
        # Extract supplier SKU before sending
        supplier_sku = item.pop('_supplier_sku', None)

        try:
            # Check if item exists
            existing = client.get_item(item['item_code'])
            if existing:
                print(f'[{i+1}/{total}] Skipping (exists): {item["item_code"]}')
                continue

            # Create item
            response = client.create_item(item)

            if response.get('data', {}).get('name'):
                results['created'] += 1
                print(f'[{i+1}/{total}] Created: {item["item_code"]}')

                # TODO: Add barcode with supplier_sku if needed

            else:
                error = response.get('exception', response.get('message', str(response)))
                results['failed'] += 1
                results['errors'].append({
                    'item_code': item['item_code'],
                    'error': error
                })
                print(f'[{i+1}/{total}] Failed: {item["item_code"]} - {error[:100]}')

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'item_code': item['item_code'],
                'error': str(e)
            })
            print(f'[{i+1}/{total}] Error: {item["item_code"]} - {str(e)[:100]}')

        # Rate limiting
        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} items, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-51: Master Data Migration')
    print('=' * 60)

    # Initialize services
    print('\n1. Connecting to Google Sheets...')
    sheets_service = get_sheets_service()

    print('\n2. Connecting to ERPNext...')
    erpnext = ERPNextClient(
        ERPNEXT_CONFIG['url'],
        ERPNEXT_CONFIG['username'],
        ERPNEXT_CONFIG['password']
    )

    # Read data
    print('\n3. Reading Masterfile from Google Sheets...')
    items, skipped = read_masterfile(sheets_service)
    print(f'   Found {len(items)} valid products')
    print(f'   Skipped {len(skipped)} rows')

    # Import items
    print(f'\n4. Importing {len(items)} items to ERPNext...')
    results = import_items(erpnext, items)

    # Summary
    print('\n' + '=' * 60)
    print('MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Failed:  {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["item_code"]}: {err["error"][:80]}')

    # Save detailed report
    report_path = '/tmp/migration_report.json'
    with open(report_path, 'w') as f:
        json.dump({
            'total_items': len(items),
            'created': results['created'],
            'failed': results['failed'],
            'skipped_rows': skipped[:50],  # First 50
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')


if __name__ == '__main__':
    main()
