#!/usr/bin/env python3
"""
SBS-64: Master Data Migration Script
Imports products from Google Sheets Masterfile into ERPNext

This script:
1. Reads product data from Masterfile sheet
2. Maps categories properly to both item_group and custom_category
3. Creates/updates Items with all custom fields

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
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constants
REQUEST_TIMEOUT = 30  # seconds
VALID_ITEM_GROUPS = [
    'Booth', 'Acoustic Panel', 'Acoustic Slat', 'Furniture',
    'Accessory', 'Moss', 'Spare Glass', 'Spare Packaging'
]


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

    def create_item(self, data):
        """Create an Item in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Item',
            json=data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
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

    def update_item(self, item_code, data):
        """Update an existing Item in ERPNext"""
        response = self.session.put(
            f'{self.url}/api/resource/Item/{item_code}',
            json=data,
            headers=self.headers,
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

    # Check if it's a file path or JSON content
    if os.path.isfile(creds_input):
        # It's a file path
        creds = Credentials.from_service_account_file(
            creds_input,
            scopes=config['google_sheets']['scopes']
        )
    else:
        # Try to parse as JSON
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


def clean_price(value):
    """Convert price string to float: '$1,486.00' -> 1486.00

    Handles edge cases like multiple decimal points by keeping only the last one.
    """
    if not value:
        return 0.0
    # Remove all non-digit and non-period characters
    cleaned = re.sub(r'[^\d.]', '', str(value))
    if not cleaned:
        return 0.0

    # Handle multiple decimal points by keeping only the last one
    parts = cleaned.split('.')
    if len(parts) > 2:
        # Multiple decimals: join all but last with empty string, then add last part
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]

    try:
        return float(cleaned)
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


def read_masterfile(service, spreadsheet_id):
    """Read and parse Masterfile sheet

    Column mapping (0-indexed, starting from row 9):
    A (0):  SBS SKU - item_code
    C (2):  Item Name - item_name
    D (3):  Description - description
    F (5):  Finish - custom_finish
    G (6):  Cost (Valuation) - valuation_rate
    H (7):  Selling Price - standard_rate
    I (8):  CBM - custom_unit_cbm
    R (17): Category - item_group (fallback)
    AH (33): Packing Size - custom_packing_size
    AL (37): Weight - weight_per_unit
    AT (45): Supplier SKU - (metadata)
    AU (46): Category - primary category
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Masterfile!A9:AU5000'
    ).execute()

    rows = result.get('values', [])
    items = []
    skipped = []
    category_counts = {}

    for i, row in enumerate(rows):
        if not row or not row[0] or not row[0].strip():
            continue

        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        sku = clean_text(get_col(0))
        name = clean_text(get_col(2))

        if not sku or not name:
            skipped.append(f'Row {i+9}: Missing SKU or name')
            continue

        # Try primary category column (AU), fallback to R column
        raw_category = clean_text(get_col(46)) or clean_text(get_col(17))

        # Normalize category to match ERPNext Item Groups
        category = normalize_category(raw_category)

        # Track category distribution
        category_counts[category] = category_counts.get(category, 0) + 1

        weight = clean_float(get_col(37))

        item = {
            'item_code': sku,
            'item_name': name[:140] if name else sku,
            'description': clean_text(get_col(3)),
            'item_group': category,
            'stock_uom': 'Nos',
            'is_stock_item': 1,
            'include_item_in_manufacturing': 0,
            'valuation_rate': clean_price(get_col(6)),
            'standard_rate': clean_price(get_col(7)),
            'custom_sku': sku,
            'custom_category': category,  # Also set custom_category field
            'custom_unit_cbm': clean_float(get_col(8)),
            'custom_finish': clean_text(get_col(5)),
            'custom_packing_size': clean_text(get_col(33)),
        }

        if weight > 0:
            item['weight_per_unit'] = weight
            item['weight_uom'] = 'Kg'

        supplier_sku = clean_text(get_col(45))
        if supplier_sku:
            item['_supplier_sku'] = supplier_sku

        items.append(item)

    # Print category distribution
    print('   Category distribution:')
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f'      {cat}: {count}')

    return items, skipped


def normalize_category(raw_category):
    """Normalize category string to match ERPNext Item Groups"""
    if not raw_category:
        return 'Furniture'  # Default

    # Map various spellings to standard categories
    category_map = {
        'BOOTH': 'Booth',
        'BOOTHS': 'Booth',
        'ACOUSTIC PANEL': 'Acoustic Panel',
        'ACOUSTIC PANELS': 'Acoustic Panel',
        'PANEL': 'Acoustic Panel',
        'ACOUSTIC SLAT': 'Acoustic Slat',
        'ACOUSTIC SLATS': 'Acoustic Slat',
        'SLAT': 'Acoustic Slat',
        'FURNITURE': 'Furniture',
        'ACCESSORY': 'Accessory',
        'ACCESSORIES': 'Accessory',
        'MOSS': 'Moss',
        'SPARE GLASS': 'Spare Glass',
        'GLASS': 'Spare Glass',
        'SPARE PACKAGING': 'Spare Packaging',
        'PACKAGING': 'Spare Packaging',
    }

    # Try exact match (case-insensitive)
    upper = raw_category.upper().strip()
    if upper in category_map:
        return category_map[upper]

    # Try partial match
    for key, value in category_map.items():
        if key in upper:
            return value

    # Default to Furniture if no match
    return 'Furniture'


def has_changes(existing, new_data, fields):
    """Check if any field has changed between existing record and new data"""
    for field in fields:
        existing_val = existing.get(field)
        new_val = new_data.get(field)
        # Normalize None and empty string
        if existing_val in (None, ''):
            existing_val = None
        if new_val in (None, ''):
            new_val = None
        # Compare (handle float comparison)
        if isinstance(new_val, float) or isinstance(existing_val, float):
            if existing_val is None and new_val is None:
                continue
            if existing_val is None or new_val is None:
                return True
            if abs(float(existing_val or 0) - float(new_val or 0)) > 0.001:
                return True
        elif existing_val != new_val:
            return True
    return False


def import_items(client, items, batch_size=50):
    """Import items into ERPNext in batches using upsert (update if exists, create if not)"""
    results = {
        'created': 0,
        'updated': 0,
        'unchanged': 0,
        'failed': 0,
        'errors': []
    }

    # Fields to compare for changes
    compare_fields = [
        'item_name', 'description', 'item_group', 'valuation_rate', 'standard_rate',
        'custom_category', 'custom_unit_cbm', 'custom_finish', 'custom_packing_size', 'weight_per_unit'
    ]

    total = len(items)

    for i, item in enumerate(items):
        supplier_sku = item.pop('_supplier_sku', None)

        try:
            existing = client.get_item(item['item_code'])

            if existing:
                # Check if anything changed
                if not has_changes(existing, item, compare_fields):
                    results['unchanged'] += 1
                    print(f'[{i+1}/{total}] Unchanged: {item["item_code"]}')
                    continue

                # Update existing item
                response = client.update_item(item['item_code'], item)
                if response.get('data', {}).get('name'):
                    results['updated'] += 1
                    print(f'[{i+1}/{total}] Updated: {item["item_code"]}')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'item_code': item['item_code'],
                        'error': f'Update failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Update failed: {item["item_code"]} - {str(error)[:100]}')
            else:
                # Create new item
                response = client.create_item(item)
                if response.get('data', {}).get('name'):
                    results['created'] += 1
                    print(f'[{i+1}/{total}] Created: {item["item_code"]}')
                else:
                    error = response.get('exception', response.get('message', response.get('error', 'Unknown error')))
                    results['failed'] += 1
                    results['errors'].append({
                        'item_code': item['item_code'],
                        'error': f'Create failed: {error}'
                    })
                    print(f'[{i+1}/{total}] Create failed: {item["item_code"]} - {str(error)[:100]}')

        except requests.exceptions.Timeout:
            results['failed'] += 1
            results['errors'].append({
                'item_code': item['item_code'],
                'error': 'Request timeout'
            })
            print(f'[{i+1}/{total}] Timeout: {item["item_code"]}')

        except requests.exceptions.RequestException as e:
            results['failed'] += 1
            results['errors'].append({
                'item_code': item['item_code'],
                'error': f'Network error: {type(e).__name__}'
            })
            print(f'[{i+1}/{total}] Network error: {item["item_code"]} - {type(e).__name__}')

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'item_code': item['item_code'],
                'error': str(e)
            })
            print(f'[{i+1}/{total}] Error: {item["item_code"]} - {str(e)[:100]}')

        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} items, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-64: Master Data Migration')
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

    print('\n3. Reading Masterfile from Google Sheets...')
    items, skipped = read_masterfile(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(items)} valid products')
    print(f'   Skipped {len(skipped)} rows')

    print(f'\n4. Importing {len(items)} items to ERPNext...')
    results = import_items(erpnext, items)

    print('\n' + '=' * 60)
    print('MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created:   {results["created"]}')
    print(f'Updated:   {results["updated"]}')
    print(f'Unchanged: {results["unchanged"]}')
    print(f'Failed:    {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["item_code"]}: {err["error"][:80]}')

    # Use tempfile with timestamp for unique report path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'migration_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'total_items': len(items),
            'created': results['created'],
            'updated': results['updated'],
            'unchanged': results['unchanged'],
            'failed': results['failed'],
            'skipped_rows': skipped[:50],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    # Exit with error code if any failures
    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
