#!/usr/bin/env python3
"""
SBS-59: Container Arrival Processing Script
Automatically transfers stock from "Goods on Water" to destination warehouse when containers arrive.

Data Flow:
1. Read ON WATER items from Inventory sheet (has CONTAINER and ETA)
2. Look up container in Container Status sheet (has LOCATION and Shipped to)
3. Map to destination warehouse using LOCATION + Shipped to
4. Create Stock Entry (Material Transfer) when ETA <= today
5. Send Telegram notifications

Warehouse Mapping:
| LOCATION | Shipped to                       | Destination Warehouse          |
|----------|----------------------------------|--------------------------------|
| UK       | Marone Solutions Ltd             | STOCK IN WAREHOUSE - UK - MAR  |
| UK       | Edward's Furniture Solutions Ltd | STOCK IN WAREHOUSE - UK - FSL  |
| UK       | Primary OFS                      | STOCK IN WAREHOUSE - UK - PRIM |
| UK       | Edward's                         | STOCK IN WAREHOUSE - UK - ED   |
| SPAIN    | Transportes Grau                 | STOCK IN WAREHOUSE - ES - GRAU |
| SPAIN    | (empty/default)                  | STOCK IN WAREHOUSE - ES        |

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)
  TELEGRAM_BOT_TOKEN   - Telegram bot token for notifications
  TELEGRAM_CHAT_ID     - Telegram chat ID for notifications
"""

import os
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
COMPANY = "Soundbox Store"
SOURCE_WAREHOUSE = "Goods on Water - SBS"

# Warehouse mapping: (LOCATION, Shipped to) -> ERPNext warehouse
WAREHOUSE_MAPPING = {
    ('UK', 'Marone Solutions Ltd'): 'Stock In Warehouse UK MAR - SBS',
    ('UK', 'Edward\'s Furniture Solutions Ltd'): 'Stock In Warehouse UK FSL - SBS',
    ('UK', 'Primary OFS'): 'Stock In Warehouse UK PRIM - SBS',
    ('UK', 'Edward\'s'): 'Stock In Warehouse UK ED - SBS',
    ('SPAIN', 'Transportes Grau'): 'Stock In Warehouse ES GRAU - SBS',
    ('SPAIN', ''): 'Stock In Warehouse ES - SBS',
}

# Default warehouses by location (when no specific "Shipped to" match)
# TODO: SBS-60 - Make this configurable via environment variable
DEFAULT_WAREHOUSES = {
    'UK': 'Stock In Warehouse UK MAR - SBS',
    'SPAIN': 'Stock In Warehouse ES - SBS',
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
        },
        'telegram': {
            'bot_token': os.environ.get('TELEGRAM_BOT_TOKEN'),
            'chat_id': os.environ.get('TELEGRAM_CHAT_ID'),
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
        print("  ERPNEXT_URL          - ERPNext server URL")
        print("  ERPNEXT_PASSWORD     - ERPNext admin password")
        print("  GOOGLE_SHEETS_CREDS  - Path to service account JSON file OR JSON content")
        print("\nOptional:")
        print("  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)")
        print("  SPREADSHEET_ID       - Google Sheets ID (has default)")
        print("  TELEGRAM_BOT_TOKEN   - Telegram bot token for notifications")
        print("  TELEGRAM_CHAT_ID     - Telegram chat ID for notifications")
        sys.exit(1)

    return config


def send_telegram(config, message):
    """Send a message to Telegram"""
    token = config['telegram']['bot_token']
    chat_id = config['telegram']['chat_id']

    if not token or not chat_id:
        print("   (Telegram not configured, skipping notification)")
        return False

    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        response = requests.post(url, data={
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"   Telegram error: {e}")
        return False


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

    def get_bin_qty(self, item_code, warehouse):
        """Get current stock quantity for an item in a warehouse"""
        filters = json.dumps([
            ['item_code', '=', item_code],
            ['warehouse', '=', warehouse]
        ])

        response = self.session.get(
            f'{self.url}/api/resource/Bin',
            params={
                'filters': filters,
                'fields': json.dumps(['actual_qty']),
                'limit_page_length': 1
            },
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            try:
                data = response.json().get('data', [])
                if data:
                    return data[0].get('actual_qty', 0)
            except json.JSONDecodeError:
                pass
        return 0

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

    def warehouse_exists(self, warehouse_name):
        """Check if warehouse exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_warehouse(self, warehouse_name):
        """Create a new warehouse"""
        data = {
            'warehouse_name': warehouse_name.replace(' - SBS', ''),
            'company': COMPANY,
            'is_group': 0,
        }
        response = self.session.post(
            f'{self.url}/api/resource/Warehouse',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code in (200, 201):
            return response.json()
        return {'error': f'HTTP {response.status_code}'}

    def create_stock_transfer(self, items, source_warehouse, target_warehouse, posting_date):
        """Create a Stock Entry (Material Transfer)"""
        stock_items = []
        for item in items:
            stock_items.append({
                'item_code': item['item_code'],
                'qty': item['qty'],
                's_warehouse': source_warehouse,
                't_warehouse': target_warehouse,
                'allow_zero_valuation_rate': 1
            })

        data = {
            'stock_entry_type': 'Material Transfer',
            'posting_date': posting_date,
            'company': COMPANY,
            'items': stock_items
        }

        response = self.session.post(
            f'{self.url}/api/resource/Stock Entry',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 201):
            return {'error': f'HTTP {response.status_code}: {response.text[:200]}'}

        try:
            result = response.json()
            entry_name = result.get('data', {}).get('name')
            if entry_name:
                return self.submit_stock_entry(entry_name)
            return result
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def submit_stock_entry(self, entry_name):
        """Submit a Stock Entry"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock Entry/{entry_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return {'error': f'Failed to fetch entry: HTTP {response.status_code}'}

        doc = response.json().get('data')

        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            result = response.json()
            if result.get('message', {}).get('docstatus') == 1:
                return {'data': {'name': entry_name, 'docstatus': 1}}

        return {'error': f'Submit failed: {response.text[:200]}'}


def get_sheets_service(config):
    """Initialize Google Sheets API service"""
    creds_input = config['google_sheets']['credentials']

    if os.path.exists(os.path.expanduser(creds_input)):
        creds_path = os.path.expanduser(creds_input)
    else:
        try:
            creds_data = json.loads(creds_input)
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            json.dump(creds_data, temp_file)
            temp_file.close()
            creds_path = temp_file.name
        except json.JSONDecodeError:
            raise Exception("GOOGLE_SHEETS_CREDS must be a valid file path or JSON content")

    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=config['google_sheets']['scopes']
    )

    return build('sheets', 'v4', credentials=creds)


def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None

    formats = ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d %b %Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def read_container_status(service, spreadsheet_id):
    """Read Container Status sheet and build lookup dict"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'Container Status'!A2:V500"
    ).execute()

    rows = result.get('values', [])
    containers = {}

    for row in rows:
        def get_col(idx):
            return row[idx].strip() if len(row) > idx and row[idx] else ''

        container_name = get_col(0)  # Col A: CONTAINER
        if not container_name:
            continue

        containers[container_name.upper()] = {
            'container_no': get_col(1),  # Col B
            'shipped_to': get_col(3),     # Col D: Shipped to (warehouse company)
            'location': get_col(21),      # Col V: LOCATION (UK/SPAIN)
        }

    return containers


def read_on_water_inventory(service, spreadsheet_id, today):
    """Read ON WATER items from Inventory sheet that have arrived (ETA <= today)"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Inventory!A2:Q2000'
    ).execute()

    rows = result.get('values', [])
    arrived_containers = defaultdict(list)

    def clean_text(val):
        return str(val).strip() if val else ''

    def clean_float(val):
        if not val:
            return 0
        try:
            cleaned = ''.join(c for c in str(val) if c.isdigit() or c in '.-')
            return float(cleaned) if cleaned else 0
        except ValueError:
            return 0

    for row in rows:
        def get_col(idx):
            return row[idx] if len(row) > idx else ''

        location = clean_text(get_col(13))  # Col N: CURRENT LOCATION
        if location.upper() != 'ON WATER':
            continue

        container = clean_text(get_col(14))  # Col O: CONTAINER
        eta_str = clean_text(get_col(15))    # Col P: ETA
        sku = clean_text(get_col(2))         # Col C: SBS SKU
        qty = clean_float(get_col(7))        # Col H: QTY (original qty, not remaining)

        if not container or not sku:
            continue

        # Parse ETA and check if arrived
        eta_date = parse_date(eta_str)
        if not eta_date:
            continue

        if eta_date.date() <= today.date():
            arrived_containers[container.upper()].append({
                'item_code': sku,
                'qty': qty,
                'eta': eta_str
            })

    return arrived_containers


def resolve_warehouse(container_info):
    """Resolve destination warehouse from container LOCATION and Shipped to"""
    location = container_info.get('location', '').upper().strip()
    shipped_to = container_info.get('shipped_to', '').strip()

    # Try exact match
    key = (location, shipped_to)
    if key in WAREHOUSE_MAPPING:
        return WAREHOUSE_MAPPING[key]

    # Try with empty shipped_to (default for location)
    key = (location, '')
    if key in WAREHOUSE_MAPPING:
        return WAREHOUSE_MAPPING[key]

    # Fallback to default by location
    if location in DEFAULT_WAREHOUSES:
        return DEFAULT_WAREHOUSES[location]

    # Ultimate fallback
    return 'Stores - SBS'


def process_container(client, container_name, items, container_info, posting_date):
    """Process a single container arrival"""
    result = {
        'container': container_name,
        'eta': items[0]['eta'] if items else '',
        'location': container_info.get('location', ''),
        'shipped_to': container_info.get('shipped_to', ''),
        'destination': None,
        'status': 'success',
        'items_transferred': 0,
        'total_qty': 0,
        'warnings': [],
        'error': None
    }

    # Resolve destination warehouse
    destination = resolve_warehouse(container_info)
    result['destination'] = destination

    print(f'\n   Container: {container_name}')
    print(f'   Location: {container_info.get("location", "N/A")}, Shipped to: {container_info.get("shipped_to", "N/A")}')
    print(f'   Destination: {destination}')
    print(f'   Items: {len(items)}')

    # Ensure destination warehouse exists
    if not client.warehouse_exists(destination):
        print(f'   Creating warehouse: {destination}')
        create_result = client.create_warehouse(destination)
        if create_result.get('error'):
            result['warnings'].append(f'Could not create warehouse: {create_result["error"]}')

    # Validate items and check stock availability
    valid_items = []
    for item in items:
        item_data = client.get_item(item['item_code'])
        if not item_data:
            result['warnings'].append(f"Item {item['item_code']} not found in ERPNext")
            continue

        # Check stock in source warehouse
        available_qty = client.get_bin_qty(item['item_code'], SOURCE_WAREHOUSE)
        if available_qty <= 0:
            result['warnings'].append(f"Item {item['item_code']} has no stock in {SOURCE_WAREHOUSE}")
            continue

        # Use minimum of requested and available
        transfer_qty = min(item['qty'], available_qty)
        if transfer_qty < item['qty']:
            result['warnings'].append(
                f"Item {item['item_code']}: requested {item['qty']}, available {available_qty}"
            )

        if transfer_qty > 0:
            valid_items.append({
                'item_code': item['item_code'],
                'qty': transfer_qty
            })

    if not valid_items:
        result['status'] = 'skipped'
        result['warnings'].append('No valid items to transfer')
        print(f'   ⚠️ SKIPPED: No valid items to transfer')
        return result

    print(f'   Transferring {len(valid_items)} items...')

    # Create Stock Transfer
    transfer_result = client.create_stock_transfer(
        valid_items,
        SOURCE_WAREHOUSE,
        destination,
        posting_date
    )

    if transfer_result.get('error'):
        result['status'] = 'error'
        result['error'] = transfer_result['error']
        print(f'   ❌ ERROR: {transfer_result["error"]}')
        return result

    entry_name = transfer_result.get('data', {}).get('name')
    print(f'   ✅ Stock Entry created: {entry_name}')

    result['items_transferred'] = len(valid_items)
    result['total_qty'] = sum(item['qty'] for item in valid_items)
    result['stock_entry'] = entry_name

    return result


def main():
    """Main entry point"""
    print('=' * 60)
    print('SBS-59: Container Arrival Processing')
    print('=' * 60)

    config = get_config()
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')

    print(f'\n1. Connecting to Google Sheets...')
    service = get_sheets_service(config)

    print(f'\n2. Reading Container Status sheet...')
    container_status = read_container_status(service, config['google_sheets']['spreadsheet_id'])
    print(f'   Found {len(container_status)} containers')

    print(f'\n3. Reading ON WATER inventory (ETA <= {today_str})...')
    arrived_containers = read_on_water_inventory(service, config['google_sheets']['spreadsheet_id'], today)
    print(f'   Found {len(arrived_containers)} containers with arrived items')

    if not arrived_containers:
        print('\n   No containers have arrived. Nothing to do.')
        send_telegram(config, f"🚢 <b>Container Arrival Check</b>\n\nNo containers have arrived (ETA <= {today_str})")
        return

    print(f'\n4. Connecting to ERPNext...')
    client = ERPNextClient(
        config['erpnext']['url'],
        config['erpnext']['username'],
        config['erpnext']['password']
    )

    print(f'\n5. Processing container arrivals...')

    results = {
        'processed': 0,
        'skipped': 0,
        'errors': 0,
        'total_items': 0,
        'total_qty': 0,
        'warnings': [],
        'details': []
    }

    for i, (container_name, items) in enumerate(sorted(arrived_containers.items()), 1):
        print(f'\n[{i}/{len(arrived_containers)}] Processing {container_name}...')

        # Get container info from Container Status sheet
        container_info = container_status.get(container_name, {})
        if not container_info:
            print(f'   ⚠️ Container not found in Container Status sheet')
            container_info = {'location': '', 'shipped_to': ''}

        result = process_container(client, container_name, items, container_info, today_str)
        results['details'].append(result)

        if result['status'] == 'success':
            results['processed'] += 1
            results['total_items'] += result['items_transferred']
            results['total_qty'] += result['total_qty']
        elif result['status'] == 'skipped':
            results['skipped'] += 1
        else:
            results['errors'] += 1

        results['warnings'].extend(result.get('warnings', []))

    # Print summary
    print('\n' + '=' * 60)
    print('CONTAINER ARRIVALS COMPLETE')
    print('=' * 60)
    print(f'Containers Processed: {results["processed"]}')
    print(f'Containers Skipped:   {results["skipped"]}')
    print(f'Containers Errors:    {results["errors"]}')
    print(f'Total Items:          {results["total_items"]}')
    print(f'Total Qty Transferred:{results["total_qty"]}')

    if results['warnings']:
        print(f'\nWarnings ({len(results["warnings"])}):')
        for w in results['warnings'][:10]:
            print(f'  ⚠️ {w}')
        if len(results['warnings']) > 10:
            print(f'  ... and {len(results["warnings"]) - 10} more')

    # Send Telegram notification
    warning_text = f"\n⚠️ Warnings: {len(results['warnings'])}" if results['warnings'] else ""
    telegram_msg = f"""🚢 <b>Container Arrival Processing</b>

✅ Processed: {results['processed']} containers
📦 Transferred: {results['total_qty']} units ({results['total_items']} items)
⏭️ Skipped: {results['skipped']}
❌ Errors: {results['errors']}{warning_text}

Date: {today_str}"""

    send_telegram(config, telegram_msg)

    # Save detailed report
    report_file = tempfile.NamedTemporaryFile(
        mode='w',
        prefix='container_arrivals_',
        suffix='.json',
        delete=False
    )
    json.dump({
        'date': today_str,
        'summary': {
            'processed': results['processed'],
            'skipped': results['skipped'],
            'errors': results['errors'],
            'total_items': results['total_items'],
            'total_qty': results['total_qty']
        },
        'warnings': results['warnings'],
        'details': results['details']
    }, report_file, indent=2)
    report_file.close()
    print(f'\nDetailed report saved to: {report_file.name}')


if __name__ == '__main__':
    main()
