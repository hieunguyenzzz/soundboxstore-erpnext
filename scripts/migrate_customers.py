#!/usr/bin/env python3
"""
SBS-51: Customer Migration Script
Imports customers from Google Sheets Despatched sheet into ERPNext

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
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


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

    def create_customer(self, data):
        """Create a Customer in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Customer',
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return response.json()

    def create_address(self, data):
        """Create an Address in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Address',
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return response.json()

    def customer_exists(self, customer_name):
        """Check if customer exists by name"""
        response = self.session.get(
            f'{self.url}/api/resource/Customer',
            params={'filters': json.dumps([['customer_name', '=', customer_name]])}
        )
        if response.status_code == 200:
            data = response.json().get('data', [])
            return len(data) > 0
        return False


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


def clean_phone(value):
    """Clean phone number"""
    if not value:
        return ''
    cleaned = re.sub(r'[^\d+]', '', str(value))
    return cleaned if cleaned else ''


def read_customers(service, spreadsheet_id):
    """Read and parse unique customers from Despatched sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Despatched!A2:N10000'
    ).execute()

    rows = result.get('values', [])
    customers = {}

    for row in rows:
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        email = clean_text(get_col(8)).lower()
        name = clean_text(get_col(7))

        if not email or not name:
            continue

        if email in customers:
            continue

        customer = {
            'customer_name': name,
            'email': email,
            'phone': clean_phone(get_col(9)),
            'address': clean_text(get_col(10)),
            'city': clean_text(get_col(11)),
            'pincode': clean_text(get_col(12)),
            'country': clean_text(get_col(13)) or 'United Kingdom',
        }

        customers[email] = customer

    return list(customers.values())


def import_customers(client, customers, batch_size=50):
    """Import customers into ERPNext"""
    results = {
        'created': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }

    total = len(customers)

    for i, cust in enumerate(customers):
        try:
            if client.customer_exists(cust['customer_name']):
                print(f'[{i+1}/{total}] Skipping (exists): {cust["customer_name"]}')
                results['skipped'] += 1
                continue

            customer_data = {
                'customer_name': cust['customer_name'],
                'customer_type': 'Company' if any(x in cust['customer_name'].lower() for x in ['ltd', 'limited', 'inc', 'plc', 'llc', 'corp', 'academy', 'school', 'university', 'college', 'council']) else 'Individual',
                'customer_group': 'All Customer Groups',
                'territory': 'All Territories',
            }

            response = client.create_customer(customer_data)

            if response.get('data', {}).get('name'):
                customer_id = response['data']['name']

                if cust['address'] or cust['city']:
                    address_data = {
                        'address_title': cust['customer_name'],
                        'address_type': 'Billing',
                        'address_line1': cust['address'] or cust['city'],
                        'city': cust['city'] or 'Not specified',
                        'pincode': cust['pincode'],
                        'country': cust['country'],
                        'phone': cust['phone'],
                        'email_id': cust['email'],
                        'links': [{'link_doctype': 'Customer', 'link_name': customer_id}]
                    }
                    client.create_address(address_data)

                results['created'] += 1
                print(f'[{i+1}/{total}] Created: {cust["customer_name"]}')
            else:
                error = response.get('exception', response.get('message', str(response)))
                results['failed'] += 1
                results['errors'].append({
                    'customer': cust['customer_name'],
                    'error': error
                })
                print(f'[{i+1}/{total}] Failed: {cust["customer_name"]} - {error[:80]}')

        except Exception as e:
            results['failed'] += 1
            results['errors'].append({
                'customer': cust['customer_name'],
                'error': str(e)
            })
            print(f'[{i+1}/{total}] Error: {cust["customer_name"]} - {str(e)[:80]}')

        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} customers, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-51: Customer Migration')
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

    print('\n3. Reading customers from Despatched sheet...')
    customers = read_customers(
        sheets_service,
        config['google_sheets']['spreadsheet_id']
    )
    print(f'   Found {len(customers)} unique customers')

    print(f'\n4. Importing {len(customers)} customers to ERPNext...')
    results = import_customers(erpnext, customers)

    print('\n' + '=' * 60)
    print('CUSTOMER MIGRATION COMPLETE')
    print('=' * 60)
    print(f'Created: {results["created"]}')
    print(f'Skipped: {results["skipped"]}')
    print(f'Failed:  {results["failed"]}')

    if results['errors']:
        print(f'\nFirst 10 errors:')
        for err in results['errors'][:10]:
            print(f'  - {err["customer"]}: {err["error"][:60]}')

    report_path = '/tmp/customer_migration_report.json'
    with open(report_path, 'w') as f:
        json.dump({
            'total_customers': len(customers),
            'created': results['created'],
            'skipped': results['skipped'],
            'failed': results['failed'],
            'errors': results['errors']
        }, f, indent=2)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
