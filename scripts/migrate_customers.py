#!/usr/bin/env python3
"""
SBS-51: Customer Migration Script
Imports customers from Google Sheets Despatched sheet into ERPNext
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

    def get_customer(self, customer_name):
        """Get a Customer by name"""
        # URL encode the customer name
        encoded_name = requests.utils.quote(customer_name, safe='')
        response = self.session.get(
            f'{self.url}/api/resource/Customer/{encoded_name}'
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None

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


def get_sheets_service():
    """Initialize Google Sheets API service"""
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['service_account_file'],
        scopes=GOOGLE_SHEETS_CONFIG['scopes']
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
    # Remove common prefixes and clean
    cleaned = re.sub(r'[^\d+]', '', str(value))
    return cleaned if cleaned else ''


def read_customers(service):
    """Read and parse unique customers from Despatched sheet"""
    result = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
        range='Despatched!A2:N10000'  # Skip header row
    ).execute()

    rows = result.get('values', [])
    customers = {}  # Use dict to dedupe by email

    for row in rows:
        # Safe column access
        def get_col(idx):
            return row[idx] if idx < len(row) else ''

        email = clean_text(get_col(8)).lower()  # Customer Email
        name = clean_text(get_col(7))  # Customer Name

        # Skip if no email or name
        if not email or not name:
            continue

        # Skip if already processed this email
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
            # Check if customer exists
            if client.customer_exists(cust['customer_name']):
                print(f'[{i+1}/{total}] Skipping (exists): {cust["customer_name"]}')
                results['skipped'] += 1
                continue

            # Create customer
            customer_data = {
                'customer_name': cust['customer_name'],
                'customer_type': 'Company' if any(x in cust['customer_name'].lower() for x in ['ltd', 'limited', 'inc', 'plc', 'llc', 'corp', 'academy', 'school', 'university', 'college', 'council']) else 'Individual',
                'customer_group': 'All Customer Groups',
                'territory': 'All Territories',
            }

            response = client.create_customer(customer_data)

            if response.get('data', {}).get('name'):
                customer_id = response['data']['name']

                # Create address if we have address data
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

        # Rate limiting
        if (i + 1) % batch_size == 0:
            print(f'Processed {i+1}/{total} customers, pausing...')
            time.sleep(1)

    return results


def main():
    """Main migration function"""
    print('=' * 60)
    print('SBS-51: Customer Migration')
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
    print('\n3. Reading customers from Despatched sheet...')
    customers = read_customers(sheets_service)
    print(f'   Found {len(customers)} unique customers')

    # Import customers
    print(f'\n4. Importing {len(customers)} customers to ERPNext...')
    results = import_customers(erpnext, customers)

    # Summary
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

    # Save detailed report
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


if __name__ == '__main__':
    main()
