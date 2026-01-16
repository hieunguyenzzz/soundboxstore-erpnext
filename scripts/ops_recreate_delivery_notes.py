#!/usr/bin/env python3
"""
SBS-64: Recreate Delivery Notes with Historical Dates

This script creates and submits Delivery Notes for all DESPATCHED Sales Orders
using the correct historical posting_date from custom_date_delivered.

Usage:
  python scripts/ops_recreate_delivery_notes.py --dry-run  # Preview
  python scripts/ops_recreate_delivery_notes.py            # Execute
"""

import os
import sys
import json
import time
import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
REQUEST_TIMEOUT = 30
BATCH_SIZE = 50


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

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }
        self._verify_connection()
        self.company_name, self.company_abbr = self._get_company_info()
        print(f'Connected to {self.url} as company: {self.company_name}')

    def _verify_connection(self):
        """Verify API connection"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed: {response.status_code}')

    def _get_company_info(self):
        """Get company name and abbreviation"""
        response = self.session.get(
            f'{self.url}/api/resource/Company?limit_page_length=1',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        companies = response.json().get('data', [])
        if not companies:
            raise Exception('No company found')

        company_name = companies[0]['name']
        response = self.session.get(
            f'{self.url}/api/resource/Company/{company_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        company_data = response.json().get('data', {})
        return company_data.get('name'), company_data.get('abbr', 'SBS')

    def get_despatched_orders(self):
        """Get all DESPATCHED Sales Orders that don't have DNs"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order',
            params={
                'filters': json.dumps([
                    ['docstatus', '=', 1],
                    ['custom_stock_status', '=', 'DESPATCHED'],
                    ['per_delivered', '=', 0]  # No DN linked yet
                ]),
                'fields': json.dumps(['name', 'customer', 'custom_date_delivered', 'transaction_date']),
                'limit_page_length': 0
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.json().get('data', [])

    def get_sales_order(self, name):
        """Get full Sales Order document"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data')
        return None

    def get_default_warehouse(self):
        """Get default warehouse"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse',
            params={
                'filters': json.dumps([['is_group', '=', 0]]),
                'fields': json.dumps(['name']),
                'limit_page_length': 1
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        warehouses = response.json().get('data', [])
        if warehouses:
            return warehouses[0]['name']
        return f'Stores - {self.company_abbr}'

    def create_delivery_note(self, so_name, posting_date, default_warehouse):
        """Create Delivery Note from Sales Order"""
        so = self.get_sales_order(so_name)
        if not so:
            return {'error': f'Could not fetch Sales Order {so_name}'}

        # Build DN items from SO items
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
            return {'error': 'No items to deliver'}

        dn_data = {
            'doctype': 'Delivery Note',
            'naming_series': 'MAT-DN-.YYYY.-',
            'company': self.company_name,
            'customer': so['customer'],
            'posting_date': posting_date,
            'set_posting_time': 1,  # Allow historical dates
            'items': dn_items
        }

        response = self.session.post(
            f'{self.url}/api/resource/Delivery Note',
            json=dn_data,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', err.get('message', f'HTTP {response.status_code}'))}
            except:
                return {'error': f'HTTP {response.status_code}'}

        return response.json()

    def submit_document(self, doctype, name):
        """Submit a document"""
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return {'error': f'Could not fetch {doctype} {name}'}

        doc = response.json().get('data', {})

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
            except:
                return {'error': f'HTTP {response.status_code}'}

        return response.json()


def main():
    parser = argparse.ArgumentParser(description='Recreate Delivery Notes with historical dates')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of DNs to create')
    args = parser.parse_args()

    # Get config from environment
    url = os.environ.get('ERPNEXT_URL')
    api_key = os.environ.get('ERPNEXT_API_KEY')
    api_secret = os.environ.get('ERPNEXT_API_SECRET')

    if not all([url, api_key, api_secret]):
        print('ERROR: Missing ERPNEXT_URL, ERPNEXT_API_KEY, or ERPNEXT_API_SECRET')
        sys.exit(1)

    print('=' * 60)
    print('SBS-64: Recreate Delivery Notes with Historical Dates')
    print('=' * 60)

    if args.dry_run:
        print('\n*** DRY RUN MODE - No changes will be made ***\n')

    client = ERPNextClient(url, api_key, api_secret)
    default_warehouse = client.get_default_warehouse()
    print(f'Default warehouse: {default_warehouse}')

    # Get orders needing DNs
    print('\nFinding DESPATCHED orders without Delivery Notes...')
    orders = client.get_despatched_orders()
    print(f'Found {len(orders)} orders needing Delivery Notes')

    if args.limit:
        orders = orders[:args.limit]
        print(f'Limited to {len(orders)} orders')

    if args.dry_run:
        print('\nSample orders that would get DNs:')
        for order in orders[:10]:
            date = order.get('custom_date_delivered') or order.get('transaction_date') or 'NO DATE'
            print(f"  - {order['name']}: {order['customer'][:40]} - posting_date: {date}")
        print(f'\n{len(orders)} Delivery Notes would be created and submitted.')

        # Show date distribution
        from collections import Counter
        years = Counter()
        for order in orders:
            date = order.get('custom_date_delivered') or order.get('transaction_date')
            if date:
                year = date[:4]
                years[year] += 1
            else:
                years['NO DATE'] += 1
        print('\nDate distribution:')
        for year, count in sorted(years.items()):
            print(f'  {year}: {count} orders')
        return

    # Create and submit DNs
    results = {
        'created': 0,
        'submitted': 0,
        'create_failed': [],
        'submit_failed': [],
        'no_date': []
    }

    total = len(orders)
    for i, order in enumerate(orders):
        so_name = order['name']
        posting_date = order.get('custom_date_delivered') or order.get('transaction_date')

        if not posting_date:
            results['no_date'].append(so_name)
            continue

        try:
            # Create DN
            response = client.create_delivery_note(so_name, posting_date, default_warehouse)

            if response.get('data', {}).get('name'):
                dn_name = response['data']['name']
                results['created'] += 1

                # Submit DN
                submit_response = client.submit_document('Delivery Note', dn_name)
                if submit_response.get('error'):
                    results['submit_failed'].append({
                        'so_name': so_name,
                        'dn_name': dn_name,
                        'error': str(submit_response['error'])[:100]
                    })
                else:
                    results['submitted'] += 1
            else:
                error = response.get('error', 'Unknown error')
                results['create_failed'].append({
                    'so_name': so_name,
                    'error': str(error)[:150]
                })

            # Progress
            if (i + 1) % 100 == 0:
                print(f'  Processed {i+1}/{total} orders...')

            # Rate limiting
            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(0.5)

        except Exception as e:
            results['create_failed'].append({
                'so_name': so_name,
                'error': str(e)[:100]
            })

    # Summary
    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    print(f"Delivery Notes Created:   {results['created']}")
    print(f"Delivery Notes Submitted: {results['submitted']}")
    print(f"Create Failures:          {len(results['create_failed'])}")
    print(f"Submit Failures:          {len(results['submit_failed'])}")
    print(f"Orders with No Date:      {len(results['no_date'])}")

    if results['create_failed']:
        print('\nCreate Failures (first 5):')
        for f in results['create_failed'][:5]:
            print(f"  - {f['so_name']}: {f['error'][:60]}")

    if results['submit_failed']:
        print('\nSubmit Failures (first 5):')
        for f in results['submit_failed'][:5]:
            print(f"  - {f['so_name']} ({f['dn_name']}): {f['error'][:60]}")

    if results['no_date']:
        print(f'\nOrders with No Date (first 5): {results["no_date"][:5]}')

    # Exit code
    if results['create_failed'] or results['submit_failed'] or results['no_date']:
        sys.exit(1)


if __name__ == '__main__':
    main()
