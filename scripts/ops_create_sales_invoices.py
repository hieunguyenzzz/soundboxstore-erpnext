#!/usr/bin/env python3
"""
SBS-64: Create Sales Invoices for DESPATCHED Orders

This script creates and submits Sales Invoices for all DESPATCHED orders
that are currently "To Bill" status. After running, these orders will
become "Completed" status.

Business Rule: FULLY PAID orders (DESPATCHED) must have status "Completed"

Usage:
  python scripts/ops_create_sales_invoices.py --dry-run  # Preview
  python scripts/ops_create_sales_invoices.py            # Execute
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

    def get_despatched_to_bill_orders(self):
        """Get all DESPATCHED orders with 'To Bill' status"""
        response = self.session.get(
            f'{self.url}/api/resource/Sales Order',
            params={
                'filters': json.dumps([
                    ['docstatus', '=', 1],
                    ['custom_stock_status', '=', 'DESPATCHED'],
                    ['status', '=', 'To Bill']
                ]),
                'fields': json.dumps(['name', 'customer', 'grand_total', 'transaction_date']),
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

    def get_default_income_account(self):
        """Get default income account for items"""
        response = self.session.get(
            f'{self.url}/api/resource/Company/{self.company_name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            company = response.json().get('data', {})
            return company.get('default_income_account')
        return None

    def create_sales_invoice(self, so_name, posting_date=None):
        """Create Sales Invoice from Sales Order"""
        so = self.get_sales_order(so_name)
        if not so:
            return {'error': f'Could not fetch Sales Order {so_name}'}

        # Build invoice items from SO items
        si_items = []
        for item in so.get('items', []):
            si_items.append({
                'item_code': item['item_code'],
                'item_name': item['item_name'],
                'qty': item['qty'],
                'rate': item['rate'],
                'amount': item['amount'],
                'sales_order': so_name,
                'so_detail': item['name'],
                'warehouse': item.get('warehouse')
            })

        if not si_items:
            return {'error': 'No items to invoice'}

        # Use custom_date_delivered if available (when order was actually delivered/completed)
        # Otherwise use transaction_date (order date)
        invoice_date = so.get('custom_date_delivered') or so.get('transaction_date')

        si_data = {
            'doctype': 'Sales Invoice',
            'naming_series': 'ACC-SINV-.YYYY.-',
            'company': self.company_name,
            'customer': so['customer'],
            'posting_date': invoice_date,
            'due_date': invoice_date,  # Same as posting for FULLY PAID
            'set_posting_time': 1,  # Allow backdated posting
            'items': si_items,
            'update_stock': 0,  # Stock already updated via DN
            'is_pos': 0,
            'allocate_advances_automatically': 1  # Auto-allocate Payment Entry
        }

        response = self.session.post(
            f'{self.url}/api/resource/Sales Invoice',
            json=si_data,
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
        # First get the document
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return {'error': f'Could not fetch {doctype} {name}'}

        doc = response.json().get('data', {})

        # Submit it
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
    parser = argparse.ArgumentParser(description='Create Sales Invoices for DESPATCHED orders')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of invoices to create')
    args = parser.parse_args()

    # Get config from environment
    url = os.environ.get('ERPNEXT_URL')
    api_key = os.environ.get('ERPNEXT_API_KEY')
    api_secret = os.environ.get('ERPNEXT_API_SECRET')

    if not all([url, api_key, api_secret]):
        print('ERROR: Missing ERPNEXT_URL, ERPNEXT_API_KEY, or ERPNEXT_API_SECRET')
        sys.exit(1)

    print('=' * 60)
    print('SBS-64: Create Sales Invoices for DESPATCHED Orders')
    print('=' * 60)

    if args.dry_run:
        print('\n*** DRY RUN MODE - No changes will be made ***\n')

    client = ERPNextClient(url, api_key, api_secret)

    # Get orders needing invoices
    print('\nFinding DESPATCHED orders with "To Bill" status...')
    orders = client.get_despatched_to_bill_orders()
    print(f'Found {len(orders)} orders needing Sales Invoices')

    if args.limit:
        orders = orders[:args.limit]
        print(f'Limited to {len(orders)} orders')

    if args.dry_run:
        print('\nSample orders that would be invoiced:')
        for order in orders[:10]:
            print(f"  - {order['name']}: {order['customer'][:40]} - {order['grand_total']}")
        print(f'\n{len(orders)} Sales Invoices would be created and submitted.')
        return

    # Create and submit invoices
    results = {
        'created': 0,
        'submitted': 0,
        'create_failed': [],
        'submit_failed': []
    }

    total = len(orders)
    for i, order in enumerate(orders):
        so_name = order['name']

        try:
            # Create SI
            response = client.create_sales_invoice(so_name)

            if response.get('data', {}).get('name'):
                si_name = response['data']['name']
                results['created'] += 1

                # Submit SI
                submit_response = client.submit_document('Sales Invoice', si_name)
                if submit_response.get('error'):
                    results['submit_failed'].append({
                        'so_name': so_name,
                        'si_name': si_name,
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
            if (i + 1) % 50 == 0:
                print(f'  Processed {i+1}/{total} orders...')

            # Rate limiting
            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(1)

        except Exception as e:
            results['create_failed'].append({
                'so_name': so_name,
                'error': str(e)[:100]
            })

    # Summary
    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    print(f"Sales Invoices Created:  {results['created']}")
    print(f"Sales Invoices Submitted: {results['submitted']}")
    print(f"Create Failures:         {len(results['create_failed'])}")
    print(f"Submit Failures:         {len(results['submit_failed'])}")

    if results['create_failed']:
        print('\nCreate Failures (first 5):')
        for f in results['create_failed'][:5]:
            print(f"  - {f['so_name']}: {f['error'][:60]}")

    if results['submit_failed']:
        print('\nSubmit Failures (first 5):')
        for f in results['submit_failed'][:5]:
            print(f"  - {f['so_name']} ({f['si_name']}): {f['error'][:60]}")

    # Exit code
    if results['create_failed'] or results['submit_failed']:
        sys.exit(1)


if __name__ == '__main__':
    main()
