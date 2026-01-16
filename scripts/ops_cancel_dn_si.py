#!/usr/bin/env python3
"""
SBS-64: Cancel Sales Invoices and Delivery Notes

This script cancels all submitted Sales Invoices and Delivery Notes
to allow recreation with correct historical dates.

Must cancel in order:
1. Sales Invoices (depend on DN)
2. Delivery Notes (depend on SO)

Usage:
  python scripts/ops_cancel_dn_si.py --dry-run  # Preview
  python scripts/ops_cancel_dn_si.py            # Execute
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
        print(f'Connected to {self.url}')

    def _verify_connection(self):
        """Verify API connection"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed: {response.status_code}')

    def get_submitted_documents(self, doctype):
        """Get all submitted documents of a type"""
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}',
            params={
                'filters': json.dumps([['docstatus', '=', 1]]),
                'fields': json.dumps(['name']),
                'limit_page_length': 0
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.json().get('data', [])

    def cancel_document(self, doctype, name):
        """Cancel a submitted document"""
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.cancel',
            json={
                'doctype': doctype,
                'name': name
            },
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
    parser = argparse.ArgumentParser(description='Cancel Sales Invoices and Delivery Notes')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--si-only', action='store_true', help='Only cancel Sales Invoices')
    parser.add_argument('--dn-only', action='store_true', help='Only cancel Delivery Notes')
    args = parser.parse_args()

    # Get config from environment
    url = os.environ.get('ERPNEXT_URL')
    api_key = os.environ.get('ERPNEXT_API_KEY')
    api_secret = os.environ.get('ERPNEXT_API_SECRET')

    if not all([url, api_key, api_secret]):
        print('ERROR: Missing ERPNEXT_URL, ERPNEXT_API_KEY, or ERPNEXT_API_SECRET')
        sys.exit(1)

    print('=' * 60)
    print('SBS-64: Cancel Sales Invoices and Delivery Notes')
    print('=' * 60)

    if args.dry_run:
        print('\n*** DRY RUN MODE - No changes will be made ***\n')

    client = ERPNextClient(url, api_key, api_secret)

    results = {
        'si_cancelled': 0,
        'si_failed': [],
        'dn_cancelled': 0,
        'dn_failed': []
    }

    # Step 1: Cancel Sales Invoices first (they depend on DN)
    if not args.dn_only:
        print('\n--- Step 1: Cancel Sales Invoices ---')
        invoices = client.get_submitted_documents('Sales Invoice')
        print(f'Found {len(invoices)} submitted Sales Invoices')

        if args.dry_run:
            print(f'Would cancel {len(invoices)} Sales Invoices')
        else:
            for i, inv in enumerate(invoices):
                name = inv['name']
                try:
                    response = client.cancel_document('Sales Invoice', name)
                    if response.get('error'):
                        results['si_failed'].append({'name': name, 'error': str(response['error'])[:100]})
                    else:
                        results['si_cancelled'] += 1

                    if (i + 1) % 100 == 0:
                        print(f'  Cancelled {i+1}/{len(invoices)} Sales Invoices...')

                    if (i + 1) % BATCH_SIZE == 0:
                        time.sleep(0.5)

                except Exception as e:
                    results['si_failed'].append({'name': name, 'error': str(e)[:100]})

            print(f'Cancelled {results["si_cancelled"]} Sales Invoices')
            if results['si_failed']:
                print(f'Failed: {len(results["si_failed"])}')

    # Step 2: Cancel Delivery Notes (they depend on SO)
    if not args.si_only:
        print('\n--- Step 2: Cancel Delivery Notes ---')
        dns = client.get_submitted_documents('Delivery Note')
        print(f'Found {len(dns)} submitted Delivery Notes')

        if args.dry_run:
            print(f'Would cancel {len(dns)} Delivery Notes')
        else:
            for i, dn in enumerate(dns):
                name = dn['name']
                try:
                    response = client.cancel_document('Delivery Note', name)
                    if response.get('error'):
                        results['dn_failed'].append({'name': name, 'error': str(response['error'])[:100]})
                    else:
                        results['dn_cancelled'] += 1

                    if (i + 1) % 100 == 0:
                        print(f'  Cancelled {i+1}/{len(dns)} Delivery Notes...')

                    if (i + 1) % BATCH_SIZE == 0:
                        time.sleep(0.5)

                except Exception as e:
                    results['dn_failed'].append({'name': name, 'error': str(e)[:100]})

            print(f'Cancelled {results["dn_cancelled"]} Delivery Notes')
            if results['dn_failed']:
                print(f'Failed: {len(results["dn_failed"])}')

    # Summary
    if not args.dry_run:
        print('\n' + '=' * 60)
        print('SUMMARY')
        print('=' * 60)
        print(f"Sales Invoices Cancelled: {results['si_cancelled']}")
        print(f"Sales Invoices Failed:    {len(results['si_failed'])}")
        print(f"Delivery Notes Cancelled: {results['dn_cancelled']}")
        print(f"Delivery Notes Failed:    {len(results['dn_failed'])}")

        if results['si_failed']:
            print('\nSI Failures (first 5):')
            for f in results['si_failed'][:5]:
                print(f"  - {f['name']}: {f['error'][:60]}")

        if results['dn_failed']:
            print('\nDN Failures (first 5):')
            for f in results['dn_failed'][:5]:
                print(f"  - {f['name']}: {f['error'][:60]}")

        if results['si_failed'] or results['dn_failed']:
            sys.exit(1)


if __name__ == '__main__':
    main()
