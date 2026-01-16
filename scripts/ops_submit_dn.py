#!/usr/bin/env python3
"""
Submit Draft Delivery Notes
Submits all Delivery Notes in Draft status to mark Sales Orders as delivered.

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)

Usage:
  python scripts/submit_delivery_notes.py
  python scripts/submit_delivery_notes.py --dry-run  # Preview without submitting
"""

import os
import sys
import time
import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 30


def get_config():
    """Load configuration from environment variables"""
    config = {
        'url': os.environ.get('ERPNEXT_URL'),
        'api_key': os.environ.get('ERPNEXT_API_KEY'),
        'api_secret': os.environ.get('ERPNEXT_API_SECRET'),
    }

    missing = []
    if not config['url']:
        missing.append('ERPNEXT_URL')
    if not config['api_key']:
        missing.append('ERPNEXT_API_KEY')
    if not config['api_secret']:
        missing.append('ERPNEXT_API_SECRET')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
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

    def get_draft_delivery_notes(self, limit=500):
        """Get all Delivery Notes in Draft status (docstatus=0)"""
        all_dns = []
        offset = 0

        while True:
            response = self.session.post(
                f'{self.url}/api/method/frappe.client.get_list',
                json={
                    'doctype': 'Delivery Note',
                    'filters': [['docstatus', '=', 0]],
                    'fields': ['name', 'customer', 'posting_date'],
                    'limit_page_length': limit,
                    'limit_start': offset
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                break

            data = response.json().get('message', [])
            if not data:
                break

            all_dns.extend(data)
            offset += limit

            if len(data) < limit:
                break

        return all_dns

    def submit_document(self, doctype, name):
        """Submit a document (fetch full doc first, then submit)"""
        # First fetch the full document
        get_response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if get_response.status_code != 200:
            return {'error': f'Could not fetch {doctype} {name}'}

        doc = get_response.json().get('data', {})
        if not doc:
            return {'error': f'Empty document for {doctype} {name}'}

        # Now submit the full document
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 201):
            try:
                error = response.json().get('exception', response.text[:200])
            except:
                error = f'HTTP {response.status_code}'
            return {'error': error}

        return {'success': True}


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Submit Draft Delivery Notes')
    parser.add_argument('--dry-run', action='store_true', help='Preview without submitting')
    args = parser.parse_args()

    print('=' * 60)
    print('Submit Draft Delivery Notes')
    if args.dry_run:
        print('MODE: DRY RUN (no changes will be made)')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    print('\n2. Getting Draft Delivery Notes...')
    dns = client.get_draft_delivery_notes()
    total = len(dns)

    if total == 0:
        print('   No Draft Delivery Notes found')
        return

    print(f'   Found {total} Draft Delivery Notes')

    print('\n3. Submitting Delivery Notes...')
    results = {'submitted': 0, 'errors': [], 'error_count': 0}

    for i, dn in enumerate(dns, 1):
        name = dn['name']

        if args.dry_run:
            print(f"   [{i}/{total}] Would submit: {name}")
            results['submitted'] += 1
            continue

        result = client.submit_document('Delivery Note', name)

        if result.get('error'):
            error_msg = str(result['error'])[:100]
            results['error_count'] += 1
            if results['error_count'] <= 10:  # Only store first 10 errors
                results['errors'].append({
                    'name': name,
                    'customer': dn.get('customer', 'Unknown'),
                    'error': error_msg
                })
            if results['error_count'] <= 5:
                print(f"   [{i}/{total}] Error: {name} - {error_msg}")
        else:
            results['submitted'] += 1

        if i % 100 == 0 or i == total:
            print(f"   Progress: {i}/{total} ({results['submitted']} submitted, {results['error_count']} errors)")

        time.sleep(0.05)  # Rate limiting

    # Summary
    print('\n' + '=' * 60)
    print('SUBMISSION COMPLETE')
    print('=' * 60)
    print(f'Total Draft DNs:   {total}')
    print(f'Submitted:         {results["submitted"]}')
    print(f'Errors:            {results["error_count"]}')

    if results['errors']:
        print(f'\nFirst {len(results["errors"])} errors:')
        for err in results['errors']:
            print(f"  - {err['name']} ({err['customer']}): {err['error'][:60]}")

    if args.dry_run:
        print('\nThis was a DRY RUN. No changes were made.')

    sys.exit(1 if results['error_count'] > 0 else 0)


if __name__ == '__main__':
    main()
