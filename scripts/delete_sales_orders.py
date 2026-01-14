#!/usr/bin/env python3
"""
Delete Sales Orders and Delivery Notes
Targeted cleanup for re-migration of Sales Orders with correct rates.

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)

Usage:
  python scripts/delete_sales_orders.py
  python scripts/delete_sales_orders.py --dry-run  # Preview without deleting
"""

import os
import sys
import json
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

    def get_all_documents(self, doctype, limit=500):
        """Get all documents of a DocType"""
        all_docs = []
        offset = 0

        while True:
            params = {
                'limit_page_length': limit,
                'limit_start': offset,
                'fields': json.dumps(['name', 'docstatus'])
            }

            response = self.session.get(
                f'{self.url}/api/resource/{doctype}',
                params=params,
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                break

            data = response.json().get('data', [])
            if not data:
                break

            all_docs.extend(data)
            offset += limit

            if len(data) < limit:
                break

        return all_docs

    def cancel_document(self, doctype, name):
        """Cancel a submitted document"""
        response = self.session.get(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code != 200:
            return {'error': f'Failed to get document: {response.status_code}'}

        doc = response.json().get('data')
        if not doc:
            return {'error': 'Document not found'}

        if doc.get('docstatus') != 1:
            return {'skip': True, 'reason': 'Not submitted'}

        response = self.session.post(
            f'{self.url}/api/method/frappe.client.cancel',
            json={'doctype': doctype, 'name': name},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 201):
            return {'error': f'Cancel failed: {response.status_code}'}

        return {'success': True}

    def delete_document(self, doctype, name):
        """Delete a document"""
        response = self.session.delete(
            f'{self.url}/api/resource/{doctype}/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 202):
            try:
                error = response.json().get('exception', response.text[:100])
            except:
                error = f'HTTP {response.status_code}'
            return {'error': error}

        return {'success': True}


def delete_doctype(client, doctype, dry_run=False, skip_cancel=False):
    """Delete all documents of a DocType"""
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Clearing {doctype}...")

    docs = client.get_all_documents(doctype)
    total = len(docs)

    if total == 0:
        print(f"  No documents to delete")
        return {'total': 0, 'deleted': 0, 'errors': 0}

    print(f"  Found {total} documents")

    results = {'total': total, 'deleted': 0, 'cancelled': 0, 'errors': 0}

    for i, doc in enumerate(docs, 1):
        name = doc['name']
        docstatus = doc.get('docstatus', 0)

        if dry_run:
            action = "Would cancel and delete" if docstatus == 1 and not skip_cancel else "Would delete"
            if i <= 5 or i % 1000 == 0 or i == total:
                print(f"  [{i}/{total}] {action}: {name}")
            results['deleted'] += 1
            continue

        # Cancel if submitted (unless skip_cancel is True for system docs like SLE)
        if docstatus == 1 and not skip_cancel:
            result = client.cancel_document(doctype, name)
            if result.get('error'):
                print(f"  [{i}/{total}] Cancel failed: {name} - {result['error'][:50]}")
                results['errors'] += 1
                continue
            elif result.get('success'):
                results['cancelled'] += 1
                time.sleep(0.2)

        # Delete
        result = client.delete_document(doctype, name)
        if result.get('error'):
            if i <= 10 or results['errors'] <= 5:
                print(f"  [{i}/{total}] Delete failed: {name} - {result['error'][:80]}")
            results['errors'] += 1
            continue

        results['deleted'] += 1

        if i % 500 == 0 or i == total:
            print(f"  Progress: {i}/{total} ({results['deleted']} deleted, {results['errors']} errors)")

        time.sleep(0.02)

    print(f"  Completed: {results['deleted']} deleted, {results['errors']} errors")
    return results


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Delete Sales Orders and Delivery Notes')
    parser.add_argument('--dry-run', action='store_true', help='Preview without deleting')
    args = parser.parse_args()

    print('=' * 60)
    print('Delete Sales Orders and Delivery Notes')
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

    print('\n2. Deleting in reverse dependency order...')

    # Delete Stock Ledger Entries first (they link to Delivery Notes)
    # SLEs are system docs - skip cancel, just delete
    sle_results = delete_doctype(client, 'Stock Ledger Entry', args.dry_run, skip_cancel=True)

    # Delete Delivery Notes (they depend on Sales Orders)
    dn_results = delete_doctype(client, 'Delivery Note', args.dry_run)

    # Delete Container Pre-Allocations (they depend on Sales Orders)
    try:
        cpa_results = delete_doctype(client, 'Container Pre-Allocation', args.dry_run)
    except Exception as e:
        print(f"\n  Container Pre-Allocation: {e}")
        cpa_results = {'total': 0, 'deleted': 0, 'errors': 0}

    # Delete Sales Orders
    so_results = delete_doctype(client, 'Sales Order', args.dry_run)

    # Summary
    print('\n' + '=' * 60)
    print('DELETION COMPLETE')
    print('=' * 60)
    print(f'Stock Ledger Entries:       {sle_results["deleted"]} deleted, {sle_results["errors"]} errors')
    print(f'Delivery Notes:             {dn_results["deleted"]} deleted, {dn_results["errors"]} errors')
    print(f'Container Pre-Allocations:  {cpa_results["deleted"]} deleted, {cpa_results["errors"]} errors')
    print(f'Sales Orders:               {so_results["deleted"]} deleted, {so_results["errors"]} errors')

    if args.dry_run:
        print('\nThis was a DRY RUN. No data was actually deleted.')

    total_errors = sle_results['errors'] + dn_results['errors'] + cpa_results['errors'] + so_results['errors']
    sys.exit(1 if total_errors > 0 else 0)


if __name__ == '__main__':
    main()
