#!/usr/bin/env python3
"""
SBS-64: Submit Draft Delivery Notes

This script submits all Draft Delivery Notes that failed during migration.
For items with zero valuation rate, it enables the allow_zero_valuation_rate flag.
For any DNs that still fail to submit, it closes the linked Sales Order.

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)

Usage:
  python scripts/ops_submit_draft_dns.py
  python scripts/ops_submit_draft_dns.py --dry-run  # Preview without changes
"""

import os
import sys
import json
import argparse
import tempfile
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
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
    """ERPNext API Client for DN operations"""

    def __init__(self, config):
        self.url = config['url'].rstrip('/')
        self.session = create_session_with_retry()
        self.headers = {
            'Authorization': f"token {config['api_key']}:{config['api_secret']}",
            'Content-Type': 'application/json'
        }
        self._verify_connection()

    def _verify_connection(self):
        """Verify API connection"""
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API connection failed: {response.status_code}')
        user = response.json().get('message', 'Unknown')
        print(f'Connected to ERPNext at {self.url} as {user}')

    def get_draft_dns(self):
        """Get all Draft Delivery Notes (docstatus=0)"""
        response = self.session.get(
            f'{self.url}/api/resource/Delivery Note',
            params={
                'filters': json.dumps([['docstatus', '=', 0]]),
                'fields': '["name"]',
                'limit_page_length': 0
            },
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Failed to get DNs: {response.status_code}')
        return [d['name'] for d in response.json().get('data', [])]

    def get_document(self, doctype, name):
        """Get full document with child tables using frappe.client.get"""
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.get',
            json={'doctype': doctype, 'name': name},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return None
        return response.json().get('message')

    def save_document(self, doc):
        """Save a document using frappe.client.save"""
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.save',
            json={'doc': doc},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', str(err)[:200])}
            except:
                return {'error': f'HTTP {response.status_code}'}
        return response.json()

    def submit_document(self, doc):
        """Submit a document using frappe.client.submit"""
        response = self.session.post(
            f'{self.url}/api/method/frappe.client.submit',
            json={'doc': doc},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', str(err)[:200])}
            except:
                return {'error': f'HTTP {response.status_code}'}
        return response.json()

    def get_linked_so(self, dn_name):
        """Get the Sales Order linked to a Delivery Note"""
        doc = self.get_document('Delivery Note', dn_name)
        if doc and doc.get('items'):
            # Get SO from first item's against_sales_order
            for item in doc['items']:
                so = item.get('against_sales_order')
                if so:
                    return so
        return None

    def close_sales_order(self, so_name):
        """Close a Sales Order by updating its status"""
        response = self.session.put(
            f'{self.url}/api/resource/Sales Order/{so_name}',
            json={'status': 'Closed'},
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                err = response.json()
                return {'error': err.get('exception', str(err)[:200])}
            except:
                return {'error': f'HTTP {response.status_code}'}
        return response.json()


def submit_draft_dns(client, dry_run=False):
    """Submit all Draft Delivery Notes with zero valuation rate bypass"""
    results = {
        'total': 0,
        'submitted': 0,
        'so_closed': 0,
        'failed': [],
        'dry_run': dry_run
    }

    # Get all draft DNs
    print('\n1. Getting Draft Delivery Notes...')
    draft_dns = client.get_draft_dns()
    results['total'] = len(draft_dns)
    print(f'   Found {len(draft_dns)} Draft DNs')

    if not draft_dns:
        print('   No Draft DNs to process')
        return results

    print('\n2. Processing DNs...')
    for i, dn_name in enumerate(draft_dns):
        try:
            # Get full document with items
            doc = client.get_document('Delivery Note', dn_name)
            if not doc:
                results['failed'].append({'dn': dn_name, 'error': 'Could not fetch document'})
                continue

            # Enable zero valuation rate on all items
            for item in doc.get('items', []):
                item['allow_zero_valuation_rate'] = 1

            if dry_run:
                print(f'   [{i+1}/{len(draft_dns)}] Would submit: {dn_name}')
                results['submitted'] += 1
                continue

            # Save with updated items
            save_result = client.save_document(doc)
            if save_result.get('error'):
                # Try to close the linked SO instead
                so_name = client.get_linked_so(dn_name)
                if so_name:
                    close_result = client.close_sales_order(so_name)
                    if not close_result.get('error'):
                        results['so_closed'] += 1
                        print(f'   [{i+1}/{len(draft_dns)}] Closed SO {so_name} (DN save failed)')
                        continue
                results['failed'].append({'dn': dn_name, 'error': save_result['error'][:100]})
                continue

            # Get updated document after save
            doc = client.get_document('Delivery Note', dn_name)
            if not doc:
                results['failed'].append({'dn': dn_name, 'error': 'Could not fetch after save'})
                continue

            # Submit
            submit_result = client.submit_document(doc)
            if submit_result.get('error'):
                # Try to close the linked SO instead
                so_name = client.get_linked_so(dn_name)
                if so_name:
                    close_result = client.close_sales_order(so_name)
                    if not close_result.get('error'):
                        results['so_closed'] += 1
                        print(f'   [{i+1}/{len(draft_dns)}] Closed SO {so_name} (DN submit failed)')
                        continue
                results['failed'].append({'dn': dn_name, 'error': submit_result['error'][:100]})
                continue

            results['submitted'] += 1
            if (i + 1) % 50 == 0 or (i + 1) == len(draft_dns):
                print(f'   Processed {i+1}/{len(draft_dns)} DNs')

        except Exception as e:
            results['failed'].append({'dn': dn_name, 'error': str(e)[:100]})

    return results


def main():
    parser = argparse.ArgumentParser(description='Submit Draft Delivery Notes')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: Submit Draft Delivery Notes')
    print('=' * 60)

    # Load config
    config = get_config()

    # Connect to ERPNext
    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(config)

    # Process DNs
    results = submit_draft_dns(client, dry_run=args.dry_run)

    # Print summary
    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    if results['dry_run']:
        print('[DRY RUN - No changes made]')
    print(f"Total Draft DNs:  {results['total']}")
    print(f"Submitted:        {results['submitted']}")
    print(f"SOs Closed:       {results['so_closed']}")
    print(f"Failed:           {len(results['failed'])}")

    if results['failed']:
        print(f"\nFailures (first 10):")
        for f in results['failed'][:10]:
            print(f"  - {f['dn']}: {f['error']}")

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(tempfile.gettempdir(), f'submit_dns_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\nReport saved to: {report_path}')

    return 0 if not results['failed'] else 1


if __name__ == '__main__':
    sys.exit(main())
