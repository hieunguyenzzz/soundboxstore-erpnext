#!/usr/bin/env python3
"""
Fix Items Missing Valuation Rate
Sets valuation_rate = standard_rate for items where valuation_rate is 0 or missing.

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)

Usage:
  python scripts/fix_valuation_rates.py
  python scripts/fix_valuation_rates.py --dry-run  # Preview without updating
  python scripts/fix_valuation_rates.py --default-rate 100  # Use specific default rate
"""

import os
import sys
import time
import argparse
import requests

REQUEST_TIMEOUT = 30
DEFAULT_VALUATION_RATE = 100.0  # Default if no standard_rate available


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


class ERPNextClient:
    """ERPNext API Client"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = requests.Session()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }

    def get_items_missing_valuation(self, limit=500):
        """Get all items with valuation_rate = 0 or NULL"""
        all_items = []
        offset = 0

        while True:
            # Get items where valuation_rate is 0 or very small
            response = self.session.post(
                f'{self.url}/api/method/frappe.client.get_list',
                json={
                    'doctype': 'Item',
                    'filters': [['valuation_rate', '<', 0.01]],
                    'fields': ['name', 'item_name', 'valuation_rate', 'standard_rate'],
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

            all_items.extend(data)
            offset += limit

            if len(data) < limit:
                break

        return all_items

    def update_item_valuation_rate(self, item_code, valuation_rate):
        """Update an item's valuation_rate"""
        response = self.session.put(
            f'{self.url}/api/resource/Item/{item_code}',
            json={'valuation_rate': valuation_rate},
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
    parser = argparse.ArgumentParser(description='Fix Items Missing Valuation Rate')
    parser.add_argument('--dry-run', action='store_true', help='Preview without updating')
    parser.add_argument('--default-rate', type=float, default=DEFAULT_VALUATION_RATE,
                        help=f'Default rate if no standard_rate (default: {DEFAULT_VALUATION_RATE})')
    args = parser.parse_args()

    print('=' * 60)
    print('Fix Items Missing Valuation Rate')
    if args.dry_run:
        print('MODE: DRY RUN (no changes will be made)')
    print('=' * 60)

    config = get_config()
    client = ERPNextClient(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    print('\n1. Finding items with missing valuation rates...')
    items = client.get_items_missing_valuation()
    total = len(items)

    if total == 0:
        print('   No items found with missing valuation rates')
        return

    print(f'   Found {total} items')

    print('\n2. Updating valuation rates...')
    results = {'updated': 0, 'errors': 0}

    for i, item in enumerate(items, 1):
        item_code = item['name']
        standard_rate = item.get('standard_rate') or 0

        # Use standard_rate if available, otherwise default
        new_rate = standard_rate if standard_rate > 0 else args.default_rate

        if args.dry_run:
            print(f"   [{i}/{total}] Would set {item_code}: {new_rate}")
            results['updated'] += 1
            continue

        result = client.update_item_valuation_rate(item_code, new_rate)

        if result.get('error'):
            print(f"   [{i}/{total}] Error: {item_code} - {result['error'][:50]}")
            results['errors'] += 1
        else:
            results['updated'] += 1

        if i % 100 == 0 or i == total:
            print(f"   Progress: {i}/{total} ({results['updated']} updated)")

        time.sleep(0.02)

    print('\n' + '=' * 60)
    print('COMPLETE')
    print('=' * 60)
    print(f'Total items:  {total}')
    print(f'Updated:      {results["updated"]}')
    print(f'Errors:       {results["errors"]}')

    if args.dry_run:
        print('\nThis was a DRY RUN. No changes were made.')


if __name__ == '__main__':
    main()
