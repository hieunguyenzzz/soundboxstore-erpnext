#!/usr/bin/env python3
"""
Enable/Disable Allow Negative Stock Setting

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)

Usage:
  python scripts/enable_negative_stock.py --enable   # Enable Allow Negative Stock
  python scripts/enable_negative_stock.py --disable  # Disable Allow Negative Stock
  python scripts/enable_negative_stock.py --status   # Check current status
"""

import os
import sys
import argparse
import requests

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


class ERPNextClient:
    """ERPNext API Client"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = requests.Session()
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

    def get_stock_settings(self):
        """Get current Stock Settings"""
        response = self.session.get(
            f'{self.url}/api/resource/Stock%20Settings/Stock%20Settings',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Failed to get Stock Settings: {response.status_code}')
        return response.json().get('data', {})

    def update_stock_settings(self, allow_negative_stock):
        """Update Allow Negative Stock setting"""
        # First get the current document
        settings = self.get_stock_settings()

        # Update the setting
        settings['allow_negative_stock'] = 1 if allow_negative_stock else 0

        response = self.session.put(
            f'{self.url}/api/resource/Stock%20Settings/Stock%20Settings',
            json=settings,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code not in (200, 201):
            try:
                error = response.json().get('exception', response.text[:200])
            except:
                error = f'HTTP {response.status_code}'
            raise Exception(f'Failed to update Stock Settings: {error}')

        return response.json().get('data', {})


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Enable/Disable Allow Negative Stock')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--enable', action='store_true', help='Enable Allow Negative Stock')
    group.add_argument('--disable', action='store_true', help='Disable Allow Negative Stock')
    group.add_argument('--status', action='store_true', help='Check current status')
    args = parser.parse_args()

    config = get_config()
    client = ERPNextClient(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    if args.status:
        settings = client.get_stock_settings()
        status = 'ENABLED' if settings.get('allow_negative_stock') else 'DISABLED'
        print(f'\nAllow Negative Stock: {status}')
        return

    if args.enable:
        print('\nEnabling Allow Negative Stock...')
        client.update_stock_settings(True)
        print('✓ Allow Negative Stock is now ENABLED')
    elif args.disable:
        print('\nDisabling Allow Negative Stock...')
        client.update_stock_settings(False)
        print('✓ Allow Negative Stock is now DISABLED')


if __name__ == '__main__':
    main()
