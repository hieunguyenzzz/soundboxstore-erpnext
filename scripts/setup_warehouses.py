#!/usr/bin/env python3
"""
Create Location-Based Warehouses for SoundboxStore ERPNext

This script creates regional warehouses:
- UK Warehouse - SBS
- Spain Warehouse - SBS
- US Warehouse - SBS

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)

Or use API Key authentication:
  ERPNEXT_API_KEY      - ERPNext API Key
  ERPNEXT_API_SECRET   - ERPNext API Secret
"""

import os
import sys
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
REQUEST_TIMEOUT = 30  # seconds

# Warehouses to create
WAREHOUSES = [
    {"warehouse_name": "UK Warehouse", "is_group": 0},
    {"warehouse_name": "Spain Warehouse", "is_group": 0},
    {"warehouse_name": "US Warehouse", "is_group": 0},
]


def get_config():
    """Load configuration from environment variables"""
    config = {
        'url': os.environ.get('ERPNEXT_URL'),
        'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
        'password': os.environ.get('ERPNEXT_PASSWORD'),
        'api_key': os.environ.get('ERPNEXT_API_KEY'),
        'api_secret': os.environ.get('ERPNEXT_API_SECRET'),
    }

    if not config['url']:
        print("ERROR: Missing required environment variable: ERPNEXT_URL")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL          - ERPNext server URL (e.g., http://100.65.0.28:8080)")
        print("\nAuthentication (choose one):")
        print("  Option 1 - Password auth:")
        print("    ERPNEXT_USERNAME   - ERPNext username (default: Administrator)")
        print("    ERPNEXT_PASSWORD   - ERPNext password")
        print("  Option 2 - API Key auth:")
        print("    ERPNEXT_API_KEY    - ERPNext API Key")
        print("    ERPNEXT_API_SECRET - ERPNext API Secret")
        sys.exit(1)

    # Check for either password or API key auth
    has_password = bool(config['password'])
    has_api_key = bool(config['api_key'] and config['api_secret'])

    if not has_password and not has_api_key:
        print("ERROR: Missing authentication credentials")
        print("Provide either ERPNEXT_PASSWORD or (ERPNEXT_API_KEY and ERPNEXT_API_SECRET)")
        sys.exit(1)

    config['use_api_key'] = has_api_key and not has_password

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

    def __init__(self, config):
        self.url = config['url'].rstrip('/')
        self.session = create_session_with_retry()

        if config['use_api_key']:
            self.setup_api_key_auth(config['api_key'], config['api_secret'])
        else:
            self.login(config['username'], config['password'])

    def setup_api_key_auth(self, api_key, api_secret):
        """Setup API Key authentication"""
        self.session.headers.update({
            'Authorization': f'token {api_key}:{api_secret}'
        })
        print(f'Using API Key authentication for {self.url}')
        # Verify connection
        response = self.session.get(
            f'{self.url}/api/method/frappe.auth.get_logged_user',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'API Key authentication failed: {response.status_code}')
        user = response.json().get('message', 'Unknown')
        print(f'Authenticated as: {user}')

    def login(self, username, password):
        """Login and get session cookie"""
        response = self.session.post(
            f'{self.url}/api/method/login',
            data={'usr': username, 'pwd': password},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Login failed with status {response.status_code}')
        if 'Logged In' not in response.text:
            raise Exception('Login failed: Invalid credentials')
        print(f'Logged in to ERPNext at {self.url}')

    def get_company(self):
        """Get the first company and its abbreviation"""
        response = self.session.get(
            f'{self.url}/api/resource/Company',
            params={'fields': '["name","abbr"]', 'limit_page_length': 1},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            raise Exception(f'Failed to get company: {response.status_code}')

        data = response.json().get('data', [])
        if not data:
            raise Exception('No company found in ERPNext')

        return data[0]

    def get_parent_warehouse(self, company_abbr):
        """Get the parent warehouse (All Warehouses) for the company"""
        # Try common parent warehouse names
        possible_names = [
            f"All Warehouses - {company_abbr}",
            f"Stores - {company_abbr}",
        ]

        for name in possible_names:
            response = self.session.get(
                f'{self.url}/api/resource/Warehouse/{name}',
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 200:
                return name

        # If not found, list all group warehouses
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse',
            params={
                'filters': json.dumps([['is_group', '=', 1]]),
                'fields': '["name"]',
                'limit_page_length': 10
            },
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            warehouses = response.json().get('data', [])
            if warehouses:
                return warehouses[0]['name']

        return None

    def warehouse_exists(self, warehouse_name):
        """Check if warehouse exists"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_warehouse(self, data):
        """Create a warehouse in ERPNext"""
        response = self.session.post(
            f'{self.url}/api/resource/Warehouse',
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            error_text = response.text[:200]
            return {'error': f'HTTP {response.status_code}: {error_text}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}

    def get_warehouse(self, warehouse_name):
        """Get warehouse details"""
        response = self.session.get(
            f'{self.url}/api/resource/Warehouse/{warehouse_name}',
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None


def create_warehouses(client):
    """Create location-based warehouses"""
    results = {
        'created': 0,
        'existing': 0,
        'failed': 0,
        'errors': []
    }

    # Get company info
    print('\n1. Getting company information...')
    company = client.get_company()
    company_name = company['name']
    company_abbr = company['abbr']
    print(f'   Company: {company_name} (abbr: {company_abbr})')

    # Get parent warehouse
    print('\n2. Finding parent warehouse...')
    parent_warehouse = client.get_parent_warehouse(company_abbr)
    if parent_warehouse:
        print(f'   Parent warehouse: {parent_warehouse}')
    else:
        print('   WARNING: No parent warehouse found, warehouses will be at root level')

    # Create warehouses
    print('\n3. Creating warehouses...')
    for wh in WAREHOUSES:
        warehouse_name = wh['warehouse_name']
        full_name = f"{warehouse_name} - {company_abbr}"

        # Check if already exists
        if client.warehouse_exists(full_name):
            print(f'   [EXISTS] {full_name}')
            results['existing'] += 1
            continue

        # Prepare warehouse data
        warehouse_data = {
            'warehouse_name': warehouse_name,
            'company': company_name,
            'is_group': wh.get('is_group', 0),
        }

        if parent_warehouse:
            warehouse_data['parent_warehouse'] = parent_warehouse

        # Create warehouse
        response = client.create_warehouse(warehouse_data)

        if response.get('data', {}).get('name'):
            print(f'   [CREATED] {full_name}')
            results['created'] += 1
        else:
            error = response.get('error', 'Unknown error')
            print(f'   [FAILED] {full_name}: {error}')
            results['failed'] += 1
            results['errors'].append({
                'warehouse': full_name,
                'error': error
            })

    return results


def main():
    """Main function"""
    print('=' * 60)
    print('Create Location-Based Warehouses')
    print('=' * 60)

    config = get_config()

    print('\nConnecting to ERPNext...')
    try:
        client = ERPNextClient(config)
    except Exception as e:
        print(f'ERROR: Failed to connect to ERPNext: {e}')
        sys.exit(1)

    results = create_warehouses(client)

    print('\n' + '=' * 60)
    print('WAREHOUSE CREATION COMPLETE')
    print('=' * 60)
    print(f'Created:  {results["created"]}')
    print(f'Existing: {results["existing"]}')
    print(f'Failed:   {results["failed"]}')

    if results['errors']:
        print('\nErrors:')
        for err in results['errors']:
            print(f'  - {err["warehouse"]}: {err["error"]}')

    sys.exit(1 if results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
