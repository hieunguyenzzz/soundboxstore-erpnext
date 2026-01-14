#!/usr/bin/env python3
"""
Create Container Pre-Allocation DocType in ERPNext

This script creates a custom DocType for tracking pre-selling of stock
before containers arrive. It allows reserving incoming stock for sales orders.

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)
"""

import os
import sys
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
REQUEST_TIMEOUT = 30  # seconds

# Container Pre-Allocation DocType definition
PREALLOCATION_DOCTYPE = {
    "custom": 1,
    "name": "Container Pre-Allocation",
    "module": "Stock",
    "naming_rule": "Expression",
    "autoname": "CPA-.#####",
    "istable": 0,
    "editable_grid": 1,
    "track_changes": 1,
    "fields": [
        {"fieldname": "sales_order", "fieldtype": "Link", "label": "Sales Order", "options": "Sales Order", "reqd": 1, "in_list_view": 1},
        {"fieldname": "container", "fieldtype": "Link", "label": "Container", "options": "Container", "reqd": 1, "in_list_view": 1},
        {"fieldname": "item_code", "fieldtype": "Link", "label": "Item", "options": "Item", "reqd": 1, "in_list_view": 1},
        {"fieldname": "qty", "fieldtype": "Float", "label": "Quantity", "reqd": 1, "in_list_view": 1},
        {"fieldname": "status", "fieldtype": "Select", "label": "Status", "options": "\nReserved\nFulfilled\nCancelled", "default": "Reserved", "in_list_view": 1},
        {"fieldname": "allocation_date", "fieldtype": "Date", "label": "Allocation Date", "default": "Today"},
        {"fieldname": "reserved_by", "fieldtype": "Link", "label": "Reserved By", "options": "User"},
        {"fieldname": "fulfilled_date", "fieldtype": "Date", "label": "Fulfilled Date"},
        {"fieldname": "notes", "fieldtype": "Small Text", "label": "Notes"}
    ],
    "permissions": [
        {"role": "Stock Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
        {"role": "Stock User", "read": 1, "write": 1, "create": 1},
        {"role": "Sales User", "read": 1}
    ]
}


def get_config():
    """Load configuration from environment variables"""
    config = {
        'url': os.environ.get('ERPNEXT_URL'),
        'username': os.environ.get('ERPNEXT_USERNAME', 'Administrator'),
        'password': os.environ.get('ERPNEXT_PASSWORD'),
    }

    missing = []
    if not config['url']:
        missing.append('ERPNEXT_URL')
    if not config['password']:
        missing.append('ERPNEXT_PASSWORD')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL          - ERPNext server URL (e.g., http://100.65.0.28:8080)")
        print("  ERPNEXT_PASSWORD     - ERPNext admin password")
        print("\nOptional:")
        print("  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)")
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

    def __init__(self, url, username, password):
        self.url = url.rstrip('/')
        self.session = create_session_with_retry()
        self.login(username, password)

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

    def doctype_exists(self, doctype_name):
        """Check if a DocType exists"""
        response = self.session.get(
            f'{self.url}/api/resource/DocType/{doctype_name}',
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_doctype(self, doctype_def):
        """Create a custom DocType"""
        response = self.session.post(
            f'{self.url}/api/resource/DocType',
            json=doctype_def,
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code not in (200, 201):
            try:
                error_data = response.json()
                return {'error': error_data.get('exception', error_data.get('message', f'HTTP {response.status_code}'))}
            except json.JSONDecodeError:
                return {'error': f'HTTP {response.status_code}'}
        try:
            return response.json()
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON response'}


def ensure_doctype(client, doctype_def, doctype_name):
    """Create custom doctype if it doesn't exist"""
    if client.doctype_exists(doctype_name):
        print(f'   {doctype_name} doctype already exists')
        return True

    print(f'   Creating {doctype_name} doctype...')
    response = client.create_doctype(doctype_def)

    if response.get('data', {}).get('name'):
        print(f'   {doctype_name} doctype created successfully')
        return True
    else:
        error = response.get('error', 'Unknown error')
        print(f'   ERROR: Failed to create {doctype_name} doctype: {error}')
        return False


def main():
    """Main function"""
    print('=' * 60)
    print('Create Container Pre-Allocation DocType')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        config['url'],
        config['username'],
        config['password']
    )

    print('\n2. Creating Container Pre-Allocation doctype...')
    success = ensure_doctype(client, PREALLOCATION_DOCTYPE, 'Container Pre-Allocation')

    print('\n' + '=' * 60)
    if success:
        print('SUCCESS: Container Pre-Allocation DocType is ready')
        print('=' * 60)
        print('\nDocType Details:')
        print(f'  Name: Container Pre-Allocation')
        print(f'  Naming: CPA-.##### (e.g., CPA-00001)')
        print(f'  Module: Stock')
        print('\nFields:')
        for field in PREALLOCATION_DOCTYPE['fields']:
            req = '*' if field.get('reqd') else ''
            print(f'  - {field["label"]}{req} ({field["fieldtype"]})')
        print('\nPermissions:')
        for perm in PREALLOCATION_DOCTYPE['permissions']:
            perms = []
            if perm.get('read'):
                perms.append('read')
            if perm.get('write'):
                perms.append('write')
            if perm.get('create'):
                perms.append('create')
            if perm.get('delete'):
                perms.append('delete')
            print(f'  - {perm["role"]}: {", ".join(perms)}')
        sys.exit(0)
    else:
        print('FAILED: Could not create Container Pre-Allocation DocType')
        print('=' * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
