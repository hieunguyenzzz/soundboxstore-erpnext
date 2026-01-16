#!/usr/bin/env python3
"""
SBS-64: Custom Fields Setup Script
Creates custom fields for Item, Purchase Order, and Sales Order doctypes in ERPNext.

This script:
1. Connects to ERPNext via REST API
2. Creates custom fields on Item, Purchase Order, and Sales Order doctypes
3. Sets up Link fields to Container and Warehouse doctypes
4. Configures fetch fields for automatic ETA population
5. Adds custom_stock_status for order lifecycle tracking

Environment Variables:
  ERPNEXT_URL        - ERPNext server URL (required)
  ERPNEXT_API_KEY    - ERPNext API key (required)
  ERPNEXT_API_SECRET - ERPNext API secret (required)

Usage:
  python scripts/setup_custom_fields.py
"""

import os
import sys
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 30

# =============================================================================
# Custom Field Definitions
# =============================================================================

ITEM_FIELDS = [
    {
        "dt": "Item",
        "fieldname": "custom_sku",
        "fieldtype": "Data",
        "label": "SKU",
        "insert_after": "item_name",
        "reqd": 1,
        "unique": 1,
        "in_list_view": 1,
        "in_standard_filter": 1,
        "description": "Product SKU identifier"
    },
    {
        "dt": "Item",
        "fieldname": "custom_category",
        "fieldtype": "Select",
        "label": "Category",
        "insert_after": "custom_sku",
        "options": "\nBooth\nAcoustic Panel\nAcoustic Slat\nFurniture\nAccessory\nMoss\nSpare Glass\nSpare Packaging",
        "in_list_view": 1,
        "in_standard_filter": 1,
        "description": "Product category"
    },
    {
        "dt": "Item",
        "fieldname": "custom_unit_cbm",
        "fieldtype": "Float",
        "label": "Unit CBM",
        "insert_after": "custom_category",
        "precision": "4",
        "description": "Cubic meters per unit for shipping calculations"
    },
]

PURCHASE_ORDER_FIELDS = [
    # Section break for Container Info
    {
        "dt": "Purchase Order",
        "fieldname": "custom_container_section",
        "fieldtype": "Section Break",
        "label": "Container Information",
        "insert_after": "address_display",
        "collapsible": 1
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_container",
        "fieldtype": "Link",
        "label": "Container",
        "insert_after": "custom_container_section",
        "options": "Container",
        "description": "Link to Container for this purchase order"
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_container_eta",
        "fieldtype": "Date",
        "label": "Container ETA",
        "insert_after": "custom_container",
        "read_only": 1,
        "fetch_from": "custom_container.eta",
        "fetch_if_empty": 1,
        "description": "Expected arrival date (auto-fetched from Container)"
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_column_break_container",
        "fieldtype": "Column Break",
        "insert_after": "custom_container_eta"
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_cbm_allocated",
        "fieldtype": "Float",
        "label": "CBM Allocated",
        "insert_after": "custom_column_break_container",
        "precision": "4",
        "description": "Total CBM for this PO in container"
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_batch",
        "fieldtype": "Data",
        "label": "Batch",
        "insert_after": "custom_cbm_allocated",
        "description": "Batch/lot number for tracking"
    },
    {
        "dt": "Purchase Order",
        "fieldname": "custom_destination_warehouse",
        "fieldtype": "Link",
        "label": "Destination Warehouse",
        "insert_after": "custom_batch",
        "options": "Warehouse",
        "description": "Target warehouse on arrival"
    },
]

SALES_ORDER_FIELDS = [
    # Stock Status field - critical for order lifecycle tracking
    {
        "dt": "Sales Order",
        "fieldname": "custom_stock_status",
        "fieldtype": "Select",
        "label": "Stock Status",
        "insert_after": "status",
        "options": "\nFOR MANUFACTURE\nSTOCK COMING\nFOR DESPATCH\nDESPATCHED",
        "in_list_view": 1,
        "in_standard_filter": 1,
        "description": "Stock availability status for this order"
    },
    # Section break for Order Notes
    {
        "dt": "Sales Order",
        "fieldname": "custom_order_notes_section",
        "fieldtype": "Section Break",
        "label": "Order Notes & Allocation",
        "insert_after": "contact_display",
        "collapsible": 1
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_order_issue_notes",
        "fieldtype": "Small Text",
        "label": "Order Issue Notes",
        "insert_after": "custom_order_notes_section",
        "description": "Notes about order problems or issues"
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_column_break_allocation",
        "fieldtype": "Column Break",
        "insert_after": "custom_order_issue_notes"
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_allocated_container",
        "fieldtype": "Link",
        "label": "Allocated Container",
        "insert_after": "custom_column_break_allocation",
        "options": "Container",
        "description": "Container allocated for this order"
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_container_eta",
        "fieldtype": "Date",
        "label": "Container ETA",
        "insert_after": "custom_allocated_container",
        "read_only": 1,
        "fetch_from": "custom_allocated_container.eta",
        "fetch_if_empty": 1,
        "description": "Expected arrival date (auto-fetched from Container)"
    },
    # Section break for Warehouse Notes
    {
        "dt": "Sales Order",
        "fieldname": "custom_warehouse_section",
        "fieldtype": "Section Break",
        "label": "Warehouse Information",
        "insert_after": "custom_container_eta",
        "collapsible": 1
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_booked_to_warehouse",
        "fieldtype": "Date",
        "label": "Booked to Warehouse",
        "insert_after": "custom_warehouse_section",
        "description": "Date stock was received at warehouse"
    },
    {
        "dt": "Sales Order",
        "fieldname": "custom_warehouse_notes",
        "fieldtype": "Small Text",
        "label": "Warehouse Notes",
        "insert_after": "custom_booked_to_warehouse",
        "description": "D&I Notes for warehouse team"
    },
    # Delivery tracking
    {
        "dt": "Sales Order",
        "fieldname": "custom_date_delivered",
        "fieldtype": "Date",
        "label": "Date Delivered",
        "insert_after": "custom_warehouse_notes",
        "description": "Actual delivery date to customer"
    },
]


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
        print("\nRequired environment variables:")
        print("  ERPNEXT_URL        - ERPNext server URL (e.g., https://erp.soundboxstore.com)")
        print("  ERPNEXT_API_KEY    - ERPNext API key")
        print("  ERPNEXT_API_SECRET - ERPNext API secret")
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

    def custom_field_exists(self, dt, fieldname):
        """Check if a custom field exists"""
        # Custom Field name format is "DocType-fieldname"
        name = f"{dt}-{fieldname}"
        response = self.session.get(
            f'{self.url}/api/resource/Custom Field/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    def create_custom_field(self, field_def):
        """Create a custom field"""
        response = self.session.post(
            f'{self.url}/api/resource/Custom Field',
            json=field_def,
            headers=self.headers,
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

    def get_custom_field(self, dt, fieldname):
        """Get a custom field"""
        name = f"{dt}-{fieldname}"
        response = self.session.get(
            f'{self.url}/api/resource/Custom Field/{name}',
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            try:
                return response.json().get('data')
            except json.JSONDecodeError:
                return None
        return None

    def update_custom_field(self, dt, fieldname, updates):
        """Update an existing custom field"""
        name = f"{dt}-{fieldname}"
        response = self.session.put(
            f'{self.url}/api/resource/Custom Field/{name}',
            json=updates,
            headers=self.headers,
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


def setup_fields(client, fields, doctype_name):
    """Create custom fields for a doctype"""
    results = {'created': 0, 'skipped': 0, 'failed': 0, 'errors': []}

    print(f"\n  Setting up {doctype_name} fields...")

    for field in fields:
        fieldname = field['fieldname']
        label = field.get('label', fieldname)

        if client.custom_field_exists(field['dt'], fieldname):
            print(f"    [SKIP] {label} ({fieldname}) - already exists")
            results['skipped'] += 1
            continue

        response = client.create_custom_field(field)

        if response.get('data', {}).get('name'):
            print(f"    [OK]   {label} ({fieldname}) - created")
            results['created'] += 1
        else:
            error = response.get('error', 'Unknown error')
            # Truncate error for display
            error_short = str(error)[:80]
            print(f"    [FAIL] {label} ({fieldname}) - {error_short}")
            results['failed'] += 1
            results['errors'].append({
                'field': fieldname,
                'doctype': field['dt'],
                'error': str(error)
            })

    return results


def main():
    """Main function"""
    print('=' * 60)
    print('SBS-53: Custom Fields Setup')
    print('=' * 60)

    config = get_config()

    print('\n1. Connecting to ERPNext...')
    client = ERPNextClient(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    print('\n2. Creating custom fields...')

    all_results = {
        'created': 0,
        'skipped': 0,
        'failed': 0,
        'errors': []
    }

    # Setup Item fields
    results = setup_fields(client, ITEM_FIELDS, "Item")
    all_results['created'] += results['created']
    all_results['skipped'] += results['skipped']
    all_results['failed'] += results['failed']
    all_results['errors'].extend(results['errors'])

    # Setup Purchase Order fields
    results = setup_fields(client, PURCHASE_ORDER_FIELDS, "Purchase Order")
    all_results['created'] += results['created']
    all_results['skipped'] += results['skipped']
    all_results['failed'] += results['failed']
    all_results['errors'].extend(results['errors'])

    # Setup Sales Order fields
    results = setup_fields(client, SALES_ORDER_FIELDS, "Sales Order")
    all_results['created'] += results['created']
    all_results['skipped'] += results['skipped']
    all_results['failed'] += results['failed']
    all_results['errors'].extend(results['errors'])

    # Summary
    print('\n' + '=' * 60)
    print('CUSTOM FIELDS SETUP COMPLETE')
    print('=' * 60)
    print(f'Created: {all_results["created"]}')
    print(f'Skipped: {all_results["skipped"]} (already exist)')
    print(f'Failed:  {all_results["failed"]}')

    if all_results['errors']:
        print(f'\nErrors:')
        for err in all_results['errors']:
            print(f'  - {err["doctype"]}.{err["field"]}: {err["error"][:60]}')

    total_expected = len(ITEM_FIELDS) + len(PURCHASE_ORDER_FIELDS) + len(SALES_ORDER_FIELDS)
    print(f'\nTotal fields: {total_expected}')
    print(f'  Item: {len(ITEM_FIELDS)}')
    print(f'  Purchase Order: {len(PURCHASE_ORDER_FIELDS)}')
    print(f'  Sales Order: {len(SALES_ORDER_FIELDS)}')

    sys.exit(1 if all_results['failed'] > 0 else 0)


if __name__ == '__main__':
    main()
