#!/usr/bin/env python3
"""
SBS-64: Migration Validation Script
Validates the ERPNext migration by checking data quality and completeness.

This script checks:
1. Items - count, stock levels, categories
2. Customers - count, addresses, contacts
3. Containers - count, status distribution
4. Sales Orders - count, status, stock_status
5. Purchase Orders - count
6. Inventory - Stock Entries, total stock value

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_API_KEY      - ERPNext API key (required)
  ERPNEXT_API_SECRET   - ERPNext API secret (required)

Usage:
  python scripts/validate_migration.py
"""

import os
import json
import sys
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


def create_session():
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


class ERPNextValidator:
    """ERPNext validation client"""

    def __init__(self, url, api_key, api_secret):
        self.url = url.rstrip('/')
        self.session = create_session()
        self.headers = {
            'Authorization': f'token {api_key}:{api_secret}',
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

    def count_doctype(self, doctype, filters=None):
        """Count documents of a doctype"""
        params = {'limit_page_length': 0}
        if filters:
            params['filters'] = json.dumps(filters)

        response = self.session.get(
            f'{self.url}/api/resource/{doctype}',
            params=params,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return len(response.json().get('data', []))
        return 0

    def get_documents(self, doctype, filters=None, fields=None, limit=1000):
        """Get documents of a doctype"""
        params = {'limit_page_length': limit}
        if filters:
            params['filters'] = json.dumps(filters)
        if fields:
            params['fields'] = json.dumps(fields)

        response = self.session.get(
            f'{self.url}/api/resource/{doctype}',
            params=params,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('data', [])
        return []

    def run_report(self, report_name, filters=None):
        """Run a report"""
        params = {'report_name': report_name}
        if filters:
            params['filters'] = json.dumps(filters)

        response = self.session.get(
            f'{self.url}/api/method/frappe.desk.query_report.run',
            params=params,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json().get('message', {})
        return {}


def validate_items(client):
    """Validate Items"""
    print('\n' + '=' * 50)
    print('ITEMS')
    print('=' * 50)

    # Total items
    total_items = client.count_doctype('Item', [['is_stock_item', '=', 1]])
    print(f'Total Stock Items: {total_items}')

    # Items by category
    items = client.get_documents('Item',
        filters=[['is_stock_item', '=', 1]],
        fields=['name', 'item_group', 'custom_category', 'valuation_rate'],
        limit=5000
    )

    # Category distribution
    by_group = {}
    by_custom_cat = {}
    zero_valuation = 0
    has_custom_category = 0

    for item in items:
        group = item.get('item_group', 'Unknown')
        by_group[group] = by_group.get(group, 0) + 1

        custom_cat = item.get('custom_category', '')
        if custom_cat:
            has_custom_category += 1
            by_custom_cat[custom_cat] = by_custom_cat.get(custom_cat, 0) + 1

        if not item.get('valuation_rate') or item.get('valuation_rate') == 0:
            zero_valuation += 1

    print(f'Items with custom_category set: {has_custom_category} ({100*has_custom_category/max(total_items,1):.1f}%)')
    print(f'Items with zero valuation: {zero_valuation} ({100*zero_valuation/max(total_items,1):.1f}%)')

    print('\nBy Item Group:')
    for group, count in sorted(by_group.items(), key=lambda x: -x[1]):
        print(f'  {group}: {count}')

    if by_custom_cat:
        print('\nBy Custom Category:')
        for cat, count in sorted(by_custom_cat.items(), key=lambda x: -x[1]):
            print(f'  {cat}: {count}')

    return {
        'total': total_items,
        'with_custom_category': has_custom_category,
        'zero_valuation': zero_valuation,
        'by_group': by_group,
        'by_custom_category': by_custom_cat
    }


def validate_customers(client):
    """Validate Customers"""
    print('\n' + '=' * 50)
    print('CUSTOMERS')
    print('=' * 50)

    total_customers = client.count_doctype('Customer')
    print(f'Total Customers: {total_customers}')

    # Count addresses
    addresses = client.get_documents('Address',
        fields=['name'],
        limit=5000
    )
    print(f'Total Addresses: {len(addresses)}')

    # Count contacts
    contacts = client.get_documents('Contact',
        fields=['name'],
        limit=5000
    )
    print(f'Total Contacts: {len(contacts)}')

    # Customer types
    customers = client.get_documents('Customer',
        fields=['name', 'customer_type'],
        limit=5000
    )
    by_type = {}
    for cust in customers:
        ctype = cust.get('customer_type', 'Unknown')
        by_type[ctype] = by_type.get(ctype, 0) + 1

    print('\nBy Customer Type:')
    for ctype, count in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f'  {ctype}: {count}')

    return {
        'total': total_customers,
        'addresses': len(addresses),
        'contacts': len(contacts),
        'by_type': by_type
    }


def validate_containers(client):
    """Validate Containers"""
    print('\n' + '=' * 50)
    print('CONTAINERS')
    print('=' * 50)

    containers = client.get_documents('Container',
        fields=['name', 'status', 'etd', 'eta'],
        limit=500
    )

    total = len(containers)
    print(f'Total Containers: {total}')

    by_status = {}
    for cont in containers:
        status = cont.get('status', 'Unknown')
        by_status[status] = by_status.get(status, 0) + 1

    print('\nBy Status:')
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        pct = 100 * count / max(total, 1)
        print(f'  {status}: {count} ({pct:.1f}%)')

    return {
        'total': total,
        'by_status': by_status
    }


def validate_sales_orders(client):
    """Validate Sales Orders"""
    print('\n' + '=' * 50)
    print('SALES ORDERS')
    print('=' * 50)

    sales_orders = client.get_documents('Sales Order',
        fields=['name', 'status', 'custom_stock_status', 'docstatus'],
        limit=5000
    )

    total = len(sales_orders)
    print(f'Total Sales Orders: {total}')

    by_status = {}
    by_stock_status = {}
    by_docstatus = {0: 0, 1: 0, 2: 0}

    for so in sales_orders:
        status = so.get('status', 'Unknown')
        by_status[status] = by_status.get(status, 0) + 1

        stock_status = so.get('custom_stock_status', 'Not Set')
        by_stock_status[stock_status] = by_stock_status.get(stock_status, 0) + 1

        docstatus = so.get('docstatus', 0)
        by_docstatus[docstatus] = by_docstatus.get(docstatus, 0) + 1

    print(f'\nBy Docstatus:')
    docstatus_labels = {0: 'Draft', 1: 'Submitted', 2: 'Cancelled'}
    for ds, count in by_docstatus.items():
        print(f'  {docstatus_labels.get(ds, ds)}: {count}')

    print('\nBy Status:')
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f'  {status}: {count}')

    print('\nBy custom_stock_status:')
    for status, count in sorted(by_stock_status.items(), key=lambda x: -x[1]):
        pct = 100 * count / max(total, 1)
        print(f'  {status}: {count} ({pct:.1f}%)')

    return {
        'total': total,
        'by_status': by_status,
        'by_stock_status': by_stock_status,
        'by_docstatus': by_docstatus
    }


def validate_purchase_orders(client):
    """Validate Purchase Orders"""
    print('\n' + '=' * 50)
    print('PURCHASE ORDERS')
    print('=' * 50)

    pos = client.get_documents('Purchase Order',
        fields=['name', 'status', 'docstatus', 'custom_container'],
        limit=500
    )

    total = len(pos)
    print(f'Total Purchase Orders: {total}')

    by_status = {}
    with_container = 0

    for po in pos:
        status = po.get('status', 'Unknown')
        by_status[status] = by_status.get(status, 0) + 1

        if po.get('custom_container'):
            with_container += 1

    print(f'POs linked to Containers: {with_container}')

    print('\nBy Status:')
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f'  {status}: {count}')

    return {
        'total': total,
        'with_container': with_container,
        'by_status': by_status
    }


def validate_stock(client):
    """Validate Stock Entries and Levels"""
    print('\n' + '=' * 50)
    print('STOCK / INVENTORY')
    print('=' * 50)

    # Stock Entries
    stock_entries = client.get_documents('Stock Entry',
        fields=['name', 'stock_entry_type', 'docstatus'],
        limit=500
    )

    total_se = len(stock_entries)
    submitted_se = sum(1 for se in stock_entries if se.get('docstatus') == 1)
    print(f'Total Stock Entries: {total_se}')
    print(f'Submitted Stock Entries: {submitted_se}')

    # Delivery Notes
    dns = client.get_documents('Delivery Note',
        fields=['name', 'docstatus'],
        limit=5000
    )
    total_dn = len(dns)
    submitted_dn = sum(1 for dn in dns if dn.get('docstatus') == 1)
    print(f'Total Delivery Notes: {total_dn}')
    print(f'Submitted Delivery Notes: {submitted_dn}')

    # Items with stock
    items_with_stock = client.get_documents('Bin',
        fields=['item_code', 'warehouse', 'actual_qty'],
        limit=5000
    )

    total_stock_qty = sum(b.get('actual_qty', 0) for b in items_with_stock)
    items_in_stock = len(set(b.get('item_code') for b in items_with_stock if b.get('actual_qty', 0) > 0))

    print(f'\nItems with stock (in Bin): {items_in_stock}')
    print(f'Total stock quantity: {total_stock_qty:,.0f}')

    # Warehouses with stock
    by_warehouse = {}
    for bin_data in items_with_stock:
        wh = bin_data.get('warehouse', 'Unknown')
        qty = bin_data.get('actual_qty', 0)
        if qty > 0:
            by_warehouse[wh] = by_warehouse.get(wh, 0) + qty

    if by_warehouse:
        print('\nStock by Warehouse:')
        for wh, qty in sorted(by_warehouse.items(), key=lambda x: -x[1])[:10]:
            print(f'  {wh}: {qty:,.0f}')

    return {
        'stock_entries': total_se,
        'stock_entries_submitted': submitted_se,
        'delivery_notes': total_dn,
        'delivery_notes_submitted': submitted_dn,
        'items_with_stock': items_in_stock,
        'total_stock_qty': total_stock_qty,
        'by_warehouse': by_warehouse
    }


def main():
    """Main validation function"""
    print('=' * 60)
    print('SBS-64: Migration Validation')
    print('=' * 60)

    config = get_config()

    print('\nConnecting to ERPNext...')
    client = ERPNextValidator(
        config['url'],
        config['api_key'],
        config['api_secret']
    )

    results = {}

    results['items'] = validate_items(client)
    results['customers'] = validate_customers(client)
    results['containers'] = validate_containers(client)
    results['sales_orders'] = validate_sales_orders(client)
    results['purchase_orders'] = validate_purchase_orders(client)
    results['stock'] = validate_stock(client)

    # Summary
    print('\n' + '=' * 60)
    print('VALIDATION SUMMARY')
    print('=' * 60)

    issues = []

    # Check for issues
    if results['items']['with_custom_category'] < results['items']['total'] * 0.5:
        issues.append(f"Less than 50% of items have custom_category set")

    if results['sales_orders']['by_stock_status'].get('Not Set', 0) > 0:
        issues.append(f"{results['sales_orders']['by_stock_status'].get('Not Set', 0)} Sales Orders without stock_status")

    if results['containers']['by_status'].get('In Transit', 0) == results['containers']['total']:
        issues.append("All containers are 'In Transit' - status logic may not be working")

    if results['stock']['items_with_stock'] == 0:
        issues.append("No items have stock - inventory migration may have failed")

    if issues:
        print('\nISSUES FOUND:')
        for issue in issues:
            print(f'  ⚠ {issue}')
    else:
        print('\n✓ No major issues detected')

    print(f'\nItems: {results["items"]["total"]}')
    print(f'Customers: {results["customers"]["total"]}')
    print(f'Containers: {results["containers"]["total"]}')
    print(f'Sales Orders: {results["sales_orders"]["total"]}')
    print(f'Purchase Orders: {results["purchase_orders"]["total"]}')
    print(f'Items with Stock: {results["stock"]["items_with_stock"]}')

    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    import tempfile
    report_path = os.path.join(tempfile.gettempdir(), f'validation_report_{timestamp}.json')
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'issues': issues,
            'results': results
        }, f, indent=2, default=str)
    print(f'\nDetailed report saved to: {report_path}')

    sys.exit(1 if issues else 0)


if __name__ == '__main__':
    main()
