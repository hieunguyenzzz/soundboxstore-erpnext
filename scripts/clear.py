#!/usr/bin/env python3
"""
SBS-64: Clear ERPNext Data

Removes migrated data from ERPNext using SQL TRUNCATE for speed.

Usage:
  python scripts/clear_data.py                     # Clear transactions only
  python scripts/clear_data.py --scope all         # Clear everything including master data
  python scripts/clear_data.py --scope transactions # Default: keep master data
  python scripts/clear_data.py --dry-run           # Preview without executing
"""

import os
import sys
import argparse
import subprocess

# SQL statements for transaction data (default scope)
SQL_TRANSACTIONS = [
    # 1. System documents (must go first)
    "TRUNCATE TABLE `tabStock Ledger Entry`",
    "TRUNCATE TABLE `tabGL Entry`",
    "TRUNCATE TABLE `tabSerial and Batch Bundle`",

    # 2. Transaction child tables
    "TRUNCATE TABLE `tabDelivery Note Item`",
    "TRUNCATE TABLE `tabSales Order Item`",
    "TRUNCATE TABLE `tabPurchase Receipt Item`",
    "TRUNCATE TABLE `tabPurchase Order Item`",
    "TRUNCATE TABLE `tabStock Entry Detail`",
    "TRUNCATE TABLE `tabSales Invoice Item`",
    "TRUNCATE TABLE `tabPurchase Invoice Item`",
    "TRUNCATE TABLE `tabStock Reconciliation Item`",
    "TRUNCATE TABLE `tabPayment Entry Reference`",

    # 3. Transaction parent tables
    "TRUNCATE TABLE `tabDelivery Note`",
    "TRUNCATE TABLE `tabSales Invoice`",
    "TRUNCATE TABLE `tabSales Order`",
    "TRUNCATE TABLE `tabPurchase Receipt`",
    "TRUNCATE TABLE `tabPurchase Invoice`",
    "TRUNCATE TABLE `tabPurchase Order`",
    "TRUNCATE TABLE `tabStock Entry`",
    "TRUNCATE TABLE `tabStock Reconciliation`",
    "TRUNCATE TABLE `tabPayment Entry`",

    # 4. Custom DocTypes
    "TRUNCATE TABLE `tabContainer Pre-Allocation`",
    "TRUNCATE TABLE `tabContainer`",
]

# Additional SQL for master data (scope=all)
SQL_MASTER_DATA = [
    "TRUNCATE TABLE `tabDynamic Link`",
    "TRUNCATE TABLE `tabAddress`",
    "TRUNCATE TABLE `tabContact`",
    "DELETE FROM `tabCustomer` WHERE name != 'Guest'",
    "TRUNCATE TABLE `tabSupplier`",
    "DELETE FROM `tabItem` WHERE is_stock_item = 1",
    "DELETE FROM `tabItem Price` WHERE item_code IN (SELECT name FROM `tabItem` WHERE is_stock_item = 1)",
]

# Verification queries
VERIFY_QUERIES = [
    ("Stock Ledger Entry", "SELECT COUNT(*) FROM `tabStock Ledger Entry`"),
    ("GL Entry", "SELECT COUNT(*) FROM `tabGL Entry`"),
    ("Delivery Note", "SELECT COUNT(*) FROM `tabDelivery Note`"),
    ("Sales Order", "SELECT COUNT(*) FROM `tabSales Order`"),
    ("Purchase Order", "SELECT COUNT(*) FROM `tabPurchase Order`"),
    ("Stock Entry", "SELECT COUNT(*) FROM `tabStock Entry`"),
    ("Container", "SELECT COUNT(*) FROM `tabContainer`"),
    ("Customer", "SELECT COUNT(*) FROM `tabCustomer`"),
    ("Item", "SELECT COUNT(*) FROM `tabItem`"),
]


def execute_sql(container, database, password, sql, dry_run=False):
    """Execute SQL statement via docker exec"""
    if dry_run:
        print(f"  [DRY-RUN] {sql[:60]}...")
        return True, None

    cmd = [
        'docker', 'exec', '-i', container,
        'mysql', '-u', 'root', f'-p{password}',
        '-D', database, '-e', sql
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return False, result.stderr
        return True, result.stdout
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)


def verify_counts(container, database, password):
    """Verify deletion by counting remaining records"""
    print('\n' + '=' * 60)
    print('VERIFICATION')
    print('=' * 60)

    for doctype, query in VERIFY_QUERIES:
        success, output = execute_sql(container, database, password, query)
        if success:
            lines = output.strip().split('\n')
            count = lines[1].strip() if len(lines) >= 2 else '?'
            print(f"  {doctype:25} {count}")
        else:
            print(f"  {doctype:25} ERROR")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Clear ERPNext data')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--scope', choices=['transactions', 'all'], default='transactions',
                        help='transactions=keep master data (default), all=delete everything')
    parser.add_argument('--container', default='soundboxstore-erpnext-db-1')
    parser.add_argument('--database', default='_77f5e42251d79843')
    parser.add_argument('--password', default=os.environ.get('MYSQL_ROOT_PASSWORD', 'erpnext-soundbox-db-2026'))

    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: Clear ERPNext Data')
    print('=' * 60)
    print(f'Scope: {args.scope}')
    print(f'Container: {args.container}')
    if args.dry_run:
        print('MODE: DRY RUN')

    # Build SQL list
    sql_list = SQL_TRANSACTIONS.copy()
    if args.scope == 'all':
        sql_list.extend(SQL_MASTER_DATA)

    # Disable FK checks
    execute_sql(args.container, args.database, args.password, 'SET FOREIGN_KEY_CHECKS = 0', args.dry_run)

    # Execute statements
    print(f'\nExecuting {len(sql_list)} statements...')
    errors = 0
    for i, sql in enumerate(sql_list, 1):
        table = sql.split('`')[1] if '`' in sql else 'unknown'
        success, err = execute_sql(args.container, args.database, args.password, sql, args.dry_run)
        if not success and not args.dry_run:
            print(f'  [{i}] {table}: ERROR')
            errors += 1
        elif not args.dry_run:
            print(f'  [{i}] {table}: OK')

    # Re-enable FK checks
    execute_sql(args.container, args.database, args.password, 'SET FOREIGN_KEY_CHECKS = 1', args.dry_run)

    # Summary
    print('\n' + '=' * 60)
    print(f'Completed: {len(sql_list) - errors} OK, {errors} errors')

    if not args.dry_run:
        verify_counts(args.container, args.database, args.password)

    return 1 if errors > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
