#!/usr/bin/env python3
"""
SBS-64: Clear All Migrated Data
Removes all migrated data from ERPNext using SQL TRUNCATE for speed.

This script:
1. Uses direct SQL TRUNCATE for fast deletion
2. Handles child tables before parent tables
3. Deletes master data (Containers, Customers, Items)
4. Preserves system items and default warehouses

Environment Variables:
  MYSQL_ROOT_PASSWORD - MySQL root password (default: admin)

Usage:
  python scripts/clear_all_data.py
  python scripts/clear_all_data.py --dry-run  # Preview without executing
  python scripts/clear_all_data.py --container soundboxstore-erpnext-db-1 --database _77f5e42251d79843
"""

import os
import sys
import argparse
import subprocess

# SQL statements in dependency order
SQL_STATEMENTS = [
    # 1. System documents (must go first)
    "TRUNCATE TABLE `tabStock Ledger Entry`",
    "TRUNCATE TABLE `tabGL Entry`",
    "TRUNCATE TABLE `tabSerial and Batch Bundle`",

    # 2. Transaction child tables (delete before parent)
    "TRUNCATE TABLE `tabDelivery Note Item`",
    "TRUNCATE TABLE `tabSales Order Item`",
    "TRUNCATE TABLE `tabPurchase Receipt Item`",
    "TRUNCATE TABLE `tabPurchase Order Item`",
    "TRUNCATE TABLE `tabStock Entry Detail`",
    "TRUNCATE TABLE `tabSales Invoice Item`",
    "TRUNCATE TABLE `tabPurchase Invoice Item`",
    "TRUNCATE TABLE `tabStock Reconciliation Item`",

    # 3. Transaction parent tables
    "TRUNCATE TABLE `tabDelivery Note`",
    "TRUNCATE TABLE `tabSales Invoice`",
    "TRUNCATE TABLE `tabSales Order`",
    "TRUNCATE TABLE `tabPurchase Receipt`",
    "TRUNCATE TABLE `tabPurchase Invoice`",
    "TRUNCATE TABLE `tabPurchase Order`",
    "TRUNCATE TABLE `tabStock Entry`",
    "TRUNCATE TABLE `tabStock Reconciliation`",

    # 4. Custom DocTypes
    "TRUNCATE TABLE `tabContainer Pre-Allocation`",
    "TRUNCATE TABLE `tabContainer`",

    # 5. Master data (preserve some records)
    "TRUNCATE TABLE `tabDynamic Link`",
    "TRUNCATE TABLE `tabAddress`",
    "TRUNCATE TABLE `tabContact`",
    "DELETE FROM `tabCustomer` WHERE name != 'Guest'",
    "TRUNCATE TABLE `tabSupplier`",
    "DELETE FROM `tabItem` WHERE is_stock_item = 1",
    "DELETE FROM `tabItem Price` WHERE item_code IN (SELECT name FROM `tabItem` WHERE is_stock_item = 1)",
]

# Verification queries to count remaining records
VERIFY_QUERIES = [
    ("Stock Ledger Entry", "SELECT COUNT(*) FROM `tabStock Ledger Entry`"),
    ("GL Entry", "SELECT COUNT(*) FROM `tabGL Entry`"),
    ("Delivery Note", "SELECT COUNT(*) FROM `tabDelivery Note`"),
    ("Sales Invoice", "SELECT COUNT(*) FROM `tabSales Invoice`"),
    ("Sales Order", "SELECT COUNT(*) FROM `tabSales Order`"),
    ("Purchase Receipt", "SELECT COUNT(*) FROM `tabPurchase Receipt`"),
    ("Purchase Invoice", "SELECT COUNT(*) FROM `tabPurchase Invoice`"),
    ("Purchase Order", "SELECT COUNT(*) FROM `tabPurchase Order`"),
    ("Stock Entry", "SELECT COUNT(*) FROM `tabStock Entry`"),
    ("Container Pre-Allocation", "SELECT COUNT(*) FROM `tabContainer Pre-Allocation`"),
    ("Container", "SELECT COUNT(*) FROM `tabContainer`"),
    ("Customer", "SELECT COUNT(*) FROM `tabCustomer`"),
    ("Item", "SELECT COUNT(*) FROM `tabItem`"),
]


def execute_sql(container, database, password, sql, dry_run=False):
    """Execute SQL statement via docker exec"""
    if dry_run:
        print(f"  [DRY-RUN] Would execute: {sql}")
        return True, None

    cmd = [
        'docker', 'exec', '-i', container,
        'mysql', '-u', 'root', f'-p{password}',
        '-D', database,
        '-e', sql
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            return False, result.stderr

        return True, result.stdout
    except subprocess.TimeoutExpired:
        return False, "SQL execution timed out"
    except Exception as e:
        return False, str(e)


def verify_counts(container, database, password):
    """Verify deletion by counting remaining records"""
    print('\n' + '=' * 60)
    print('VERIFICATION - Counting Remaining Records')
    print('=' * 60)

    results = []
    for doctype, query in VERIFY_QUERIES:
        success, output = execute_sql(container, database, password, query, dry_run=False)
        if success:
            # Parse count from output
            lines = output.strip().split('\n')
            if len(lines) >= 2:
                count = lines[1].strip()
                results.append((doctype, count))
                print(f"  {doctype}: {count}")
        else:
            results.append((doctype, 'ERROR'))
            print(f"  {doctype}: ERROR - {output}")

    return results


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Clear all migrated data from ERPNext using SQL TRUNCATE'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview SQL statements without executing'
    )
    parser.add_argument(
        '--container',
        default='soundboxstore-erpnext-db-1',
        help='Docker container name (default: soundboxstore-erpnext-db-1)'
    )
    parser.add_argument(
        '--database',
        default='_77f5e42251d79843',
        help='Database name (default: _77f5e42251d79843)'
    )
    parser.add_argument(
        '--password',
        default=os.environ.get('MYSQL_ROOT_PASSWORD', 'erpnext-soundbox-db-2026'),
        help='MySQL root password (default: from MYSQL_ROOT_PASSWORD env)'
    )
    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: Clear All Migrated Data (SQL TRUNCATE)')
    if args.dry_run:
        print('MODE: DRY RUN (no changes will be made)')
    print('=' * 60)
    print(f'\nContainer: {args.container}')
    print(f'Database:  {args.database}')
    print(f'Password:  {"*" * len(args.password)}')

    # Disable foreign key checks
    print('\n1. Disabling foreign key checks...')
    success, output = execute_sql(
        args.container,
        args.database,
        args.password,
        'SET FOREIGN_KEY_CHECKS = 0',
        args.dry_run
    )
    if not success and not args.dry_run:
        print(f'ERROR: Failed to disable foreign key checks: {output}')
        sys.exit(1)

    # Execute SQL statements
    print('\n2. Executing TRUNCATE/DELETE statements...')
    total_statements = len(SQL_STATEMENTS)
    success_count = 0
    error_count = 0
    errors = []

    for i, sql in enumerate(SQL_STATEMENTS, 1):
        # Extract table name for display
        table_name = sql.split('`')[1] if '`' in sql else 'unknown'
        print(f'  [{i}/{total_statements}] {table_name}...')

        success, output = execute_sql(
            args.container,
            args.database,
            args.password,
            sql,
            args.dry_run
        )

        if success or args.dry_run:
            success_count += 1
        else:
            error_count += 1
            errors.append({'table': table_name, 'error': output})
            print(f'    ERROR: {output[:100]}')

    # Re-enable foreign key checks
    print('\n3. Re-enabling foreign key checks...')
    success, output = execute_sql(
        args.container,
        args.database,
        args.password,
        'SET FOREIGN_KEY_CHECKS = 1',
        args.dry_run
    )
    if not success and not args.dry_run:
        print(f'WARNING: Failed to re-enable foreign key checks: {output}')

    # Summary
    print('\n' + '=' * 60)
    print('CLEAR DATA COMPLETE')
    print('=' * 60)
    print(f'Total Statements: {total_statements}')
    print(f'Successful:       {success_count}')
    print(f'Errors:           {error_count}')

    if errors:
        print('\nErrors encountered:')
        for error in errors[:5]:  # Show first 5 errors
            print(f"  {error['table']}: {error['error'][:80]}")

    if args.dry_run:
        print('\nThis was a DRY RUN. No data was actually deleted.')
        print('Run without --dry-run to execute SQL statements.')
    else:
        # Verify deletion
        verify_counts(args.container, args.database, args.password)

    sys.exit(1 if error_count > 0 else 0)


if __name__ == '__main__':
    main()
