#!/usr/bin/env python3
"""
SBS-64: Unified Migration Entry Point

Single entry point for all ERPNext migrations from Google Sheets.

Usage:
  python scripts/migrate.py           # Run all migrations in order
  python scripts/migrate.py --dry-run # Preview without changes

Migration Steps (in order):
  1. master_data     - Products/Items from Masterfile sheet
  2. customers       - Customers, Addresses, Contacts
  3. containers      - Container tracking
  4. inventory       - Opening stock quantities
  5. sales_orders    - Sales Orders + Delivery Notes
  6. purchase_orders - Purchase Orders
  7. allocations     - Container Pre-Allocations

Environment Variables:
  ERPNEXT_URL          - ERPNext server URL
  ERPNEXT_API_KEY      - ERPNext API key
  ERPNEXT_API_SECRET   - ERPNext API secret
  GOOGLE_SHEETS_CREDS  - Path to service account JSON
"""

import sys
import argparse
import importlib
from datetime import datetime

# Migration steps in dependency order
MIGRATION_STEPS = [
    ('master_data', 'migrate_master_data', 'Products/Items'),
    ('customers', 'migrate_customers', 'Customers, Addresses, Contacts'),
    ('containers', 'migrate_containers', 'Container tracking'),
    ('inventory', 'migrate_inventory', 'Opening stock'),
    ('sales_orders', 'migrate_sales_orders', 'Sales Orders + Delivery Notes'),
    ('purchase_orders', 'migrate_purchase_orders', 'Purchase Orders'),
    ('allocations', 'migrate_allocations', 'Container Pre-Allocations'),
]


def run_step(name, module_name, description, dry_run=False):
    """Run a single migration step"""
    print(f"\n{'=' * 60}")
    print(f"STEP: {name} - {description}")
    print('=' * 60)

    if dry_run:
        print(f"[DRY-RUN] Would run: python scripts/{module_name}.py")
        return True

    try:
        module = importlib.import_module(module_name)

        if not hasattr(module, 'main'):
            print(f"ERROR: Module {module_name} has no main() function")
            return False

        start_time = datetime.now()
        print(f"Started at: {start_time.strftime('%H:%M:%S')}")

        try:
            module.main()
            success = True
        except SystemExit as e:
            success = (e.code == 0 or e.code is None)

        duration = (datetime.now() - start_time).total_seconds()
        print(f"Completed in: {duration:.1f}s")
        return success

    except ImportError as e:
        print(f"ERROR: Could not import {module_name}: {e}")
        return False
    except Exception as e:
        print(f"ERROR: {name} failed: {e}")
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Unified ERPNext migration from Google Sheets'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview migrations without running them'
    )
    args = parser.parse_args()

    print('=' * 60)
    print('SBS-64: ERPNext Migration')
    print('=' * 60)
    print(f'\nRunning {len(MIGRATION_STEPS)} migration steps')

    if args.dry_run:
        print('[DRY-RUN MODE] No changes will be made')

    start_time = datetime.now()
    results = []

    for name, module_name, description in MIGRATION_STEPS:
        success = run_step(name, module_name, description, args.dry_run)
        results.append((name, success))

        if not success and not args.dry_run:
            print(f"\nMigration stopped at: {name}")
            break

    # Summary
    duration = (datetime.now() - start_time).total_seconds()

    print('\n' + '=' * 60)
    print('MIGRATION SUMMARY')
    print('=' * 60)

    for name, success in results:
        status = 'OK' if success else 'FAILED'
        print(f"  {name:20} [{status}]")

    successful = sum(1 for _, s in results if s)
    failed = sum(1 for _, s in results if not s)

    print(f'\nTotal: {successful} succeeded, {failed} failed')
    print(f'Duration: {duration:.1f}s')

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
