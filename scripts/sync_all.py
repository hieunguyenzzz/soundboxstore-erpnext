#!/usr/bin/env python3
"""
Master Sync Script
Runs all data migrations in the correct order.

This script orchestrates:
1. migrate_master_data.py - Products/Items (SBS-51)
2. migrate_customers.py - Customers (SBS-51)
3. migrate_containers.py - Shipping Containers (SBS-51)
4. migrate_inventory.py - Opening Stock (SBS-52)
5. process_container_arrivals.py - Container Arrivals (SBS-59)
6. migrate_purchase_orders.py - Purchase Orders (SBS-61)
7. migrate_sales_orders.py - Sales Orders (SBS-61)
8. migrate_stock_reservations.py - Stock Reservations (SBS-61)
9. stock_reconciliation_report.py - Stock Reconciliation (SBS-61)

Environment Variables (same as individual scripts):
  ERPNEXT_URL          - ERPNext server URL (required)
  ERPNEXT_USERNAME     - ERPNext username (default: Administrator)
  ERPNEXT_PASSWORD     - ERPNext password (required)
  GOOGLE_SHEETS_CREDS  - Path to service account JSON OR the JSON content itself
  SPREADSHEET_ID       - Google Sheets spreadsheet ID (optional, has default)
  TELEGRAM_BOT_TOKEN   - Telegram bot token for notifications (optional)
  TELEGRAM_CHAT_ID     - Telegram chat ID for notifications (optional)

Usage:
  python scripts/sync_all.py
"""

import sys
import os

# Add scripts directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    """Run all migrations in order"""
    print('=' * 70)
    print('FULL DATA SYNC')
    print('=' * 70)
    print('\nThis will run all migrations in order:')
    print('  1. Products/Items (migrate_master_data.py)')
    print('  2. Customers (migrate_customers.py)')
    print('  3. Containers (migrate_containers.py)')
    print('  4. Inventory (migrate_inventory.py)')
    print('  5. Container Arrivals (process_container_arrivals.py)')
    print('  6. Purchase Orders (migrate_purchase_orders.py)')
    print('  7. Sales Orders (migrate_sales_orders.py)')
    print('  8. Stock Reservations (migrate_stock_reservations.py)')
    print('  9. Stock Reconciliation Report (stock_reconciliation_report.py)')
    print('')

    results = {
        'products': None,
        'customers': None,
        'containers': None,
        'inventory': None,
        'arrivals': None,
        'purchase_orders': None,
        'sales_orders': None,
        'stock_reservations': None,
        'reconciliation': None
    }

    # 1. Products/Items
    print('\n' + '=' * 70)
    print('PHASE 1: PRODUCTS/ITEMS')
    print('=' * 70)
    try:
        from migrate_master_data import main as sync_products
        sync_products()
        results['products'] = 'success'
    except SystemExit as e:
        results['products'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in products migration: {e}')
        results['products'] = 'failed'

    # 2. Customers
    print('\n' + '=' * 70)
    print('PHASE 2: CUSTOMERS')
    print('=' * 70)
    try:
        from migrate_customers import main as sync_customers
        sync_customers()
        results['customers'] = 'success'
    except SystemExit as e:
        results['customers'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in customers migration: {e}')
        results['customers'] = 'failed'

    # 3. Containers
    print('\n' + '=' * 70)
    print('PHASE 3: CONTAINERS')
    print('=' * 70)
    try:
        from migrate_containers import main as sync_containers
        sync_containers()
        results['containers'] = 'success'
    except SystemExit as e:
        results['containers'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in containers migration: {e}')
        results['containers'] = 'failed'

    # 4. Inventory (Opening Stock)
    print('\n' + '=' * 70)
    print('PHASE 4: INVENTORY (OPENING STOCK)')
    print('=' * 70)
    try:
        from migrate_inventory import main as sync_inventory
        sync_inventory()
        results['inventory'] = 'success'
    except SystemExit as e:
        results['inventory'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in inventory migration: {e}')
        results['inventory'] = 'failed'

    # 5. Container Arrivals
    print('\n' + '=' * 70)
    print('PHASE 5: CONTAINER ARRIVALS')
    print('=' * 70)
    try:
        from process_container_arrivals import main as process_arrivals
        process_arrivals()
        results['arrivals'] = 'success'
    except SystemExit as e:
        results['arrivals'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in container arrivals: {e}')
        results['arrivals'] = 'failed'

    # 6. Purchase Orders (SBS-61)
    print('\n' + '=' * 70)
    print('PHASE 6: PURCHASE ORDERS')
    print('=' * 70)
    try:
        from migrate_purchase_orders import main as sync_purchase_orders
        sync_purchase_orders()
        results['purchase_orders'] = 'success'
    except SystemExit as e:
        results['purchase_orders'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in purchase orders migration: {e}')
        results['purchase_orders'] = 'failed'

    # 7. Sales Orders (SBS-61)
    print('\n' + '=' * 70)
    print('PHASE 7: SALES ORDERS')
    print('=' * 70)
    try:
        from migrate_sales_orders import main as sync_sales_orders
        sync_sales_orders()
        results['sales_orders'] = 'success'
    except SystemExit as e:
        results['sales_orders'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in sales orders migration: {e}')
        results['sales_orders'] = 'failed'

    # 8. Stock Reservations (SBS-61)
    print('\n' + '=' * 70)
    print('PHASE 8: STOCK RESERVATIONS')
    print('=' * 70)
    try:
        from migrate_stock_reservations import main as sync_stock_reservations
        sync_stock_reservations()
        results['stock_reservations'] = 'success'
    except SystemExit as e:
        results['stock_reservations'] = 'failed' if e.code != 0 else 'success'
    except Exception as e:
        print(f'ERROR in stock reservations migration: {e}')
        results['stock_reservations'] = 'failed'

    # 9. Stock Reconciliation Report (SBS-61) - Non-blocking
    print('\n' + '=' * 70)
    print('PHASE 9: STOCK RECONCILIATION REPORT')
    print('=' * 70)
    try:
        from stock_reconciliation_report import main as run_reconciliation
        run_reconciliation()
        results['reconciliation'] = 'success'
    except SystemExit as e:
        # Reconciliation report is informational, always treat as success
        results['reconciliation'] = 'success'
    except Exception as e:
        print(f'ERROR in stock reconciliation: {e}')
        results['reconciliation'] = 'failed'

    # Summary
    print('\n' + '=' * 70)
    print('FULL SYNC SUMMARY')
    print('=' * 70)
    for phase, status in results.items():
        icon = '✓' if status == 'success' else '✗'
        print(f'  {icon} {phase.capitalize()}: {status}')

    # Exit with error if any phase failed
    failed = [k for k, v in results.items() if v == 'failed']
    if failed:
        print(f'\nWARNING: {len(failed)} phase(s) had failures')
        sys.exit(1)
    else:
        print('\nAll phases completed successfully!')
        sys.exit(0)


if __name__ == '__main__':
    main()
