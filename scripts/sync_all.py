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
    print('')

    results = {
        'products': None,
        'customers': None,
        'containers': None,
        'inventory': None,
        'arrivals': None
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
