# CLAUDE.md - Soundbox ERPNext

ERPNext v15 deployment for Soundbox inventory management.

## Required Skills

| Skill | When to Load | Purpose |
|-------|--------------|---------|
| `soundboxstore-erpnext` | **Always** | Server access, credentials, API, bench commands, migrations, troubleshooting |
| `soundboxstore-inventory` | When accessing Google Sheets | Original spreadsheet data, column mappings, sheet structure |

**The `soundboxstore-erpnext` skill contains all operational details:**
- Production server access and credentials
- Dokploy deployment commands
- ERPNext API reference
- Migration scripts guide
- Post-migration validation
- Troubleshooting guides

## Quick Reference

| Property | Value |
|----------|-------|
| **Production URL** | https://erp.soundboxstore.com |
| **Server** | 139.99.9.132 (OVH) |
| **Deployment** | Dokploy |
| **GitHub** | https://github.com/hieunguyenzzz/soundboxstore-erpnext |

## Local Development

| Property | Value |
|----------|-------|
| **Local URL** | http://sbserp.loc (via /etc/hosts) |
| **Docker Context** | 100.65.0.28 |
| **API Key** | 797d76cdc43eb6f |
| **API Secret** | 6f4d01de7d47cd8 |

```bash
# Switch to local Docker context
docker context use 100.65.0.28

# Start services
docker compose up -d

# Access local ERPNext
open http://sbserp.loc
```

## Project Goal: Single Source of Truth

**Goal: ERPNext becomes the single source of truth for Soundbox inventory management.**

All order data, inventory levels, customers, and containers are being migrated from Google Sheets to ERPNext. Once migration is complete, the original Google Sheets will no longer be needed.

**Migration Status:** In progress

## Migration Commands

```bash
# Run all migrations in order
python scripts/migrate.py

# Preview without changes
python scripts/migrate.py --dry-run

# Clear all transactional data (keep master data)
python scripts/clear.py

# Clear everything including master data
python scripts/clear.py --scope all

# Preview clear without executing
python scripts/clear.py --dry-run
```

## Script Naming Convention

| Prefix | Purpose | Examples |
|--------|---------|----------|
| (none) | Entry points | `migrate.py`, `clear.py` |
| `migrate_` | Migration modules | `migrate_customers.py`, `migrate_sales_orders.py` |
| `setup_` | One-time setup | `setup_custom_fields.py`, `setup_warehouses.py` |
| `ops_` | Operations/maintenance | `ops_validate.py`, `ops_reconcile.py` |

### Migration Steps (in order)
1. `master_data` - Products/Items
2. `customers` - Customers, Addresses, Contacts
3. `containers` - Container tracking
4. `inventory` - Opening stock
5. `sales_orders` - Sales Orders + Delivery Notes
6. `purchase_orders` - Purchase Orders
7. `allocations` - Container Pre-Allocations

## Subagent Usage

When using subagents (erpnext-developer, devops, etc.) for this project:
- **You decide the approach** - Determine the specific method/commands before spawning
- **Give explicit instructions** - Tell subagents exactly what to execute, not problems to solve
- **Subagents are executors, not planners** - They implement your plan, not create their own

**Run commands yourself, don't delegate:**
- Bash/SQL commands → Execute directly with Bash tool
- API calls → Make them directly
- Only use subagents for complex domain tasks requiring expertise

## Important Notes

1. **Always load `soundboxstore-erpnext` skill** for credentials and operational commands
2. **Production changes** should be deployed via Dokploy, not directly on the server
3. **Local development** requires Docker context `100.65.0.28`
