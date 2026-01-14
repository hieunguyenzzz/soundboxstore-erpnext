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

**Migration Status:** In progress - see `scripts/` for migration scripts.

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
