# Restore + upgrade to ERPNext v16 (SBS-624)

## Why the site went down

Dokploy used to deploy both this app (`compose-synthesize-redundant-bus-lmvxll`) and Meraki's ERPNext
(`compose-bypass-optical-monitor-*`) from a directory called `code`, so both got Compose project name `code`.
A Meraki deploy replaced our `code-*` containers. The data survived in the now-unused volumes
`code_db-data`, `code_sites`, `code_logs`, `code_redis-queue-data` (DB `_a61f2189740aabbc`, site `erp.soundboxstore.com`).

Dokploy now derives the project name from the app name, so a plain redeploy would mount *empty*
`compose-synthesize-redundant-bus-lmvxll_*` volumes. The compose file now pins volume names
(`sbs-erpnext-*`) so the data no longer depends on the project or directory name.

## Versions

| | Before | After |
|---|---|---|
| Image | `frappe/erpnext:v15.94.3` (frappe 15.96.0) | `frappe/erpnext:v16.37.0` (frappe 16.36.0) |
| MariaDB | 10.6 | 11.8, `MARIADB_AUTO_UPGRADE=1` |

MariaDB 11.8 images no longer ship `mysqladmin`, so the healthcheck uses `mariadb-admin`. It pings
`127.0.0.1` (TCP) on purpose: during auto-upgrade the entrypoint runs a socket-only temporary server,
and a `localhost` (socket) ping reports healthy while the real server is still about to restart.

## Production steps (OVH, `ssh debian@139.99.9.132`)

1. **Backup** (done 2026-10-02): cold tars of `code_db-data` and `code_sites` with `SHA256SUMS` in
   `/home/debian/backups/erpnext-2026-10-02/`.
2. **Copy the data into the new volume names** (cold; nothing mounts `code_*`). The `code_*` volumes are
   never written and stay as the rollback.
   ```bash
   for v in db-data sites logs redis-queue-data; do
     sudo docker ps -a --filter volume=code_$v --format '{{.Names}}'   # must print nothing
     sudo docker volume create sbs-erpnext-$v
     sudo docker run --rm -v code_$v:/from:ro -v sbs-erpnext-$v:/to alpine cp -a /from/. /to/
   done
   ```
   Compose warns that the volumes "were not created by Docker Compose"; that is expected and harmless.
3. **Dokploy env**: set `ERPNEXT_VERSION=v16.37.0` (or delete the variable). The current `v15` value
   overrides the compose default.
4. **Merge the PR** — Dokploy auto-deploys `main`.
5. **Migrate** once the db container is `healthy` (backend is `<app>-backend-1`):
   ```bash
   B=$(sudo docker ps --format '{{.Names}}' | grep compose-synthesize-redundant-bus-lmvxll-backend)
   sudo docker exec $B bench --site erp.soundboxstore.com migrate     # ~2 min locally, 122 patches
   sudo docker exec $B bench --site erp.soundboxstore.com clear-cache
   sudo docker restart $(sudo docker ps --format '{{.Names}}' | grep -E 'compose-synthesize-redundant-bus-lmvxll-(backend|websocket|queue|scheduler|frontend)')
   sudo docker exec $B bench --site erp.soundboxstore.com backup --with-files
   ```
6. **Verify**: `https://erp.soundboxstore.com/api/method/ping` returns 200, desk loads, record counts match
   the baseline below.

Expected non-fatal migrate output: `Error in setting standard field Could not find Row #1: Link To: Payments`
(caught inside `add_standard_field_in_workspace_sidebar`; the patch reports Success).

## Baseline (cold copy, verified before and after the local upgrade)

Item 3420, Customer 581, Container 124, Sales Order 111, Purchase Order 74, Bin 92, Warehouse 16, User 3.
Delivery Note, Stock Entry, Sales Invoice, Payment Entry, Stock Ledger Entry, GL Entry, Purchase Receipt: 0.
Custom Field 27 → 31 after migrate (all 27 kept; v16 adds `impersonate` on DocPerm/Custom DocPerm/DocShare
and `UTM Campaign-crm_campaign`).

## Rollback

MariaDB 11.8 upgrades the datadir in place, so the `sbs-erpnext-*` volumes cannot go back to 10.6.
Roll back from the untouched `code_*` volumes instead:

1. Stop the stack in Dokploy.
2. `sudo docker volume rm sbs-erpnext-db-data sbs-erpnext-sites` (other two optional), then repeat step 2 above.
3. Deploy a commit with `frappe/erpnext:v15.94.3`, `mariadb:10.6` and the pinned `sbs-erpnext-*` volume names
   (revert only the image/MariaDB lines), and set `ERPNEXT_VERSION=v15.94.3` in Dokploy.

## Local rehearsal

```bash
# load the tars into sbs-erpnext-* volumes, then:
docker compose -p sbs-erpnext-local --env-file <env with prod DB_PASSWORD> \
  -f docker-compose.yml -f docker-compose.local.yml up -d
docker exec sbs-erpnext-backend bench --site erp.soundboxstore.com migrate
```
Served at http://sbserp.loc via traefik-local. Don't run `bench browse` inside the backend container on v16:
it spawns `xdg-open`, whose exit code 3 is reaped by gunicorn (PID 1) as a worker boot failure and kills it.
