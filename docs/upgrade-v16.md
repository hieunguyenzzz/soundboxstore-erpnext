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

Dokploy settings (read via `compose.one`, composeId `2JPiergVusPdel0d9EoPa`, 2026-10-02):
`isolatedDeployment: false`, `randomize: false`, `isolatedDeploymentsVolume: false`, `suffix: ""`,
`autoDeploy: true` on `main`. So the pinned `sbs-erpnext-*` volume names are used as written (no suffixing).

### Before merging

1. **Backup** (done 2026-10-02): cold tars of `code_db-data` and `code_sites` with `SHA256SUMS` in
   `/home/debian/backups/erpnext-2026-10-02/`.
2. **Copy the data into the new volume names** (cold; nothing mounts `code_*`). The `code_*` volumes are
   never written and stay as the rollback. Refuse to copy over an existing target, so an empty datadir
   from a stray deploy never gets mixed with the 10.6 files:
   ```bash
   for v in db-data sites logs redis-queue-data; do
     [ -z "$(sudo docker ps -aq --filter volume=code_$v)" ] || { echo "code_$v is mounted"; exit 1; }
     sudo docker volume inspect sbs-erpnext-$v >/dev/null 2>&1 && { echo "sbs-erpnext-$v exists"; exit 1; }
     sudo docker volume create sbs-erpnext-$v
     sudo docker run --rm -v code_$v:/from:ro -v sbs-erpnext-$v:/to alpine cp -a /from/. /to/
   done
   ```
   Compose later warns these volumes "were not created by Docker Compose"; expected and harmless.
3. **Keep v16 workers/scheduler off the v15 schema until migrate**: set `maintenance_mode` and
   `pause_scheduler` in the copied site config (the desk answers 503 meanwhile):
   ```bash
   sudo docker run --rm --entrypoint bash -v sbs-erpnext-sites:/home/frappe/frappe-bench/sites frappe/erpnext:v16.37.0 -c \
     'cd sites/erp.soundboxstore.com && jq ". + {maintenance_mode: 1, pause_scheduler: 1}" site_config.json > /tmp/sc && cat /tmp/sc > site_config.json'
   ```
4. **Dokploy env**: set `ERPNEXT_VERSION=v16.37.0` (or delete the variable); the current `v15` overrides
   the compose default. Keep `DB_PASSWORD` as is: it is the datadir's root password (the local rehearsal
   used exactly this value for root against the copied datadir).

### Merge, then migrate

5. **Merge the PR**; Dokploy auto-deploys `main`.
6. **Check the db mounts the right volume and has finished auto-upgrading.** Don't rely on `healthy`
   (1s x 20 retries may be too short during the upgrade); wait for the real server on port 3306:
   ```bash
   P=compose-synthesize-redundant-bus-lmvxll
   DB=$(sudo docker ps --format '{{.Names}}' | grep "^$P-db-")
   sudo docker inspect $DB --format '{{range .Mounts}}{{.Name}} -> {{.Destination}}{{println}}{{end}}'  # sbs-erpnext-db-data -> /var/lib/mysql
   until sudo docker logs $DB 2>&1 | grep -q "Finished mariadb-upgrade" && sudo docker logs $DB 2>&1 | grep -q "ready for connections" && sudo docker logs $DB 2>&1 | grep -q "port: 3306"; do sleep 2; done
   ```
7. **Migrate, then lift the flags**:
   ```bash
   B=$(sudo docker ps --format '{{.Names}}' | grep "^$P-backend-")
   sudo docker exec $B bench --site erp.soundboxstore.com migrate     # ~2 min locally, 122 patches
   sudo docker exec $B bench --site erp.soundboxstore.com clear-cache
   sudo docker exec $B bench --site erp.soundboxstore.com set-maintenance-mode off
   sudo docker exec $B bench --site erp.soundboxstore.com scheduler resume
   sudo docker restart $(sudo docker ps --format '{{.Names}}' | grep -E "^$P-(backend|websocket|queue|scheduler|frontend)")
   sudo docker exec $B bench --site erp.soundboxstore.com backup --with-files
   ```
8. **Verify**: `https://erp.soundboxstore.com/api/method/ping` returns 200, desk loads, record counts match
   the baseline below, Scheduled Job Log shows new `Complete` rows.

Expected non-fatal migrate output: `Error in setting standard field Could not find Row #1: Link To: Payments`
(caught inside `add_standard_field_in_workspace_sidebar`; the patch reports Success).

Steps 2-3 and 6-7 were rehearsed locally on a fresh restore: no scheduled jobs ran while paused, desk
returned 503, migrate exited 0, counts matched, jobs resumed after `scheduler resume`.

## Baseline (cold copy, verified before and after the local upgrade)

Item 3420, Customer 581, Container 124, Sales Order 111, Purchase Order 74, Bin 92, Warehouse 16, User 3.
Delivery Note, Stock Entry, Sales Invoice, Payment Entry, Stock Ledger Entry, GL Entry, Purchase Receipt: 0.
Custom Field 27 → 31 after migrate (all 27 kept; v16 adds `impersonate` on DocPerm/Custom DocPerm/DocShare
and `UTM Campaign-crm_campaign`).

## Rollback

Rollback **discards anything entered in v16 after cutover**: MariaDB 11.8 upgrades the datadir in place,
so the `sbs-erpnext-*` volumes cannot go back to 10.6, and the data comes back from the untouched `code_*` volumes.

1. Stop the stack in Dokploy.
2. Open a new PR/commit to `main` (it auto-deploys when merged) that reverts the image default to
   `frappe/erpnext:v15.94.3`, `mariadb:10.6`, drops `MARIADB_AUTO_UPGRADE` (keep the pinned volume names and
   the healthcheck can go back to `mysqladmin`). Set Dokploy `ERPNEXT_VERSION=v15.94.3`.
3. Before merging it: `sudo docker volume rm sbs-erpnext-db-data sbs-erpnext-sites sbs-erpnext-logs sbs-erpnext-redis-queue-data`,
   then recreate them from `code_*` with step 2 above (skip step 3; v15 needs no flags).
4. Merge, then verify ping and counts.

## Local rehearsal

```bash
# load the tars into sbs-erpnext-* volumes, then:
docker compose -p sbs-erpnext-local --env-file <env with prod DB_PASSWORD> \
  -f docker-compose.yml -f docker-compose.local.yml up -d
docker exec sbs-erpnext-backend bench --site erp.soundboxstore.com migrate
```
Served at http://sbserp.loc via traefik-local. Don't run `bench browse` inside the backend container on v16:
it spawns `xdg-open`, whose exit code 3 is reaped by gunicorn (PID 1) as a worker boot failure and kills it.
