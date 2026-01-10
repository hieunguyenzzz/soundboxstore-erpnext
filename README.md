# Soundbox ERPNext

ERPNext deployment for Soundbox inventory management.

## Stack

- ERPNext v15
- MariaDB 10.6
- Redis (cache + queue)
- Nginx

## Quick Start (Local)

1. Copy environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your passwords

3. Switch to local Docker context:
   ```bash
   docker context use 100.65.0.28
   ```

4. Start services:
   ```bash
   docker compose up -d
   ```

5. Create the site (first time only):
   ```bash
   docker compose --profile setup up create-site
   ```

6. Access at http://localhost:8080

## Production Deployment

Production is deployed via Dokploy on OVH server (139.99.9.132).

- URL: https://erp.soundboxstore.com
- Dokploy project: soundboxstore

## Services

| Service | Port | Description |
|---------|------|-------------|
| frontend | 8080 | Nginx reverse proxy |
| backend | 8000 | Frappe/ERPNext app |
| websocket | 9000 | Socket.io |
| db | 3306 | MariaDB |
| redis-cache | 6379 | Redis cache |
| redis-queue | 6379 | Redis queue |

## Backups

Daily backups to MinIO (minio-api.hieunguyen.dev) bucket: `erpnext-backups`
