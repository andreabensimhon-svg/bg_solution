# Copy Jobs

Jobs de copie pour l'ingestion depuis les systèmes legacy (batch).

## Fichiers
- [`cj_erp_clients_full.json`](cj_erp_clients_full.json) : Chargement complet clients (daily, 02h00)

## Configuration
- **Mode** : Overwrite (full refresh)
- **Parallélisme** : 4 copies parallèles
- **Logging** : `Files/logs/copy_jobs/`
