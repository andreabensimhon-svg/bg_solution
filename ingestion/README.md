# Ingestion - Sources de données

## Sources configurées

### Structurées
- ERP (données finance, opérations, ventes)
- BDD Clients partagée

### Semi-structurées
- Fichiers JSON (commandes, logs)

### Non-structurées
- Corps de mails
- Données de plan de supply (optimisation chaîne)
- Images capteurs IoT

### Temps réel
- Capteurs IoT → Eventstream

## Composants

| Dossier | Contenu |
|---------|--------|
| `pipelines/` | 5 pipelines JSON (orchestrateur + 4 ingestions) |
| `connectors/` | Configuration des 6 connecteurs sources/destinations |
| `copy_jobs/` | Job de copie full clients |
| `dataflow_gen2/` | Transformations Power Query M (mails, JSON orders) |
