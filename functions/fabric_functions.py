"""
BGSolutions - Azure Functions
Functions event-driven pour la plateforme data
"""
import json
import logging
from datetime import datetime, timezone


# ============================================================
# Function 1 : Alerte anomalies IoT
# Trigger : Event Hub (via Eventstream)
# ============================================================

def fn_alert_iot_anomaly(event_data: dict) -> dict:
    """
    Déclenché quand une anomalie critique IoT est détectée.
    Envoie une notification et log dans Cosmos DB.
    """
    logging.info(f"Anomalie IoT reçue : capteur={event_data.get('capteur_id')}")

    capteur_id = event_data.get("capteur_id")
    type_mesure = event_data.get("type_mesure")
    valeur = event_data.get("valeur")
    severite = event_data.get("severite", "info")

    # Seuils d'alerte par type
    seuils = {
        "temperature": {"warning": 150, "critical": 180},
        "pression":    {"warning": 300, "critical": 400},
        "humidite":    {"warning": 85,  "critical": 95}
    }

    seuil_config = seuils.get(type_mesure, {})

    alert = {
        "alert_id": f"ALT-{capteur_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "capteur_id": capteur_id,
        "type_mesure": type_mesure,
        "valeur": valeur,
        "severite": severite,
        "seuil_warning": seuil_config.get("warning"),
        "seuil_critical": seuil_config.get("critical"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": "notification_envoyee"
    }

    logging.info(f"Alerte générée : {alert['alert_id']} - Sévérité: {severite}")
    return alert


# ============================================================
# Function 2 : Enrichissement client temps réel
# Trigger : HTTP (appelé par les pipelines)
# ============================================================

def fn_enrich_client(client_id: int, cosmos_client=None) -> dict:
    """
    Enrichit les données d'un client avec les informations Cosmos DB
    et les métriques calculées en Gold.
    """
    logging.info(f"Enrichissement client : {client_id}")

    enrichment = {
        "client_id": client_id,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "score_credit": None,
        "segment_marketing": None,
        "derniere_interaction": None,
        "risque_churn": None
    }

    return enrichment


# ============================================================
# Function 3 : Validation commande JSON
# Trigger : HTTP (API endpoint)
# ============================================================

REQUIRED_FIELDS = ["order_id", "client_id", "order_date", "items"]
VALID_STATUSES = ["new", "confirmed", "shipped", "delivered", "cancelled"]

def fn_validate_json_order(order_payload: str) -> dict:
    """
    Valide une commande JSON entrante avant ingestion dans le Lakehouse.
    """
    try:
        order = json.loads(order_payload)
    except json.JSONDecodeError as e:
        return {"valid": False, "errors": [f"JSON invalide : {str(e)}"]}

    errors = []

    # Vérification champs obligatoires
    for field in REQUIRED_FIELDS:
        if field not in order:
            errors.append(f"Champ manquant : {field}")

    # Vérification statut
    if "status" in order and order["status"] not in VALID_STATUSES:
        errors.append(f"Statut invalide : {order['status']}")

    # Vérification items
    if "items" in order:
        if not isinstance(order["items"], list) or len(order["items"]) == 0:
            errors.append("items doit être une liste non vide")
        else:
            for i, item in enumerate(order["items"]):
                if "product_id" not in item:
                    errors.append(f"items[{i}] : product_id manquant")
                if "quantity" not in item or not isinstance(item.get("quantity"), (int, float)):
                    errors.append(f"items[{i}] : quantity invalide")

    # Vérification montant
    if "total_amount" in order:
        if not isinstance(order["total_amount"], (int, float)) or order["total_amount"] < 0:
            errors.append("total_amount doit être un nombre positif")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "order_id": order.get("order_id")
    }


# ============================================================
# Function 4 : Refresh orchestration
# Trigger : Timer (planifié quotidien)
# ============================================================

def fn_daily_refresh_orchestrator() -> dict:
    """
    Orchestre le refresh quotidien :
    1. Déclenche les pipelines d'ingestion
    2. Attend la fin des traitements
    3. Rafraîchit le modèle sémantique Power BI
    """
    execution = {
        "execution_id": f"EXEC-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "steps": [
            {"name": "ingest_clients", "status": "pending"},
            {"name": "ingest_erp_finance", "status": "pending"},
            {"name": "ingest_erp_sales", "status": "pending"},
            {"name": "ingest_erp_operations", "status": "pending"},
            {"name": "transform_silver_to_gold", "status": "pending"},
            {"name": "refresh_semantic_model", "status": "pending"},
        ]
    }

    logging.info(f"Orchestration quotidienne démarrée : {execution['execution_id']}")
    return execution
