"""
BGSolutions - Fabric Functions (cross-domaine)
"""
import json, logging
from datetime import datetime, timezone

REQUIRED_FIELDS = ["order_id", "client_id", "order_date", "items"]
VALID_STATUSES = ["new", "confirmed", "shipped", "delivered", "cancelled"]

def fn_alert_iot_anomaly(event_data: dict) -> dict:
    """Event Hub trigger : alerte sur anomalie IoT critique → Cosmos DB + notification"""
    capteur_id = event_data.get("capteur_id")
    return {
        "alert_id": f"ALT-{capteur_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "capteur_id": capteur_id, "severite": event_data.get("severite", "info"),
        "timestamp": datetime.now(timezone.utc).isoformat(), "action": "notification_envoyee"
    }

def fn_validate_json_order(order_payload: str) -> dict:
    """HTTP trigger : validation commande JSON avant ingestion lh_sales"""
    try:
        order = json.loads(order_payload)
    except json.JSONDecodeError as e:
        return {"valid": False, "errors": [f"JSON invalide : {str(e)}"]}

    errors = []
    for field in REQUIRED_FIELDS:
        if field not in order:
            errors.append(f"Champ manquant : {field}")
    if "status" in order and order["status"] not in VALID_STATUSES:
        errors.append(f"Statut invalide : {order['status']}")
    if "items" in order and (not isinstance(order["items"], list) or len(order["items"]) == 0):
        errors.append("items doit être une liste non vide")
    return {"valid": len(errors) == 0, "errors": errors, "order_id": order.get("order_id")}

def fn_daily_orchestrator() -> dict:
    """Timer trigger : orchestration quotidienne de tous les domaines"""
    return {
        "execution_id": f"EXEC-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "steps": [
            {"name": "pl_shared", "status": "pending"},
            {"name": "pl_finance", "status": "pending"},
            {"name": "pl_operations", "status": "pending"},
            {"name": "pl_sales", "status": "pending"},
            {"name": "iot_ml_anomalies", "status": "pending"},
        ]
    }
