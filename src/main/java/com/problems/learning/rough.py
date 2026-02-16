import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr

# ---------- Env ----------
DDB_TABLE = os.environ["EXECUTIONS_TABLE"]                # e.g., tdm-dynamo-sdg-executions
P2X_LAMBDA_ARN = os.environ["P2X_LAMBDA_ARN"]             # cross-account allowed via Lambda resource policy
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Optional tuning
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "5"))
MAX_PROCESSING_PER_RUN = int(os.environ.get("MAX_PROCESSING_PER_RUN", "25"))
DDB_STATUS_ATTR = os.environ.get("DDB_STATUS_ATTR", "status")
DDB_QUEUEID_ATTR = os.environ.get("DDB_QUEUEID_ATTR", "queueId")
DDB_PK_ATTR = os.environ.get("DDB_PK_ATTR", "executionId")  # adjust to your table key

# Status values used in DDB
STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_FAILED = "FAILED"
STATUS_COMPLETE = "GENERATION_COMPLETE"  # or whatever your terminal success is

# What we store as the "GMUS observed state" (separate from orchestration status)
OBS_ACTIVE = "ACTIVE"
OBS_TERMINAL_SUCCESS = "TERMINAL_SUCCESS"
OBS_TERMINAL_FAIL = "TERMINAL_FAIL"
OBS_UNKNOWN = "UNKNOWN"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_table = dynamodb.Table(DDB_TABLE)

lambda_client = boto3.client("lambda", region_name=AWS_REGION)


# ---------- Helpers ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def invoke_p2x(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Invokes P2X Lambda synchronously and returns parsed JSON.
    Requires cross-account invoke permission + correct IAM on this Lambda.
    """
    resp = lambda_client.invoke(
        FunctionName=P2X_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw = resp["Payload"].read()
    if not raw:
        raise RuntimeError("Empty response from P2X")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Sometimes Lambda wraps response in 'body'
        raise RuntimeError(f"Non-JSON response from P2X: {raw[:300]!r}")

    # Handle common proxy-style wrapping
    if isinstance(data, dict) and "body" in data:
        try:
            return json.loads(data["body"])
        except Exception:
            return data

    return data

def safe_get_list(resp: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    """
    Your screenshot shows:
      "list_queue": {"success": true, "data": [...]}
    """
    node = resp.get(key) or {}
    if isinstance(node, dict) and node.get("success") is True:
        return node.get("data") or []
    # allow direct list too
    if isinstance(node, list):
        return node
    return []

def index_by_queue_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    GMUS responses vary; we try common keys: queueId, queueID, id
    """
    out = {}
    for it in items:
        qid = (
            str(it.get("queueId"))
            if it.get("queueId") is not None
            else str(it.get("queueID")) if it.get("queueID") is not None
            else str(it.get("id")) if it.get("id") is not None
            else None
        )
        if qid and qid != "None":
            out[qid] = it
    return out

def classify_job(queue_item: Dict[str, Any], default_active: bool) -> Tuple[str, str]:
    """
    Returns (observed_state, detail_status_string).
    Because GMUS fields can differ, we use heuristics.
    - For active queues, treat as ACTIVE unless explicit completion.
    - For history queues, treat as TERMINAL; check success/fail fields if present.
    """
    # Common possible fields (varies by GMUS / org wrapper)
    status = str(queue_item.get("status") or queue_item.get("state") or "").upper()
    result = str(queue_item.get("result") or "").upper()
    error = queue_item.get("error") or queue_item.get("errorMessage") or queue_item.get("exception")

    # If it explicitly says failed
    if "FAIL" in status or "ERROR" in status or error:
        return (OBS_TERMINAL_FAIL if not default_active else OBS_ACTIVE, status or "FAILED")

    # If it explicitly says completed/success
    if "COMPLETE" in status or "SUCCESS" in status or result == "SUCCESS":
        return (OBS_TERMINAL_SUCCESS if not default_active else OBS_ACTIVE, status or "COMPLETED")

    # If in active list, treat as ACTIVE (WAITING/RUNNING unknown granularity)
    if default_active:
        # Try to differentiate waiting vs running if a field exists
        if "WAIT" in status or "PENDING" in status or "QUEUED" in status:
            return (OBS_ACTIVE, status or "WAITING")
        if "RUN" in status or "EXEC" in status or "PROCESS" in status:
            return (OBS_ACTIVE, status or "RUNNING")
        return (OBS_ACTIVE, status or "ACTIVE")

    # In history but no explicit success/fail → terminal unknown; assume success unless fields show otherwise
    return (OBS_TERMINAL_SUCCESS, status or "COMPLETED")

def determine_queueid_status(queue_id: str, list_resp: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Uses four lists:
      list_queue, list_queue_history, list_repo_queue, list_repo_queue_history

    Returns:
      (observed_state, observed_detail, source_bucket)
    where source_bucket tells which list we found it in.
    """
    active_main = index_by_queue_id(safe_get_list(list_resp, "list_queue"))
    hist_main = index_by_queue_id(safe_get_list(list_resp, "list_queue_history"))
    active_repo = index_by_queue_id(safe_get_list(list_resp, "list_repo_queue"))
    hist_repo = index_by_queue_id(safe_get_list(list_resp, "list_repo_queue_history"))

    qid = str(queue_id)

    if qid in active_main:
        obs, detail = classify_job(active_main[qid], default_active=True)
        return obs, detail, "list_queue"

    if qid in active_repo:
        obs, detail = classify_job(active_repo[qid], default_active=True)
        return obs, detail, "list_repo_queue"

    if qid in hist_main:
        obs, detail = classify_job(hist_main[qid], default_active=False)
        return obs, detail, "list_queue_history"

    if qid in hist_repo:
        obs, detail = classify_job(hist_repo[qid], default_active=False)
        return obs, detail, "list_repo_queue_history"

    return OBS_UNKNOWN, "NOT_FOUND", "none"


# ---------- DDB access ----------
def scan_by_status(status_value: str, limit: int) -> List[Dict[str, Any]]:
    """
    For simplicity, uses Scan with FilterExpression.
    For production, prefer a GSI on status (much cheaper).
    """
    items: List[Dict[str, Any]] = []
    last_key = None

    while len(items) < limit:
        kwargs = {
            "FilterExpression": Attr(DDB_STATUS_ATTR).eq(status_value),
            "Limit": min(50, limit - len(items)),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = ddb_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return items[:limit]

def update_execution(execution_id: str, updates: Dict[str, Any]) -> None:
    """
    Updates attributes on the item identified by executionId (PK).
    Adjust if you have a sort key.
    """
    # Build UpdateExpression
    expr_parts = []
    expr_attr_values = {}
    expr_attr_names = {}

    for k, v in updates.items():
        name_key = f"#{k}"
        value_key = f":{k}"
        expr_attr_names[name_key] = k
        expr_attr_values[value_key] = v
        expr_parts.append(f"{name_key} = {value_key}")

    update_expr = "SET " + ", ".join(expr_parts)

    ddb_table.update_item(
        Key={DDB_PK_ATTR: execution_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
    )


# ---------- Orchestration ----------
def handle_new_executions() -> Dict[str, int]:
    started = 0
    failed = 0

    new_items = scan_by_status(STATUS_NEW, MAX_NEW_PER_RUN)

    for item in new_items:
        execution_id = item[DDB_PK_ATTR]

        try:
            # P2X execute: P2P passes along the execution context you already store.
            p2x_payload = {
                "cmdType": "execute",
                "execution": item,  # pass full item or a reduced subset
            }
            p2x_resp = invoke_p2x(p2x_payload)

            # Expect P2X returns queueId somewhere
            queue_id = (
                p2x_resp.get("queueId")
                or p2x_resp.get("queueID")
                or (p2x_resp.get("data") or {}).get("queueId")
            )
            if not queue_id:
                raise RuntimeError(f"P2X execute did not return queueId. resp keys={list(p2x_resp.keys())}")

            update_execution(execution_id, {
                DDB_STATUS_ATTR: STATUS_PROCESSING,
                DDB_QUEUEID_ATTR: str(queue_id),
                "updatedAt": utc_now_iso(),
                "observedState": OBS_ACTIVE,
                "observedDetail": "SUBMITTED",
                "observedSource": "execute",
            })
            started += 1

        except Exception as e:
            update_execution(execution_id, {
                DDB_STATUS_ATTR: STATUS_FAILED,
                "updatedAt": utc_now_iso(),
                "observedState": OBS_TERMINAL_FAIL,
                "observedDetail": "SUBMIT_FAILED",
                "errorMessage": str(e)[:1000],
            })
            failed += 1

    return {"started": started, "failed": failed}

def handle_processing_executions() -> Dict[str, int]:
    checked = 0
    changed = 0

    processing_items = scan_by_status(STATUS_PROCESSING, MAX_PROCESSING_PER_RUN)
    if not processing_items:
        return {"checked": 0, "changed": 0}

    # One listQueue call can cover many executions (cheaper than per-queueId calls)
    list_resp = invoke_p2x({"cmdType": "listQueue"})

    for item in processing_items:
        execution_id = item[DDB_PK_ATTR]
        queue_id = item.get(DDB_QUEUEID_ATTR)

        if not queue_id:
            # Bad state; mark failed or unknown
            update_execution(execution_id, {
                DDB_STATUS_ATTR: STATUS_FAILED,
                "updatedAt": utc_now_iso(),
                "observedState": OBS_TERMINAL_FAIL,
                "observedDetail": "MISSING_QUEUE_ID",
            })
            changed += 1
            continue

        obs_state, obs_detail, obs_source = determine_queueid_status(str(queue_id), list_resp)
        checked += 1

        prev_obs_state = item.get("observedState")
        prev_obs_detail = item.get("observedDetail")

        # Update only when there is a meaningful change
        if obs_state != prev_obs_state or obs_detail != prev_obs_detail:
            updates = {
                "updatedAt": utc_now_iso(),
                "observedState": obs_state,
                "observedDetail": obs_detail,
                "observedSource": obs_source,
            }

            # Map observed terminal state to orchestration status
            if obs_state == OBS_TERMINAL_SUCCESS:
                updates[DDB_STATUS_ATTR] = STATUS_COMPLETE
            elif obs_state == OBS_TERMINAL_FAIL:
                updates[DDB_STATUS_ATTR] = STATUS_FAILED
            elif obs_state == OBS_UNKNOWN:
                # Keep PROCESSING but record unknown; you can add retry counters here
                updates["unknownSince"] = item.get("unknownSince") or utc_now_iso()

            update_execution(execution_id, updates)
            changed += 1

    return {"checked": checked, "changed": changed}


def lambda_handler(event, context):
    """
    Run by EventBridge schedule every minute (recommended).
    """
    results = {
        "new": handle_new_executions(),
        "processing": handle_processing_executions(),
        "ts": utc_now_iso(),
    }
    return results