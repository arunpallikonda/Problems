import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr

# =============================================================================
# ENV
# =============================================================================
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

DDB_TABLE = os.environ["EXECUTIONS_TABLE"]          # e.g., tdm-dynamo-sdg-executions
P2X_LAMBDA_ARN = os.environ["P2X_LAMBDA_ARN"]        # cross-account invoke allowed

# Dynamo keys/attrs (adjust to your table schema)
DDB_PK_ATTR = os.environ.get("DDB_PK_ATTR", "executionId")
DDB_STATUS_ATTR = os.environ.get("DDB_STATUS_ATTR", "status")

# Dual-ID fields we store
DDB_REPO_QID_ATTR = os.environ.get("DDB_REPO_QID_ATTR", "repoQueueId")
DDB_EXEC_QID_ATTR = os.environ.get("DDB_EXEC_QID_ATTR", "execQueueId")

# Orchestration statuses
STATUS_NEW = os.environ.get("STATUS_NEW", "NEW")
STATUS_PROCESSING = os.environ.get("STATUS_PROCESSING", "PROCESSING")
STATUS_COMPLETE = os.environ.get("STATUS_COMPLETE", "GENERATION_COMPLETE")
STATUS_FAILED = os.environ.get("STATUS_FAILED", "FAILED")

# Limits per run
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "5"))
MAX_PROCESSING_PER_RUN = int(os.environ.get("MAX_PROCESSING_PER_RUN", "25"))

# If your repo-queue/history item contains a link to an execution ID, list the possible field names here.
# (Docs don't guarantee these; your env might use one of them.)
REPO_TO_EXEC_LINK_FIELDS = [
    f.strip() for f in os.environ.get(
        "REPO_TO_EXEC_LINK_FIELDS",
        "executionQueueId,execQueueId,gmuxQueueId,executionId,jobQueueId"
    ).split(",")
    if f.strip()
]

# =============================================================================
# AWS CLIENTS
# =============================================================================
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_table = dynamodb.Table(DDB_TABLE)

lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# =============================================================================
# UTIL
# =============================================================================
OBS_ACTIVE = "ACTIVE"
OBS_TERMINAL_SUCCESS = "TERMINAL_SUCCESS"
OBS_TERMINAL_FAIL = "TERMINAL_FAIL"
OBS_UNKNOWN = "UNKNOWN"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def invoke_p2x(payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = lambda_client.invoke(
        FunctionName=P2X_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw = resp["Payload"].read()
    if not raw:
        raise RuntimeError("Empty response from P2X")
    data = json.loads(raw)

    # Support APIGW-style wrapping
    if isinstance(data, dict) and "body" in data and isinstance(data["body"], str):
        try:
            return json.loads(data["body"])
        except Exception:
            return data
    return data

def safe_get_list(resp: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    """
    Expected shapes:
      "list_queue": {"success": true, "data": [...]}
      OR sometimes directly a list
    """
    node = resp.get(key)
    if isinstance(node, dict) and node.get("success") is True:
        return node.get("data") or []
    if isinstance(node, list):
        return node
    return []

def extract_queue_id(item: Dict[str, Any]) -> Optional[str]:
    """
    Normalize common queue id keys.
    """
    for k in ("queueId", "queueID", "id"):
        if k in item and item[k] is not None:
            return str(item[k])
    return None

def index_by_queue_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for it in items:
        qid = extract_queue_id(it)
        if qid:
            out[qid] = it
    return out

def classify_job(queue_item: Dict[str, Any], in_active_list: bool) -> Tuple[str, str]:
    """
    Heuristic classification; tighten once you see real GMUS fields.
    """
    status = str(queue_item.get("status") or queue_item.get("state") or "").upper()
    result = str(queue_item.get("result") or "").upper()
    error = queue_item.get("error") or queue_item.get("errorMessage") or queue_item.get("exception")

    # failure signals
    if error or "FAIL" in status or "ERROR" in status:
        return (OBS_ACTIVE, status or "FAILED") if in_active_list else (OBS_TERMINAL_FAIL, status or "FAILED")

    # success/complete signals
    if "COMPLETE" in status or "SUCCESS" in status or result == "SUCCESS":
        return (OBS_ACTIVE, status or "COMPLETED") if in_active_list else (OBS_TERMINAL_SUCCESS, status or "COMPLETED")

    # active list: assume ACTIVE
    if in_active_list:
        if "WAIT" in status or "PENDING" in status or "QUEUED" in status:
            return (OBS_ACTIVE, status or "WAITING")
        if "RUN" in status or "EXEC" in status or "PROCESS" in status:
            return (OBS_ACTIVE, status or "RUNNING")
        return (OBS_ACTIVE, status or "ACTIVE")

    # history list but no explicit markers: assume terminal success
    return (OBS_TERMINAL_SUCCESS, status or "COMPLETED")

def try_discover_exec_id_from_repo_item(repo_item: Dict[str, Any]) -> Optional[str]:
    """
    If your environment includes a link field, return it.
    Docs don't guarantee this; it's environment-specific.
    """
    for f in REPO_TO_EXEC_LINK_FIELDS:
        v = repo_item.get(f)
        if v is not None and str(v).strip():
            return str(v)
    return None

# =============================================================================
# DDB HELPERS
# =============================================================================
def scan_by_status(status_value: str, limit: int) -> List[Dict[str, Any]]:
    """
    Uses Scan for simplicity. Prefer a GSI on status in production.
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
    expr_parts = []
    expr_attr_values = {}
    expr_attr_names = {}

    for k, v in updates.items():
        nk = f"#{k}"
        vk = f":{k}"
        expr_attr_names[nk] = k
        expr_attr_values[vk] = v
        expr_parts.append(f"{nk} = {vk}")

    ddb_table.update_item(
        Key={DDB_PK_ATTR: execution_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
    )

# =============================================================================
# STATUS RESOLUTION (DUAL ID)
# =============================================================================
def resolve_status_dual_ids(
    repo_qid: Optional[str],
    exec_qid: Optional[str],
    list_resp: Dict[str, Any]
) -> Tuple[str, str, str, Optional[str]]:
    """
    Returns:
      (observedState, observedDetail, observedSourceListName, discoveredExecQueueId_if_any)

    Priority:
      - if exec_qid exists: check list_queue, list_queue_history first
      - else check repo lists for repo_qid
      - also attempt to discover exec id from repo items if possible
    """
    lq = index_by_queue_id(safe_get_list(list_resp, "list_queue"))
    lqh = index_by_queue_id(safe_get_list(list_resp, "list_queue_history"))
    lrq = index_by_queue_id(safe_get_list(list_resp, "list_repo_queue"))
    lrqh = index_by_queue_id(safe_get_list(list_resp, "list_repo_queue_history"))

    # 1) Prefer exec queue id if present
    if exec_qid:
        if exec_qid in lq:
            obs, detail = classify_job(lq[exec_qid], in_active_list=True)
            return obs, detail, "list_queue", None
        if exec_qid in lqh:
            obs, detail = classify_job(lqh[exec_qid], in_active_list=False)
            return obs, detail, "list_queue_history", None

    # 2) Fall back to repo queue id
    discovered_exec = None
    if repo_qid:
        if repo_qid in lrq:
            # Try discover exec id link (if any) even while active
            discovered_exec = try_discover_exec_id_from_repo_item(lrq[repo_qid])
            obs, detail = classify_job(lrq[repo_qid], in_active_list=True)
            return obs, detail, "list_repo_queue", discovered_exec

        if repo_qid in lrqh:
            discovered_exec = try_discover_exec_id_from_repo_item(lrqh[repo_qid])
            obs, detail = classify_job(lrqh[repo_qid], in_active_list=False)
            return obs, detail, "list_repo_queue_history", discovered_exec

    return OBS_UNKNOWN, "NOT_FOUND", "none", None

# =============================================================================
# ORCHESTRATION
# =============================================================================
def handle_new_executions() -> Dict[str, int]:
    started = 0
    failed = 0

    items = scan_by_status(STATUS_NEW, MAX_NEW_PER_RUN)
    for item in items:
        execution_id = item[DDB_PK_ATTR]

        try:
            # Submit to P2X (execute mode)
            resp = invoke_p2x({"cmdType": "execute", "execution": item})

            # P2X may return queueId only (common), or may distinguish repoQueueId/execQueueId.
            # We handle all possibilities.
            queue_id = resp.get("queueId") or resp.get("queueID")
            repo_qid = resp.get("repoQueueId") or queue_id
            exec_qid = resp.get("execQueueId")

            updates = {
                DDB_STATUS_ATTR: STATUS_PROCESSING,
                "updatedAt": utc_now_iso(),
                "observedState": OBS_ACTIVE,
                "observedDetail": "SUBMITTED",
                "observedSource": "execute",
            }
            if repo_qid:
                updates[DDB_REPO_QID_ATTR] = str(repo_qid)
            if exec_qid:
                updates[DDB_EXEC_QID_ATTR] = str(exec_qid)

            update_execution(execution_id, updates)
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

    items = scan_by_status(STATUS_PROCESSING, MAX_PROCESSING_PER_RUN)
    if not items:
        return {"checked": 0, "changed": 0}

    # Single call to P2X to fetch all queues
    list_resp = invoke_p2x({"cmdType": "listQueue"})

    for item in items:
        execution_id = item[DDB_PK_ATTR]
        repo_qid = item.get(DDB_REPO_QID_ATTR)
        exec_qid = item.get(DDB_EXEC_QID_ATTR)

        obs_state, obs_detail, obs_src, discovered_exec = resolve_status_dual_ids(
            str(repo_qid) if repo_qid else None,
            str(exec_qid) if exec_qid else None,
            list_resp
        )
        checked += 1

        prev_obs_state = item.get("observedState")
        prev_obs_detail = item.get("observedDetail")
        prev_exec_qid = item.get(DDB_EXEC_QID_ATTR)

        updates: Dict[str, Any] = {}
        # Update when state/detail changed OR we discovered exec id
        if (obs_state != prev_obs_state) or (obs_detail != prev_obs_detail) or (discovered_exec and not prev_exec_qid):
            updates.update({
                "updatedAt": utc_now_iso(),
                "observedState": obs_state,
                "observedDetail": obs_detail,
                "observedSource": obs_src,
            })

            # If we can learn exec id from repo items, persist it
            if discovered_exec and not prev_exec_qid:
                updates[DDB_EXEC_QID_ATTR] = str(discovered_exec)

            # Map terminal to orchestration status
            if obs_state == OBS_TERMINAL_SUCCESS:
                updates[DDB_STATUS_ATTR] = STATUS_COMPLETE
            elif obs_state == OBS_TERMINAL_FAIL:
                updates[DDB_STATUS_ATTR] = STATUS_FAILED
            elif obs_state == OBS_UNKNOWN:
                # remain PROCESSING but annotate
                updates.setdefault("unknownSince", item.get("unknownSince") or utc_now_iso())

            update_execution(execution_id, updates)
            changed += 1

    return {"checked": checked, "changed": changed}

def lambda_handler(event, context):
    results = {
        "new": handle_new_executions(),
        "processing": handle_processing_executions(),
        "ts": utc_now_iso(),
    }
    return results