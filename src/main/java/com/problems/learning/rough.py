import json
import boto3
import requests
import os
import traceback
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
MWAA_ENV_NAME = os.environ.get("MWAA_ENV_NAME")
DAG_TO_TRIGGER = os.environ.get("DAG_ID")

def lambda_handler(event, context):
    logger.info("🔧 Starting MWAA DAG trigger Lambda execution")
    logger.info(f"📌 MWAA_ENV_NAME: {MWAA_ENV_NAME}")
    logger.info(f"📌 DAG_TO_TRIGGER: {DAG_TO_TRIGGER}")
    logger.info(f"📌 Event received: {json.dumps(event)}")

    try:
        # Step 1: Get CLI token and hostname
        logger.info("🔑 Fetching CLI token and hostname from MWAA")
        mwaa = boto3.client("mwaa")
        token_response = mwaa.create_cli_token(Name=MWAA_ENV_NAME)
        token = token_response["CliToken"]
        hostname = token_response["WebServerHostname"]
        base_url = f"https://{hostname}"
        logger.info(f"✅ CLI token received. Hostname: {hostname}")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Step 2: List all available DAGs
        dags_url = f"{base_url}/api/v1/dags"
        logger.info(f"📥 Listing DAGs from: {dags_url}")
        dags_response = requests.get(dags_url, headers=headers, verify=True)
        logger.info(f"📥 DAG list response status: {dags_response.status_code}")
        logger.info(f"📥 DAG list response body: {dags_response.text}")
        dags_response.raise_for_status()
        dags = dags_response.json().get("dags", [])

        logger.info("📋 Available DAGs:")
        for dag in dags:
            logger.info(f"   - {dag['dag_id']}")

        # Step 3: Validate DAG exists
        if not any(d["dag_id"] == DAG_TO_TRIGGER for d in dags):
            msg = f"❌ DAG '{DAG_TO_TRIGGER}' not found in environment '{MWAA_ENV_NAME}'"
            logger.error(msg)
            return {
                "statusCode": 404,
                "body": msg
            }

        # Step 4: Trigger the specified DAG
        dag_trigger_url = f"{base_url}/api/v1/dags/{DAG_TO_TRIGGER}/dagRuns"
        dag_run_id = f"manual__{context.aws_request_id}"
        payload = {
            "dag_run_id": dag_run_id,
            "conf": {}
        }

        logger.info(f"🚀 Triggering DAG via: {dag_trigger_url}")
        logger.info(f"🚀 Payload: {json.dumps(payload)}")

        trigger_response = requests.post(dag_trigger_url, headers=headers, json=payload, verify=True)
        logger.info(f"🚀 Trigger response status: {trigger_response.status_code}")
        logger.info(f"🚀 Trigger response body: {trigger_response.text}")
        trigger_response.raise_for_status()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"DAG '{DAG_TO_TRIGGER}' triggered successfully.",
                "dag_run_id": dag_run_id,
                "trigger_response": trigger_response.json()
            })
        }

    except Exception as e:
        logger.error("❗ Exception occurred", exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Internal error: {str(e)}"
        }
