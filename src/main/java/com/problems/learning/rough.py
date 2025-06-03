import json
import boto3
import requests
import os
import traceback

MWAA_ENV_NAME = os.environ.get("MWAA_ENV_NAME")  # Lambda environment variable
DAG_TO_TRIGGER = os.environ.get("DAG_ID")        # Lambda environment variable

def lambda_handler(event, context):
    print("🔧 Starting MWAA DAG trigger Lambda execution")
    print(f"📌 MWAA_ENV_NAME: {MWAA_ENV_NAME}")
    print(f"📌 DAG_TO_TRIGGER: {DAG_TO_TRIGGER}")
    print(f"📌 Event received: {json.dumps(event)}")

    try:
        # Step 1: Get CLI token and hostname
        print("🔑 Fetching CLI token and hostname from MWAA")
        mwaa = boto3.client("mwaa")
        token_response = mwaa.create_cli_token(Name=MWAA_ENV_NAME)
        token = token_response["CliToken"]
        hostname = token_response["WebServerHostname"]
        base_url = f"https://{hostname}"
        print(f"✅ CLI token received. Hostname: {hostname}")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Step 2: List all available DAGs
        dags_url = f"{base_url}/api/v1/dags"
        print(f"📥 Listing DAGs from: {dags_url}")
        dags_response = requests.get(dags_url, headers=headers, verify=True)
        print(f"📥 DAG list response status: {dags_response.status_code}")
        print(f"📥 DAG list response body: {dags_response.text}")
        dags_response.raise_for_status()
        dags = dags_response.json().get("dags", [])

        print("📋 Available DAGs:")
        for dag in dags:
            print(f"   - {dag['dag_id']}")

        # Step 3: Validate DAG exists
        if not any(d["dag_id"] == DAG_TO_TRIGGER for d in dags):
            error_msg = f"❌ DAG '{DAG_TO_TRIGGER}' not found in environment '{MWAA_ENV_NAME}'"
            print(error_msg)
            return {
                "statusCode": 404,
                "body": error_msg
            }

        # Step 4: Trigger the specified DAG
        dag_trigger_url = f"{base_url}/api/v1/dags/{DAG_TO_TRIGGER}/dagRuns"
        dag_run_id = f"manual__{context.aws_request_id}"
        payload = {
            "dag_run_id": dag_run_id,
            "conf": {}  # Add conf if needed
        }

        print(f"🚀 Triggering DAG via: {dag_trigger_url}")
        print(f"🚀 Payload: {json.dumps(payload)}")

        trigger_response = requests.post(dag_trigger_url, headers=headers, json=payload, verify=True)
        print(f"🚀 Trigger response status: {trigger_response.status_code}")
        print(f"🚀 Trigger response body: {trigger_response.text}")
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
        print("❗ An exception occurred:")
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": f"Internal error: {str(e)}"
        }
