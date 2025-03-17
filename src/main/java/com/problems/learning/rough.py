from datetime import datetime
import boto3
import psycopg
import time
from psycopg.rows import dict_row

IAM_ROLE = "arn:aws:iam::283005923416:role/service-role/AmazonRedshift-CommandsAccessRole-20250316T162253"
S3_PATH = "s3://source-s3copy/export/"
TABLE_NAME = "public.users"


def get_connection():
    """Establish a connection to Redshift."""
    conn = psycopg.connect(dbname="dev", user="", password="",
                           host="redshift-cluster-1.cq3emus4frhl.us-east-1.redshift.amazonaws.com",
                           row_factory=dict_row, sslmode="require", port=5439, client_encoding="utf8")
    conn.autocommit = True
    return conn


def delete_all_s3_objects():
    """Deletes all objects in the specified S3 prefix."""
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket="source-s3copy", Prefix="export/")
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3_client.delete_object(Bucket="source-s3copy", Key=obj['Key'])
        print("✅ All objects deleted successfully.")


def unload_to_s3():
    """Triggers UNLOAD command asynchronously (non-blocking)."""
    unload_query = f"""
    UNLOAD ('SELECT * FROM {TABLE_NAME}')
    TO '{S3_PATH}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT PARQUET
    ALLOWOVERWRITE;
    """

    print(f"Before invoking unload to s3: {datetime.now()}")
    start_time = time.time()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(unload_query)  # Trigger UNLOAD

    print(f"✅ UNLOAD command triggered asynchronously. TimeTakenToExecute: {time.time() - start_time}")


def check_unload_status():
    """Checks the latest UNLOAD query's status in Redshift."""
    status_query = """
    SELECT q.query, 
           EXTRACT(EPOCH FROM (q.endtime - q.starttime)) AS execution_time_sec, 
           CASE 
               WHEN q.endtime IS NULL THEN 'Running'
               WHEN q.aborted = 1 THEN 'Failed'
               ELSE 'Completed'
           END AS status
    FROM stl_query q
    WHERE q.querytxt LIKE '%UNLOAD%' AND querytxt LIKE '%s3://source-s3copy/export/%'
    ORDER BY q.starttime DESC
    LIMIT 1;
    """

    with get_connection() as conn:
        while True:
            with conn.cursor() as cur:
                cur.execute(status_query)
                result = cur.fetchone()

                if result:
                    print(
                        f"🔄 UNLOAD Query ID: {result['query']}, Status: {result['status']}, Execution Time: {result['execution_time_sec']}s")

                    if result['status'] == 'Completed':
                        print("✅ UNLOAD operation completed successfully!")
                        break
                    elif result['status'] == 'Failed':
                        print("❌ UNLOAD operation failed!")
                        break
                else:
                    print("⚠️ No active or recent UNLOAD query found.")

            time.sleep(5)  # Poll every 5 seconds
        print(f"After unload is done to s3: {datetime.now()}")


def load_from_s3():
    """Loads data from S3 (Parquet) into Redshift."""
    load_query = f"""
    COPY {TABLE_NAME}
    FROM '{S3_PATH}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT PARQUET;
    """

    print(f"Before invoking load from s3: {datetime.now()}")
    start_time = time.time()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(load_query)  # Trigger COPY

    print(f"✅ LOAD command triggered asynchronously. TimeTakenToExecute: {time.time() - start_time}")


def check_load_status():
    """Checks the latest COPY (LOAD) query's status in Redshift."""
    status_query = """
    SELECT query, 
           EXTRACT(EPOCH FROM (endtime - starttime)) AS execution_time_sec, 
           CASE 
               WHEN endtime IS NULL THEN 'Running'
               WHEN aborted = 1 THEN 'Failed'
               ELSE 'Completed'
           END AS status
    FROM stl_load_commits
    ORDER BY starttime DESC
    LIMIT 1;
    """

    with get_connection() as conn:
        while True:
            with conn.cursor() as cur:
                cur.execute(status_query)
                result = cur.fetchone()

                if result:
                    print(
                        f"🔄 LOAD Query ID: {result['query']}, Status: {result['status']}, Execution Time: {result['execution_time_sec']}s")

                    if result['status'] == 'Completed':
                        print("✅ LOAD operation completed successfully!")
                        break
                    elif result['status'] == 'Failed':
                        print("❌ LOAD operation failed!")
                        break
                else:
                    print("⚠️ No active or recent LOAD query found.")

            time.sleep(5)  # Poll every 5 seconds
        print(f"After load is done from s3: {datetime.now()}")


# 🚀 Main Execution
if __name__ == "__main__":
    delete_all_s3_objects()
    unload_to_s3()  # Step 1: Trigger UNLOAD (Non-blocking)
    check_unload_status()  # Step 2: Monitor UNLOAD status
    load_from_s3()  # Step 3: Load data back into Redshift
    check_load_status()  # Step 4: Monitor LOAD status
