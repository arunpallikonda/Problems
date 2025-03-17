import threading
from datetime import datetime
import boto3
import psycopg
import time
from psycopg.rows import dict_row

IAM_ROLE = "arn:aws:iam::283005923416:role/service-role/AmazonRedshift-CommandsAccessRole-20250316T162253"
S3_PATH = "s3://source-s3copy/export/"
TABLE_NAME = "public.sales"

unload_done = threading.Event()
load_done = threading.Event()
unload_lock = threading.Lock()  # Ensure thread-safe query ID access
load_lock = threading.Lock()
unload_query_id = None  # Store UNLOAD query ID
load_query_id = None

def get_connection():
    """Establish a connection to Redshift."""
    conn = psycopg.connect(dbname="dev", user="", password="",
                           host="redshift-cluster-1.cq3emus4frhl.us-east-1.redshift.amazonaws.com",
                           row_factory=dict_row, sslmode="require", port=5439, client_encoding="utf8")
    conn.autocommit = True
    return conn


# def delete_all_s3_objects():
#     """Deletes all objects in the specified S3 prefix."""
#     s3_client = boto3.client('s3')
#     objects = s3_client.list_objects_v2(Bucket="source-s3copy", Prefix="export/")
#     if 'Contents' in objects:
#         for obj in objects['Contents']:
#             s3_client.delete_object(Bucket="source-s3copy", Key=obj['Key'])


def unload_to_s3():
    """Triggers UNLOAD command asynchronously in a separate thread and stores Query ID."""
    def run_unload():
        global unload_query_id  # Use the global variable to store query ID
        unload_query = f"""
        UNLOAD ('SELECT * FROM {TABLE_NAME}')
        TO '{S3_PATH}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT PARQUET
        ALLOWOVERWRITE;
        """
        print(f"UnloadThread - Before invoking unload to s3: {datetime.now()}")
        start_time = time.time()

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(unload_query)  # Run UNLOAD

                    # Fetch the latest UNLOAD query ID from `stl_query`
                    cur.execute("""
                        SELECT query 
                        FROM stl_query 
                        WHERE querytxt LIKE '%UNLOAD%' AND querytxt LIKE '%s3://source-s3copy/export/%' AND  querytxt LIKE '%{TABLE_NAME}%'
                        ORDER BY starttime DESC 
                        LIMIT 1;
                    """)

                    result = cur.fetchone()
                    if result:
                        with unload_lock:  # Ensure thread-safe update
                            unload_query_id = result['query']

            print(f"UnloadThread - ✅ UNLOAD command executed. Query ID: {unload_query_id}")
        except Exception as e:
            print(f"UnloadThread - ❌ UNLOAD failed: {str(e)}")
        finally:
            unload_done.set()  # Signal completion

    unload_thread = threading.Thread(target=run_unload)
    unload_thread.start()


def check_unload_status():
    """Waits for UNLOAD thread to complete and checks status for the exact Query ID."""
    print("🔍 Waiting for UNLOAD to complete...")
    unload_done.wait()  # Ensure UNLOAD thread finishes

    with unload_lock:
        query_id = unload_query_id  # Get the query ID from the thread

    if not query_id:
        print("❌ No UNLOAD Query ID found. Unable to track status.")
        return

    status_query = f"""
    SELECT query, 
           EXTRACT(EPOCH FROM (endtime - starttime)) AS execution_time_sec, 
           CASE 
               WHEN endtime IS NULL THEN 'Running'
               WHEN aborted = 1 THEN 'Failed'
               ELSE 'Completed'
           END AS status
    FROM stl_query 
    WHERE query = {query_id}
    ORDER BY starttime DESC
    LIMIT 1;
    """

    with get_connection() as conn:
        while True:
            with conn.cursor() as cur:
                cur.execute(status_query)
                result = cur.fetchone()

                if result:
                    print(f"🔄 UNLOAD Query ID: {result['query']}, Status: {result['status']}, Execution Time: {result['execution_time_sec']}s")

                    if result['status'] == 'Completed':
                        print("✅ UNLOAD operation completed successfully!")
                        break
                    elif result['status'] == 'Failed':
                        print("❌ UNLOAD operation failed!")
                        break
                else:
                    print(f"⚠️ No matching UNLOAD query found for Query ID: {query_id}")

            time.sleep(5)  # Poll every 5 seconds

# def check_unload_status():
#     """Checks the latest UNLOAD query's status in Redshift."""
#     print("Checking unload status")
#     status_query = f"""
#     SELECT q.query,
#            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) AS execution_time_sec,
#            CASE
#                WHEN q.endtime IS NULL THEN 'Running'
#                WHEN q.aborted = 1 THEN 'Failed'
#                ELSE 'Completed'
#            END AS status
#     FROM stl_query q
#     WHERE q.querytxt LIKE '%UNLOAD%' AND querytxt LIKE '%s3://source-s3copy/export/%' AND  querytxt LIKE '%{TABLE_NAME}%'
#     ORDER BY q.starttime DESC
#     LIMIT 1;
#     """
#
#     with get_connection() as conn:
#         while True:
#             with conn.cursor() as cur:
#                 cur.execute(status_query)
#                 result = cur.fetchone()
#
#                 if result:
#                     print(
#                         f"🔄 UNLOAD Query ID: {result['query']}, Status: {result['status']}, Execution Time: {result['execution_time_sec']}s")
#
#                     if result['status'] == 'Completed':
#                         print("✅ UNLOAD operation completed successfully!")
#                         break
#                     elif result['status'] == 'Failed':
#                         print("❌ UNLOAD operation failed!")
#                         break
#                 else:
#                     print("⚠️ No active or recent UNLOAD query found.")
#
#             time.sleep(5)  # Poll every 5 seconds
#         print(f"After unload is done to s3: {datetime.now()}")

def load_from_s3():
    """Triggers LOAD command asynchronously in a separate thread and stores Query ID."""
    def run_load():
        global load_query_id
        load_query = f"""
        COPY {TABLE_NAME}
        FROM '{S3_PATH}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT PARQUET;
        """
        print(f"loadThread - Before invoking LOAD from S3: {datetime.now()}")
        start_time = time.time()

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(load_query)

                    # Fetch LOAD query ID
                    cur.execute("""
                        SELECT query 
                        FROM stl_load_commits
                        WHERE filename LIKE '%s3://source-s3copy/export/%'
                        ORDER BY curtime DESC 
                        LIMIT 1;
                    """)
                    result = cur.fetchone()
                    if result:
                        with load_lock:
                            load_query_id = result['query']

            print(f"loadThread - ✅ LOAD executed. Query ID: {load_query_id}")
        except Exception as e:
            print(f"loadThread - ❌ LOAD failed: {str(e)}")
        finally:
            load_done.set()

    threading.Thread(target=run_load).start()

# def load_from_s3():
#     """Triggers LOAD command asynchronously in a separate thread."""
#
#     def run_load():
#         load_query = f"""
#         COPY {TABLE_NAME}
#         FROM '{S3_PATH}'
#         IAM_ROLE '{IAM_ROLE}'
#         FORMAT PARQUET;
#         """
#         print(f"Before invoking load from s3: {datetime.now()}")
#         start_time = time.time()
#
#         try:
#             with get_connection() as conn:
#                 with conn.cursor() as cur:
#                     cur.execute(load_query)  # Run LOAD in a separate thread
#                     cur.execute("""
#                         SELECT query
#                         FROM stl_query
#                         WHERE querytxt LIKE '%COPY%' AND querytxt LIKE '%s3://source-s3copy/export/%'
#                         ORDER BY starttime DESC
#                         LIMIT 1;
#                     """)
#                     result = cur.fetchone()
#                     if result:
#                         query_id = result['query']
#             print(f"✅ LOAD command executed. QueryId: {query_id}, TimeTaken: {time.time() - start_time}")
#         except Exception as e:
#             print(f"❌ LOAD failed: {str(e)}")
#         finally:
#             load_done.set()  # Signal completion
#
#     load_thread = threading.Thread(target=run_load)
#     load_thread.start()


# def check_load_status():
#     """Waits for the LOAD process to complete and checks status."""
#     status_query = """
#     SELECT q.query,
#            EXTRACT(EPOCH FROM (q.endtime - q.starttime)) AS execution_time_sec,
#            CASE
#                WHEN q.endtime IS NULL THEN 'Running'
#                WHEN q.aborted = 1 THEN 'Failed'
#                ELSE 'Completed'
#            END AS status
#     FROM stl_query q
#     WHERE q.querytxt LIKE '%COPY%' AND querytxt LIKE '%s3://source-s3copy/export/%'
#     ORDER BY q.starttime DESC
#     LIMIT 1;
#     """
#
#     print("🔍 Waiting for LOAD to complete...")
#     load_done.wait()  # Wait for thread to complete
#
#     with get_connection() as conn:
#         while True:
#             with conn.cursor() as cur:
#                 cur.execute(status_query)
#                 result = cur.fetchone()
#
#                 if result:
#                     print(
#                         f"🔄 LOAD Query ID: {result['query']}, Status: {result['status']}, Execution Time: {result['execution_time_sec']}s")
#
#                     if result['status'] == 'Completed':
#                         print("✅ LOAD operation completed successfully!")
#                         break
#                     elif result['status'] == 'Failed':
#                         print("❌ LOAD operation failed!")
#                         break
#                 else:
#                     print("⚠️ No active or recent LOAD query found.")
#
#             time.sleep(5)  # Poll every 5 seconds

def check_load_status():
    """Waits for the LOAD thread to complete and checks status."""
    print("🔍 Waiting for LOAD to complete...")
    load_done.wait()  # Ensure LOAD thread finishes

    with load_lock:
        query_id = load_query_id

    if not query_id:
        print("❌ No LOAD Query ID found. Unable to track status.")
        return

    while True:
        # Step 1: Check if COPY command is still running
        query_status_query = f"""
        SELECT query, 
               CASE 
                   WHEN endtime IS NULL THEN 'Running'
                   WHEN aborted = 1 THEN 'Failed'
                   ELSE 'Completed'
               END AS status
        FROM stl_query
        WHERE query = {query_id}
        ORDER BY starttime DESC
        LIMIT 1;
        """

        # Step 2: Check if any files were loaded successfully
        load_status_query = f"""
        SELECT COUNT(*) AS files_loaded
        FROM stl_load_commits
        WHERE query = {query_id};
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Check query execution status
                cur.execute(query_status_query)
                query_result = cur.fetchone()

                # Check if any files were loaded
                cur.execute(load_status_query)
                load_result = cur.fetchone()

                query_status = "Unknown"
                if query_result:
                    query_status = query_result["status"]

                files_loaded = 0
                if load_result:
                    files_loaded = load_result["files_loaded"]

                print(f"🔄 LOAD Query ID: {query_id}, Status: {query_status}, Files Loaded: {files_loaded}")

                if query_status in ["Completed", "Failed"]:
                    break  # Exit loop when COPY is done

        time.sleep(5)  # Poll every 5 seconds

if __name__ == "__main__":
    # delete_all_s3_objects()

    unload_to_s3()  # Step 1: Trigger UNLOAD (Runs in Background)
    check_unload_status()  # Step 2: Wait for UNLOAD and Check Status

    load_from_s3()  # Step 3: Trigger LOAD (Runs in Background)
    check_load_status()  # Step 4: Wait for LOAD and Check Status
