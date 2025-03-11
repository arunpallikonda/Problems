import time
import psycopg
from psycopg.rows import dict_row

CHECK_INTERVAL = 30  # Check every 30 seconds
MAX_WAIT_TIME = 900  # 15 minutes

class RedshiftS3Loader:
    def __init__(self, host, db, user, password, iam_role):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.iam_role = iam_role

    def get_redshift_connection(self):
        """Returns a psycopg connection object."""
        return psycopg.connect(
            host=self.host,
            dbname=self.db,
            user=self.user,
            password=self.password,
            row_factory=dict_row
        )

    def initiate_unload_to_s3(self, schema_name, table_name, s3_staging_location):
        """Initiates UNLOAD command asynchronously."""
        s3_path = f"{s3_staging_location}{schema_name}/{table_name}/"
        
        unload_sql = f"""
        UNLOAD ('SELECT * FROM {schema_name}.{table_name}')
        TO '{s3_path}'
        IAM_ROLE '{self.iam_role}'
        FORMAT AS PARQUET;
        """

        try:
            with self.get_redshift_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(unload_sql)
                    print(f"UNLOAD initiated for {schema_name}.{table_name} to {s3_path}")
        except Exception as e:
            print(f"Error initiating UNLOAD: {e}")

    def check_unload_status(self, s3_staging_location):
        """Checks if the UNLOAD operation for the specific S3 location and IAM role is completed."""
        check_sql = f"""
        SELECT pid, query, state, elapsed 
        FROM STL_QUERY 
        WHERE querytxt ILIKE 'UNLOAD%' 
          AND querytxt ILIKE '%{s3_staging_location}%' 
          AND querytxt ILIKE '%{self.iam_role}%'
        ORDER BY starttime DESC 
        LIMIT 1;
        """

        start_time = time.time()
        
        while time.time() - start_time < MAX_WAIT_TIME:
            try:
                with self.get_redshift_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(check_sql)
                        result = cur.fetchone()
                        
                        if result:
                            state = result["state"]
                            print(f"UNLOAD Status: {state} for {s3_staging_location}")
                            
                            if state.lower() == "completed":
                                print(f"UNLOAD to {s3_staging_location} finished successfully.")
                                return True
                            elif state.lower() in ("failed", "error"):
                                print(f"UNLOAD to {s3_staging_location} failed.")
                                return False
                        else:
                            print(f"No matching UNLOAD operation found for {s3_staging_location}.")
                            return False
            except Exception as e:
                print(f"Error checking UNLOAD status: {e}")
            
            time.sleep(CHECK_INTERVAL)

        print(f"UNLOAD to {s3_staging_location} timed out.")
        return False

    def initiate_load_from_s3(self, schema_name, table_name, s3_staging_location):
        """Initiates COPY command asynchronously."""
        s3_path = f"{s3_staging_location}{schema_name}/{table_name}/"

        copy_sql = f"""
        COPY {schema_name}.{table_name}
        FROM '{s3_path}'
        IAM_ROLE '{self.iam_role}'
        FORMAT AS PARQUET;
        """

        try:
            with self.get_redshift_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(copy_sql)
                    print(f"COPY initiated for {schema_name}.{table_name} from {s3_path}")
        except Exception as e:
            print(f"Error initiating COPY: {e}")

    def check_load_status(self, s3_staging_location):
        """Checks if the COPY operation for the specific S3 location and IAM role is completed."""
        check_sql = f"""
        SELECT pid, query, state, elapsed 
        FROM STL_QUERY 
        WHERE querytxt ILIKE 'COPY%' 
          AND querytxt ILIKE '%{s3_staging_location}%'
          AND querytxt ILIKE '%{self.iam_role}%'
        ORDER BY starttime DESC 
        LIMIT 1;
        """

        start_time = time.time()

        while time.time() - start_time < MAX_WAIT_TIME:
            try:
                with self.get_redshift_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(check_sql)
                        result = cur.fetchone()
                        
                        if result:
                            state = result["state"]
                            print(f"COPY Status: {state} for {s3_staging_location}")
                            
                            if state.lower() == "completed":
                                print(f"COPY from {s3_staging_location} finished successfully.")
                                return True
                            elif state.lower() in ("failed", "error"):
                                print(f"COPY from {s3_staging_location} failed.")
                                return False
                        else:
                            print(f"No matching COPY operation found for {s3_staging_location}.")
                            return False
            except Exception as e:
                print(f"Error checking COPY status: {e}")

            time.sleep(CHECK_INTERVAL)

        print(f"COPY from {s3_staging_location} timed out.")
        return False
