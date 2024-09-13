import argparse
import json
import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def upload_results(host, database, user, password, directory, delete_files):
    conn_string = f"dbname={database} user={user} password={password} host={host}"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    insert_query = sql.SQL("""
    INSERT INTO benchmark_results (
        api, branch, commit, dataset_file, ef, ef_construction, heap_alloc_bytes,
        heap_inuse_bytes, heap_sys_bytes, host, import_time, limit_val, max_connections,
        mean_latency, p99_latency, parallelization, qps, recall, run_id, shards,
        commit_time, run_time
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """)

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as f:
                data = json.load(f)
                for result in data:
                    cur.execute(insert_query, (
                        result['api'], result['branch'], result['commit'], result['dataset_file'],
                        result['ef'], result['efConstruction'], result['heap_alloc_bytes'],
                        result['heap_inuse_bytes'], result['heap_sys_bytes'], result['host'],
                        result['importTime'], result['limit'], result['maxConnections'],
                        result['meanLatency'], result['p99Latency'], result['parallelization'],
                        result['qps'], result['recall'], result['run_id'], result['shards'],
                        result['commit_time'], result['run_time']
                    ))
            
            if delete_files:
                os.remove(file_path)
                print(f"Uploaded and deleted: {filename}")
            else:
                print(f"Uploaded: {filename}")

    conn.commit()
    cur.close()
    conn.close()

    print("Results uploaded successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload benchmark results to PostgreSQL")
    parser.add_argument("--host", help="Database host (can also be set via DB_HOST environment variable)")
    parser.add_argument("--database", help="Database name (can also be set via DB_NAME environment variable)")
    parser.add_argument("--user", help="Database user (can also be set via DB_USER environment variable)")
    parser.add_argument("--password", help="Database password (can also be set via DB_PASS environment variable)")
    parser.add_argument("--directory", required=True, help="Directory containing JSON result files")
    parser.add_argument("--delete", action="store_true", help="Delete JSON files after uploading")

    args = parser.parse_args()

    host = args.host or os.environ.get('DB_HOST')
    database = args.database or os.environ.get('DB_NAME')
    user = args.user or os.environ.get('DB_USER')
    password = args.password or os.environ.get('DB_PASS')

    if not all([host, database, user, password]):
        parser.error("All database connection details must be provided either via arguments or environment variables")

    upload_results(host, database, user, password, args.directory, args.delete)