import argparse
import psycopg2, os
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def create_schema(host, database, user, password):
    print(f"Creating schema for {database} on {host} with user {user}")
    conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=5432)
    cur = conn.cursor()

    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS benchmark_results (
        id SERIAL PRIMARY KEY,
        api TEXT,
        branch TEXT,
        commit TEXT,
        commit_time TIMESTAMP,
        run_time TIMESTAMP,
        dataset_file TEXT,
        ef INTEGER,
        ef_construction INTEGER,
        heap_alloc_bytes BIGINT,
        heap_inuse_bytes BIGINT,
        heap_sys_bytes BIGINT,
        host TEXT,
        import_time FLOAT,
        limit_val INTEGER,
        max_connections INTEGER,
        mean_latency FLOAT,
        p99_latency FLOAT,
        parallelization INTEGER,
        qps FLOAT,
        recall FLOAT,
        run_id TEXT,
        shards INTEGER
    );
                                 
    CREATE INDEX IF NOT EXISTS idx_benchmark_results_commit_time ON benchmark_results (commit_time);
    """)

    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

    print("Schema created successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create PostgreSQL schema for benchmark metrics")
    parser.add_argument("--host", required=False, help="Database host")
    parser.add_argument("--database", required=False, help="Database name")
    parser.add_argument("--user", required=False, help="Database user")
    parser.add_argument("--password", required=False, help="Database password")

    args = parser.parse_args()

    host = args.host or os.environ.get('DB_HOST')
    database = args.database or os.environ.get('DB_NAME')
    user = args.user or os.environ.get('DB_USER')
    password = args.password or os.environ.get('DB_PASS')

    create_schema(host, database, user, password)
