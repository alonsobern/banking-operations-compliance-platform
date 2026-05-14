import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def deploy_analytics():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cur = conn.cursor()
        
        sql_files = ['sql/analytics_queries.sql', 'sql/kpi_metrics.sql', 'sql/reporting_views.sql']
        
        for sql_file in sql_files:
            if os.path.exists(sql_file):
                print(f"Deploying {sql_file}...")
                with open(sql_file, 'r') as f:
                    cur.execute(f.read())
                conn.commit()
        
        print("DONE: All analytical and reporting views deployed.")
        
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    deploy_analytics()
