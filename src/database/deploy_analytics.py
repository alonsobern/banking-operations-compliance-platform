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
        
        with open('sql/analytics_queries.sql', 'r') as f:
            sql = f.read()
            cur.execute(sql)
            
        conn.commit()
        print("✅ Analytics views deployed successfully.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    deploy_analytics()
