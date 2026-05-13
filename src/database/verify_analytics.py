import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def verify_analytics():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cur = conn.cursor()
        
        views = [
            'dashboard_daily_stats',
            'dashboard_segment_analysis',
            'dashboard_branch_ranking',
            'dashboard_peak_hours',
            'dashboard_compliance_alerts',
            'dashboard_channel_usage',
            'dashboard_merchant_insights'
        ]
        
        print(f"{'Analytical View':<30} | {'Status':<10}")
        print("-" * 45)
        
        for view in views:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {view}")
                count = cur.fetchone()[0]
                print(f"{view:<30} | OK ({count:,} rows)")
            except Exception as e:
                print(f"{view:<30} | FAILED ({e})")
                conn.rollback()
        
    except Exception as e:
        print(f"Connection Error: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    verify_analytics()
