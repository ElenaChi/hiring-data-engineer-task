import logging
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import argparse
import sys
import psycopg
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self):
        self.pg_params = {
            "host": os.getenv("PG_HOST", "localhost"),
            "port": os.getenv("PG_PORT", "5432"),
            "dbname": os.getenv("PG_DATABASE", "postgres"),
            "user": os.getenv("PG_USER", "postgres"),
        }
        self.ch_params = {
            "host": os.getenv("CH_HOST", "localhost"),
            "port": int(os.getenv("CH_PORT", "9000")),
            "database": os.getenv("CH_DATABASE", "default"),
            "user": os.getenv("CH_USER", "default"),
        }
        self.pg_conn = self._connect_pg()
        self.ch_client = self._connect_ch()


    def _connect_pg(self):
        try:
            conn = psycopg.connect(**self.pg_params)
            logger.info("Successfully connected to Postgres")
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            sys.exit(1)

    def _connect_ch(self):
        try:
            ch_client = Client(**self.ch_params)
            logger.info("Successfully connected to ClickHouse")
            return ch_client
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            sys.exit(1)
    
    def _get_min_impression_date(self):
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT MIN(created_at) FROM impressions")
            result = cur.fetchone()
            return result[0]
    
    def _get_min_click_date(self):
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT MIN(created_at) FROM clicks")
            result = cur.fetchone()
            return result[0]
    
    def reset(self):
        logger.info("Starting cleanup of ClickHouse tables")
        try:
            self.ch_client.execute("TRUNCATE TABLE IF EXISTS campaign_dimension")
            self.ch_client.execute("TRUNCATE TABLE IF EXISTS ad_events")
            logger.info("ClickHouse tables cleanup completed")
        except Exception as e:
            logger.warning(f"Error cleaning up one of ClickHouse tables: {str(e)}")
    
    def sync_ad_events(self):
        """Sync incrementally impressions and clicks from PostgreSQL to ClickHouse"""
        with self.pg_conn.cursor() as cursor:
        
            impression_date = self.ch_client.execute("SELECT MAX(inserted_at) FROM ad_events WHERE event_type = 'impression'")[0][0] or 0
            click_date = self.ch_client.execute("SELECT MAX(inserted_at) FROM ad_events WHERE event_type = 'click'")[0][0] or 0

            extract_sql = f"""
                SELECT id, campaign_id, 'impression' as event_type, created_at
                FROM impressions
                where created_at >= '{impression_date}'
                UNION
                SELECT id, campaign_id, 'click' as event_type, created_at
                FROM clicks
                where created_at >= '{click_date}'
            """

            cursor.execute(extract_sql)
            
            data = cursor.fetchall()
            records_processed = len(data)
            if data:
                self.ch_client.execute(
                    'INSERT INTO ad_events (event_id, campaign_id, event_type, created_at) VALUES',
                    data
                )
            
                logger.info(f"Successfully synced {records_processed} records")
            else:
                logger.info("No new records to sync")
    
    def sync_campaign_data(self):
        """Populate campaign_dimension from PostgreSQL to ClickHouse (denormalized campaign and advertiser data)"""
        with self.pg_conn.cursor() as pg_cur:
            pg_cur.execute("""
                SELECT c.id, c.name, c.bid, c.budget, a.name as advertiser_name,
                        c.end_date - c.start_date as campaign_duration_days,
                        CASE WHEN c.end_date > NOW() THEN 1 ELSE 0 END as is_active,
                        c.updated_at
                FROM campaign c
                JOIN advertiser a ON c.advertiser_id = a.id
            """)
            
            data = pg_cur.fetchall()
            records_processed = len(data)

            ch_data = [(
                        row[0],           # campaign_id
                        row[1],           # campaign_name
                        row[4],           # advertiser_name
                        float(row[2]),    # bid_amount (converted from Decimal)
                        float(row[3]),    # budget_amount (converted from Decimal)
                        row[5],           # campaign_duration_days
                        row[6],           # is_active
                        row[7]            # updated_at
                    ) for row in data]

            self.ch_client.execute(
                'INSERT INTO campaign_dimension VALUES',
                ch_data
            )
            
            logger.info(f"Successfully synced {records_processed} records")
    
    def update_daily_campaign_performance(self):
        """Update daily campaign performance metrics"""
        metrics = self.ch_client.execute("""
            SELECT 
                c.campaign_id,
                countIf(event_id, event_type = 'impression') as impressions_count,
                countIf(event_id, event_type = 'click') as clicks_count,
                countIf(event_id, event_type = 'impression') * c.bid_amount as spend_amount,
                CASE 
                    WHEN countIf(event_id, event_type = 'impression') > 0 
                    THEN countIf(event_id, event_type = 'click') / countIf(event_id, event_type = 'impression')
                    ELSE 0 
                END as ctr,
                CASE 
                    WHEN countIf(event_id, event_type = 'impression') > 0 
                    THEN (countIf(event_id, event_type = 'impression') * c.bid_amount) / (countIf(event_id, event_type = 'impression') / 1000.0)
                    ELSE 0 
                END as cpm
            FROM campaign_dimension AS c
            LEFT JOIN ad_events AS ae ON c.campaign_id = ae.campaign_id
            GROUP BY c.campaign_id, c.bid_amount;
        """)
        self.ch_client.execute(
            'INSERT INTO daily_campaign_performance (campaign_id, impressions_count, clicks_count, spend_amount, ctr, cpm) VALUES',
            metrics
        )
        logger.info(f"Successfully updated daily_campaign_performance metrics")
    
    def run(self, full_refresh=False, no_metrics_update=False):
        if full_refresh:
            self.reset()
        self.sync_ad_events()
        self.sync_campaign_data()
        if not no_metrics_update:
            self.update_daily_campaign_performance()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
         description="RetailMediaTool: PostgreSQL to ClickHouse", formatter_class=argparse.ArgumentDefaultsHelpFormatter
     )
    parser.add_argument("--full-refresh", action="store_true", help="Cleanup ClickHouse tables")
    parser.add_argument("--no_metrics_update", action="store_true", help="Update metric tables for reporting")
    args = parser.parse_args()
    pipeline = Pipeline()

    try:
        pipeline.run(full_refresh=args.full_refresh, no_metrics_update=args.no_metrics_update)
    except Exception as e:
        logger.error("Pipeline operation failed", error=str(e))
        sys.exit(1)
        