"""
Netflix Crawler Airflow DAG
DAG để lập lịch chạy Netflix movie crawler hàng ngày
"""

import os
import sys
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Thêm đường dẫn đến thư mục scripts vào sys.path
SCRIPT_DIR = "/opt/crawler/scripts"
sys.path.insert(0, SCRIPT_DIR)

# Import các module từ scripts
try:
    from config import Config
    from netflix_crawler import NetflixDataCrawler, setup_logging
except ImportError as e:
    logging.error(f"Error importing modules from {SCRIPT_DIR}: {e}")
    raise

# DAG Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Khởi tạo DAG
dag = DAG(
    'netflix_daily_crawler',
    default_args=default_args,
    description='Crawl Netflix movie data daily',
    schedule_interval='0 1 * * *',  # Chạy lúc 1:00 AM mỗi ngày
    catchup=False,
    tags=['netflix', 'crawler', 'movies'],
)

# Task Functions
def run_netflix_crawler(**context):
    """
    Chạy Netflix crawler và thu thập dữ liệu mới
    """
    # Setup logging
    logger = setup_logging(log_dir='/opt/crawler/data/logs')
    logging.info("Starting Netflix crawler from Airflow DAG")
    
    try:
        # Load configuration
        config = Config(env_path='/opt/crawler/.env')
        
        # Create crawler
        crawler = NetflixDataCrawler(config)
        
        # Run crawl
        result = crawler.crawl_daily_netflix_data(
            num_pages_per_source=5,
            max_movies=100
        )
        
        # Push metadata to XCom for downstream tasks
        context['ti'].xcom_push(key='crawl_timestamp', 
                               value=result['crawl_summary']['crawl_timestamp'])
        context['ti'].xcom_push(key='movies_count', 
                               value=result['crawl_summary']['successful_crawls'])
        context['ti'].xcom_push(key='failed_count', 
                               value=result['crawl_summary']['failed_crawls'])
        
        return result['crawl_summary']['successful_crawls']
        
    except Exception as e:
        logging.error(f"Error in Netflix crawler: {e}")
        raise

def clear_old_cache(**context):
    """
    Xóa cache cũ để tiết kiệm không gian
    """
    from cache_manager import CacheManager
    
    logging.info("Clearing old cache files")
    
    try:
        # Load configuration
        config = Config(env_path='/opt/crawler/.env')
        
        # Tạo cache manager
        cache = CacheManager(config)
        
        # Xóa cache cũ hơn 7 ngày
        deleted = cache.clear(older_than=7*86400)  # 7 days in seconds
        
        logging.info(f"Cleared {deleted} old cache files")
        return deleted
        
    except Exception as e:
        logging.error(f"Error clearing cache: {e}")
        raise

# Define Tasks
crawl_task = PythonOperator(
    task_id='crawl_netflix_data',
    python_callable=run_netflix_crawler,
    provide_context=True,
    dag=dag,
)

clear_cache_task = PythonOperator(
    task_id='clear_old_cache',
    python_callable=clear_old_cache,
    provide_context=True,
    dag=dag,
)

# Log results task
log_results = BashOperator(
    task_id='log_crawl_results',
    bash_command='echo "Netflix crawl completed at $(date). '
                'Crawled {{ ti.xcom_pull(task_ids="crawl_netflix_data", key="movies_count") }} movies. '
                'Failed: {{ ti.xcom_pull(task_ids="crawl_netflix_data", key="failed_count") }} movies."',
    dag=dag,
)

# Define task dependencies
crawl_task >> clear_cache_task >> log_results