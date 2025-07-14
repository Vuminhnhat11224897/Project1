"""
Netflix Data Crawler
Crawler thu thập dữ liệu phim từ TMDB API
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import hashlib
from logging.handlers import RotatingFileHandler
# Thêm các import cần thiết
try:
    # Import từ thư mục hiện tại khi chạy trực tiếp
    from config import Config
    from tmdb_client import TMDBClient
    from cache_manager import CacheManager
except ImportError:
    # Import đường dẫn tuyệt đối khi chạy trong Docker/Airflow
    import sys
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, script_dir)
    from config import Config
    from tmdb_client import TMDBClient
    from cache_manager import CacheManager


class NetflixDataCrawler:
    """
    Crawler chính để thu thập dữ liệu phim từ TMDB API.
    """
    
    def __init__(self, config=None):
        """
        Khởi tạo Netflix data crawler.
        
        Args:
            config: Config object hoặc None để tạo mới từ mặc định
        """
        # Sử dụng cấu hình được cung cấp hoặc tạo mới từ mặc định
        self.config = config if config else Config()
        
        # Khởi tạo TMDB client
        self.tmdb = TMDBClient(self.config)
        
        # Khởi tạo cache manager
        cache_dir = self.config.get("cache_dir", "cache")
        os.makedirs(cache_dir, exist_ok=True)
        self.cache = CacheManager(cache_dir)
        
        # Đường dẫn thư mục dữ liệu
        self.raw_data_dir = self.config.get("raw_data_dir")
        self.processed_data_dir = self.config.get("processed_data_dir")
        
        # Đảm bảo thư mục tồn tại
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        # Lấy các tham số cấu hình
        self.batch_size = self.config.get("batch_size", 25)
        self.max_workers = self.config.get("max_workers", 5)
        self.max_retries = self.config.get("max_retries", 3)
        self.retry_backoff = self.config.get("retry_backoff", 1.5)
        self.rate_limit_wait = self.config.get("rate_limit_wait", 0.25)
        
        logging.info(f"Netflix Data Crawler đã khởi tạo: raw_data_dir={self.raw_data_dir}")
    
    def with_retry(self, func, *args, max_retries=None, backoff_factor=None, **kwargs):
        """
        Thực hiện API call với cơ chế retry tự động
        
        Args:
            func: Phương thức API cần gọi
            max_retries: Số lần thử lại tối đa (None = dùng cấu hình)
            backoff_factor: Hệ số tăng thời gian chờ (None = dùng cấu hình)
            
        Returns:
            Kết quả từ API call hoặc None nếu tất cả các lần thử đều thất bại
        """
        if max_retries is None:
            max_retries = self.max_retries
            
        if backoff_factor is None:
            backoff_factor = self.retry_backoff
            
        retries = 0
        last_exception = None
        
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                wait_time = backoff_factor * (2 ** retries)
                logging.warning(f"API call failed: {e}. Retrying in {wait_time:.1f}s ({retries+1}/{max_retries})")
                time.sleep(wait_time)
                retries += 1
        
        logging.error(f"API call failed after {max_retries} retries: {last_exception}")
        return None
    
    def crawl_movie_list(self, list_type: str, num_pages: int = 5) -> List[Dict]:
        """
        Crawl danh sách phim theo loại (popular, trending, netflix, etc).
        
        Args:
            list_type: Loại danh sách phim (netflix, popular, trending_day, trending_week, top_rated)
            num_pages: Số trang để crawl
            
        Returns:
            List[Dict]: Danh sách phim đã crawl
        """
        logging.info(f"Crawling {list_type} movies - {num_pages} pages")
        
        all_movies = []
        
        for page in range(1, num_pages + 1):
            logging.info(f"Crawling {list_type} page {page}/{num_pages}")
            
            if list_type == "popular":
                movies = self.with_retry(self.tmdb.get_popular_movies, page)
            elif list_type == "top_rated":
                movies = self.with_retry(self.tmdb.get_top_rated_movies, page)
            elif list_type == "now_playing":
                movies = self.with_retry(self.tmdb.get_now_playing_movies, page)
            elif list_type == "netflix":
                movies = self.with_retry(self.tmdb.get_netflix_movies, page)
            elif list_type == "trending_day":
                movies = self.with_retry(self.tmdb.get_trending_movies, "day", page)
            elif list_type == "trending_week":
                movies = self.with_retry(self.tmdb.get_trending_movies, "week", page)
            else:
                logging.warning(f"Unknown list type: {list_type}")
                continue
            
            if movies and 'results' in movies:
                for movie in movies['results']:
                    movie['source_list'] = list_type
                    movie['page'] = page
                all_movies.extend(movies['results'])
                logging.info(f"Found {len(movies['results'])} movies")
            
            time.sleep(self.rate_limit_wait)  # Rate limiting
        
        logging.info(f"Total {list_type} movies: {len(all_movies)}")
        return all_movies
    
    def crawl_movie_details(self, movie_id: int) -> Dict:
        """
        Crawl thông tin chi tiết của một phim với cache.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Thông tin chi tiết của phim
        """
        logging.info(f"Crawling details for movie ID: {movie_id}")
        
        # Kiểm tra cache
        cache_key = f"movie_details_{movie_id}"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            logging.info(f"Using cached data for movie ID: {movie_id}")
            return cached_data
        
        movie_data = {
            'movie_id': movie_id,
            'crawl_timestamp': datetime.now().isoformat(),
            'details': None,
            'credits': None,
            'keywords': None,
            'videos': None,
            'reviews': None,
            'similar': None
        }
        
        # 1. Movie details
        movie_details = self.with_retry(self.tmdb.get_movie_details, movie_id)
        if movie_details:
            movie_data['details'] = movie_details
            logging.info(f"✓ Details: {movie_details.get('title', 'N/A')}")
        else:
            logging.warning(f"✗ No details found for movie ID: {movie_id}")
            return movie_data  # Return early if we can't get basic details
        
        # 2. Credits (cast & crew)
        credits = self.with_retry(self.tmdb.get_movie_credits, movie_id)
        if credits:
            movie_data['credits'] = credits
            cast_count = len(credits.get('cast', []))
            crew_count = len(credits.get('crew', []))
            logging.info(f"✓ Credits: {cast_count} cast, {crew_count} crew")
        
        # 3. Keywords
        keywords = self.with_retry(self.tmdb.get_movie_keywords, movie_id)
        if keywords:
            movie_data['keywords'] = keywords
            keyword_count = len(keywords.get('keywords', []))
            logging.info(f"✓ Keywords: {keyword_count} keywords")
        
        # 4. Videos/Trailers
        videos = self.with_retry(self.tmdb.get_movie_videos, movie_id)
        if videos:
            movie_data['videos'] = videos
            video_count = len(videos.get('results', []))
            logging.info(f"✓ Videos: {video_count} videos")
        
        # 5. Reviews
        reviews = self.with_retry(self.tmdb.get_movie_reviews, movie_id)
        if reviews:
            movie_data['reviews'] = reviews
            review_count = len(reviews.get('results', []))
            logging.info(f"✓ Reviews: {review_count} reviews")
        
        # 6. Similar movies
        similar = self.with_retry(self.tmdb.get_movie_similar, movie_id)
        if similar:
            movie_data['similar'] = similar
            similar_count = len(similar.get('results', []))
            logging.info(f"✓ Similar: {similar_count} similar movies")
        
        # Lưu vào cache nếu có dữ liệu cơ bản
        if movie_data['details']:
            self.cache.set(cache_key, movie_data)
        
        return movie_data
    
    def save_movie_batch(self, movies, timestamp, batch_index):
        """
        Lưu một batch phim vào file riêng biệt
        
        Args:
            movies: Danh sách phim cần lưu
            timestamp: Timestamp cho tên file
            batch_index: Chỉ số batch
            
        Returns:
            str: Đường dẫn đến file đã lưu
        """
        if not movies:
            return None
            
        batch_file = os.path.join(self.raw_data_dir, 
                                  f"detailed_movies_batch_{batch_index}_{timestamp}.json")
        self.save_json(movies, batch_file)
        logging.info(f"Saved batch {batch_index}: {len(movies)} movies")
        return batch_file
    
    def crawl_daily_netflix_data(self, 
                                num_pages_per_source: int = None,
                                max_movies: int = None,
                                sources: List[str] = None,
                                resume_from: str = None) -> Dict:
        """
        Crawl dữ liệu hàng ngày từ nhiều nguồn.
        
        Args:
            num_pages_per_source: Số trang mỗi nguồn (None = dùng cấu hình)
            max_movies: Số phim tối đa để crawl chi tiết (None = dùng cấu hình)
            sources: Danh sách nguồn để crawl (None = dùng tất cả nguồn được bật trong cấu hình)
            resume_from: Đường dẫn file lưu danh sách movie_ids đã thất bại để thử lại
            
        Returns:
            Dict: Kết quả crawl với summary và danh sách phim
        """
        # Sử dụng cấu hình mặc định nếu không được chỉ định
        if num_pages_per_source is None:
            num_pages_per_source = self.config.get("max_pages_per_source", 5)
            
        if max_movies is None:
            max_movies = self.config.get("max_movies_per_day", 100)
        
        # Nếu không chỉ định nguồn, sử dụng tất cả nguồn được bật trong cấu hình
        if sources is None:
            sources = []
            data_sources = self.config.get("data_sources", {})
            for source, config in data_sources.items():
                if config.get("enabled", True):
                    sources.append(source)
                    
        logging.info(f"Starting daily Netflix data crawl:")
        logging.info(f"- Sources: {sources}")
        logging.info(f"- Pages per source: {num_pages_per_source}")
        logging.info(f"- Max movies for detailed crawl: {max_movies}")
        
        # Tạo timestamp cho tên file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Nếu là chế độ tiếp tục, load movie IDs đã thất bại
        selected_movies = []
        if resume_from and os.path.exists(resume_from):
            try:
                with open(resume_from, 'r') as f:
                    failed_data = json.load(f)
                    selected_movies = failed_data.get('failed_movie_ids', [])
                    logging.info(f"Resuming crawl for {len(selected_movies)} previously failed movies")
            except Exception as e:
                logging.error(f"Error loading resume file {resume_from}: {e}")
                # Tiếp tục với quy trình bình thường
                selected_movies = []
        
        # Nếu không phải chế độ resume hoặc resume thất bại, thu thập movie IDs mới
        if not selected_movies:
            # 1. Thu thập movie IDs từ các nguồn khác nhau
            all_movie_ids = set()
            movie_sources = {}
            
            for source in sources:
                try:
                    # Điều chỉnh số trang theo cấu hình nguồn cụ thể nếu có
                    source_pages = self.config.get_nested("data_sources", source, "pages", default=num_pages_per_source)
                    movies = self.crawl_movie_list(source, source_pages)
                    
                    for movie in movies:
                        movie_id = movie['id']
                        all_movie_ids.add(movie_id)
                        
                        if movie_id not in movie_sources:
                            movie_sources[movie_id] = []
                        movie_sources[movie_id].append(source)
                        
                except Exception as e:
                    logging.error(f"Error crawling {source}: {e}")
                    continue
            
            logging.info(f"Collected {len(all_movie_ids)} unique movies from {len(sources)} sources")
            
            # 2. Lưu summary danh sách phim
            movie_list_summary = {
                'crawl_date': datetime.now().isoformat(),
                'total_unique_movies': len(all_movie_ids),
                'sources': sources,
                'pages_per_source': num_pages_per_source,
                'movie_sources': movie_sources
            }
            
            summary_file = os.path.join(self.raw_data_dir, f"movie_list_summary_{timestamp}.json")
            self.save_json(movie_list_summary, summary_file)
            
            # Giới hạn số lượng phim để crawl chi tiết
            selected_movies = list(all_movie_ids)[:max_movies]
        
        logging.info(f"Crawling detailed info for {len(selected_movies)} movies...")
        
        # Khởi tạo file dataset cho toàn bộ kết quả
        dataset_header = {
            'crawl_summary': {
                'crawl_date': datetime.now().isoformat(),
                'crawl_timestamp': timestamp,
                'total_movies_attempted': len(selected_movies),
                'sources': sources if not resume_from else "resume"
            },
            'movies': []
        }
        
        final_file = os.path.join(self.raw_data_dir, f"netflix_raw_dataset_{timestamp}.json")
        self.save_json(dataset_header, final_file)
        
        detailed_movies = []
        failed_movies = []
        batch_buffer = []
        
        # 3. Crawl thông tin chi tiết với xử lý song song
        max_workers = min(self.max_workers, len(selected_movies))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_id = {executor.submit(self.crawl_movie_details, movie_id): movie_id for movie_id in selected_movies}
            
            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_id), 1):
                movie_id = future_to_id[future]
                try:
                    movie_data = future.result()
                    
                    if movie_data['details']:  # Only save if we got basic details
                        if not resume_from:  # Nếu không phải resume, thêm thông tin nguồn
                            movie_data['sources'] = movie_sources.get(movie_id, [])
                        detailed_movies.append(movie_data)
                        batch_buffer.append(movie_data)
                    else:
                        if movie_id not in failed_movies:  # Tránh thêm trùng lặp
                            failed_movies.append(movie_id)
                    
                    # Progress save every batch_size movies
                    if len(batch_buffer) >= self.batch_size or i == len(selected_movies):
                        batch_file = self.save_movie_batch(batch_buffer, timestamp, i // self.batch_size)
                        batch_buffer = []  # Reset buffer
                        
                except Exception as e:
                    logging.error(f"Error crawling movie {movie_id}: {e}")
                    if movie_id not in failed_movies:  # Tránh thêm trùng lặp
                        failed_movies.append(movie_id)
                    continue
        
        # 4. Lưu kết quả cuối cùng
        final_results = {
            'crawl_summary': {
                'crawl_date': datetime.now().isoformat(),
                'crawl_timestamp': timestamp,
                'total_movies_attempted': len(selected_movies),
                'successful_crawls': len(detailed_movies),
                'failed_crawls': len(failed_movies),
                'failed_movie_ids': failed_movies
            },
            'movies': detailed_movies
        }
        
        self.save_json(final_results, final_file)
        
        # Lưu riêng danh sách phim thất bại để có thể retry sau
        if failed_movies:
            failed_file = os.path.join(self.raw_data_dir, f"failed_movies_{timestamp}.json")
            self.save_json({'failed_movie_ids': failed_movies}, failed_file)
            logging.info(f"Saved {len(failed_movies)} failed movie IDs to {failed_file}")
        
        logging.info(f"CRAWL COMPLETED!")
        logging.info(f"- Successfully crawled: {len(detailed_movies)} movies")
        logging.info(f"- Failed: {len(failed_movies)} movies")
        logging.info(f"- Saved to: {final_file}")
        
        return final_results
    
    def save_json(self, data, filepath):
        """
        Lưu dữ liệu vào file JSON với xử lý lỗi.
        
        Args:
            data: Dữ liệu để lưu
            filepath: Đường dẫn file để lưu
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Đảm bảo thư mục tồn tại
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Lấy kích thước file
            file_size = os.path.getsize(filepath)
            logging.info(f"Saved to {filepath} ({file_size:,} bytes)")
            return True
            
        except Exception as e:
            logging.error(f"Error saving {filepath}: {e}")
            return False
    
    def load_latest_raw_data(self):
        """
        Load file raw data mới nhất.
        
        Returns:
            Dict: Dữ liệu từ file mới nhất hoặc None nếu không tìm thấy
        """
        if not os.path.exists(self.raw_data_dir):
            logging.error(f"Raw data directory not found: {self.raw_data_dir}")
            return None
        
        # Tìm file mới nhất
        raw_files = []
        for filename in os.listdir(self.raw_data_dir):
            if filename.startswith("netflix_raw_dataset_") and filename.endswith(".json"):
                filepath = os.path.join(self.raw_data_dir, filename)
                creation_time = os.path.getctime(filepath)
                raw_files.append((creation_time, filepath))
        
        if not raw_files:
            logging.warning("No raw dataset files found")
            return None
        
        # Load file mới nhất
        latest_file = max(raw_files, key=lambda x: x[0])[1]
        logging.info(f"Loading latest raw data: {latest_file}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logging.info(f"Loaded {len(data.get('movies', []))} movies")
            return data
            
        except Exception as e:
            logging.error(f"Error loading {latest_file}: {e}")
            return None


def setup_logging(log_dir=None, log_level=logging.INFO):
    """
    Configure logging to write to both file and console (safe for Airflow).
    Args:
        log_dir: Folder to store log file
        log_level: Logging level
    Returns:
        logger: Configured logger
    """
    if log_dir is None:
        log_dir = os.environ.get("LOGS_DIR", "logs")

    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"netflix_crawler_{timestamp}.log")

    # ✅ Tạo logger riêng biệt, không dùng root logger
    logger = logging.getLogger("netflix_crawler")
    logger.setLevel(log_level)
    logger.propagate = False  # ⛔ Không lan lên root (Airflow logger)

    # 🧹 Xóa handler cũ nếu có
    if logger.handlers:
        logger.handlers.clear()

    # File handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
    )
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler (tuỳ chọn, có thể tắt nếu chạy trong Airflow)
    if os.environ.get("DOCKER_ENV", "").lower() != "true":
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(file_formatter)
        logger.addHandler(console_handler)

    # 🛑 Không dùng logging.info(...) ở đây — thay vào đó:
    logger.info(f"✅ Logging configured. File: {log_file}")
    return logger


def main():
    """
    Main function to run crawler directly.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Netflix Data Crawler')
    parser.add_argument('--resume', help='Resume from failed movies file')
    parser.add_argument('--max-movies', type=int, help='Maximum number of movies to crawl')
    parser.add_argument('--pages', type=int, help='Number of pages per source')
    parser.add_argument('--sources', nargs='+', help='Specific sources to crawl')
    parser.add_argument('--log-dir', help='Directory for log files')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default='INFO', help='Logging level')
    args = parser.parse_args()
    
    # Setup logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_dir=args.log_dir, log_level=log_level)
    
    logging.info("=== NETFLIX CRAWLER STARTING ===")
    
    # Load configuration
    try:
        config = Config()
        logging.info(f"Configuration loaded successfully")
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return
    
    # Create crawler
    try:
        crawler = NetflixDataCrawler(config)
        logging.info("Netflix crawler initialized")
    except Exception as e:
        logging.error(f"Error initializing crawler: {e}")
        return
    
    # Run daily crawl
    try:
        crawler.crawl_daily_netflix_data(
            num_pages_per_source=args.pages,
            max_movies=args.max_movies,
            sources=args.sources,
            resume_from=args.resume
        )
        logging.info("Crawling completed successfully")
    except Exception as e:
        logging.error(f"Error during crawling: {e}")
        return
    
    logging.info("=== NETFLIX CRAWLER FINISHED ===")


if __name__ == "__main__":
    main()