"""
Netflix Data Crawler
Crawler thu thập dữ liệu phim từ TMDB API
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import hashlib

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
        
        # Đường dẫn thư mục dữ liệu
        self.raw_data_dir = self.config.get("raw_data_dir")
        self.processed_data_dir = self.config.get("processed_data_dir")
        
        # Đảm bảo thư mục tồn tại
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        logging.info(f"Netflix Data Crawler đã khởi tạo: raw_data_dir={self.raw_data_dir}")
    
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
                movies = self.tmdb.get_popular_movies(page)
            elif list_type == "top_rated":
                movies = self.tmdb.get_top_rated_movies(page)
            elif list_type == "now_playing":
                movies = self.tmdb.get_now_playing_movies(page)
            elif list_type == "netflix":
                movies = self.tmdb.get_netflix_movies(page)
            elif list_type == "trending_day":
                movies = self.tmdb.get_trending_movies("day", page)
            elif list_type == "trending_week":
                movies = self.tmdb.get_trending_movies("week", page)
            else:
                logging.warning(f"Unknown list type: {list_type}")
                continue
            
            if movies and 'results' in movies:
                for movie in movies['results']:
                    movie['source_list'] = list_type
                    movie['page'] = page
                all_movies.extend(movies['results'])
                logging.info(f"Found {len(movies['results'])} movies")
            
            time.sleep(0.25)  # Rate limiting
        
        logging.info(f"Total {list_type} movies: {len(all_movies)}")
        return all_movies
    
    def crawl_movie_details(self, movie_id: int) -> Dict:
        """
        Crawl thông tin chi tiết của một phim.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Thông tin chi tiết của phim
        """
        logging.info(f"Crawling details for movie ID: {movie_id}")
        
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
        movie_details = self.tmdb.get_movie_details(movie_id)
        if movie_details:
            movie_data['details'] = movie_details
            logging.info(f"✓ Details: {movie_details.get('title', 'N/A')}")
        else:
            logging.warning(f"✗ No details found for movie ID: {movie_id}")
            return movie_data  # Return early if we can't get basic details
        
        # 2. Credits (cast & crew)
        credits = self.tmdb.get_movie_credits(movie_id)
        if credits:
            movie_data['credits'] = credits
            cast_count = len(credits.get('cast', []))
            crew_count = len(credits.get('crew', []))
            logging.info(f"✓ Credits: {cast_count} cast, {crew_count} crew")
        
        # 3. Keywords
        keywords = self.tmdb.get_movie_keywords(movie_id)
        if keywords:
            movie_data['keywords'] = keywords
            keyword_count = len(keywords.get('keywords', []))
            logging.info(f"✓ Keywords: {keyword_count} keywords")
        
        # 4. Videos/Trailers
        videos = self.tmdb.get_movie_videos(movie_id)
        if videos:
            movie_data['videos'] = videos
            video_count = len(videos.get('results', []))
            logging.info(f"✓ Videos: {video_count} videos")
        
        # 5. Reviews
        reviews = self.tmdb.get_movie_reviews(movie_id)
        if reviews:
            movie_data['reviews'] = reviews
            review_count = len(reviews.get('results', []))
            logging.info(f"✓ Reviews: {review_count} reviews")
        
        # 6. Similar movies
        similar = self.tmdb.get_movie_similar(movie_id)
        if similar:
            movie_data['similar'] = similar
            similar_count = len(similar.get('results', []))
            logging.info(f"✓ Similar: {similar_count} similar movies")
        
        return movie_data
    
    def crawl_daily_netflix_data(self, 
                                num_pages_per_source: int = None,
                                max_movies: int = None,
                                sources: List[str] = None) -> Dict:
        """
        Crawl dữ liệu hàng ngày từ nhiều nguồn.
        
        Args:
            num_pages_per_source: Số trang mỗi nguồn (None = dùng cấu hình)
            max_movies: Số phim tối đa để crawl chi tiết (None = dùng cấu hình)
            sources: Danh sách nguồn để crawl (None = dùng tất cả nguồn được bật trong cấu hình)
            
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
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_file = os.path.join(self.raw_data_dir, f"movie_list_summary_{timestamp}.json")
        self.save_json(movie_list_summary, summary_file)
        
        # 3. Crawl thông tin chi tiết cho các phim đã chọn
        # Giới hạn số lượng phim để crawl chi tiết
        selected_movies = list(all_movie_ids)[:max_movies]
        
        logging.info(f"Crawling detailed info for {len(selected_movies)} movies...")
        
        detailed_movies = []
        failed_movies = []
        
        # Parallel crawling with ThreadPoolExecutor
        max_workers = min(5, len(selected_movies))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_id = {executor.submit(self.crawl_movie_details, movie_id): movie_id for movie_id in selected_movies}
            
            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_id), 1):
                movie_id = future_to_id[future]
                try:
                    movie_data = future.result()
                    
                    if movie_data['details']:  # Only save if we got basic details
                        movie_data['sources'] = movie_sources.get(movie_id, [])
                        detailed_movies.append(movie_data)
                    else:
                        failed_movies.append(movie_id)
                    
                    # Progress save every 25 movies
                    if i % 25 == 0 or i == len(selected_movies):
                        batch_file = os.path.join(self.raw_data_dir, 
                                                  f"detailed_movies_batch_{i//25}_{timestamp}.json")
                        self.save_json(detailed_movies[-25:], batch_file)
                        logging.info(f"Saved batch {i//25}: {len(detailed_movies)} movies so far")
                        
                except Exception as e:
                    logging.error(f"Error crawling movie {movie_id}: {e}")
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
        
        final_file = os.path.join(self.raw_data_dir, f"netflix_raw_dataset_{timestamp}.json")
        self.save_json(final_results, final_file)
        
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
    Configure logging to write to both file and console.
    
    Args:
        log_dir: Thư mục để lưu log file
        log_level: Logging level
        
    Returns:
        logger: Configured logger
    """
    from logging.handlers import RotatingFileHandler
    
    if log_dir is None:
        # Try to get from environment
        log_dir = os.environ.get("LOGS_DIR", "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"netflix_crawler_{timestamp}.log")
    
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # Create file handler with rotation (max 5MB, keep 3 backups)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info(f"Logging configured. Log file: {log_file}")
    return logger


def main():
    """
    Main function to run crawler directly.
    """
    # Setup logging
    setup_logging()
    
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
        crawler.crawl_daily_netflix_data()
        logging.info("Crawling completed successfully")
    except Exception as e:
        logging.error(f"Error during crawling: {e}")
        return
    
    logging.info("=== NETFLIX CRAWLER FINISHED ===")


if __name__ == "__main__":
    main()