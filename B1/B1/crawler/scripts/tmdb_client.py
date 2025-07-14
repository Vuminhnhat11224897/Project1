"""
TMDB API Client
Client để gọi The Movie Database API
"""

import os
import time
import logging
import requests
import hashlib
from datetime import datetime
from typing import Dict, Optional, Any, List

try:
    # Import từ thư mục hiện tại khi chạy trực tiếp
    from cache_manager import CacheManager
except ImportError:
    # Import đường dẫn tuyệt đối khi chạy trong Docker/Airflow
    import sys
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, script_dir)
    from cache_manager import CacheManager

class TMDBClient:
    """Client cho The Movie Database (TMDB) API với caching và rate limiting."""
    
    def __init__(self, config):
        """
        Khởi tạo TMDB client.
        
        Args:
            config: Config object
        """
        self.api_key = config.get("tmdb_api_key")
        self.base_url = config.get("tmdb_base_url")
        self.image_base_url = config.get("tmdb_image_base_url")
        self.default_language = config.get("default_language", "vi-VN")
        
        # API rate limiting
        self.api_delay = config.get("crawler_delay", 0.3)
        self.max_retries = config.get("crawler_max_retries", 3)
        self.timeout = config.get("crawler_timeout", 30)
        
        # Cache manager
        self.cache = CacheManager(config)
        
        # Request state
        self.last_request_time = 0
        
        if not self.api_key:
            raise ValueError("TMDB API Key không được cung cấp trong cấu hình")
            
        logging.info(f"TMDBClient đã khởi tạo với base URL: {self.base_url}")
    
    def _respect_rate_limit(self):
        """
        Đảm bảo tuân thủ rate limit bằng cách thêm delay phù hợp.
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.api_delay:
            time.sleep(self.api_delay - elapsed)
    
    def make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Thực hiện request đến TMDB API với caching và retry.
        
        Args:
            endpoint: API endpoint (không bao gồm base URL)
            params: Dict các parameters query
            
        Returns:
            Dict: API response hoặc None nếu request thất bại
        """
        if params is None:
            params = {}
        
        # Thử lấy từ cache trước
        cached_response = self.cache.get(endpoint, params)
        if cached_response is not None:
            return cached_response
        
        # Chuẩn bị request parameters
        request_params = params.copy()
        request_params['api_key'] = self.api_key
        request_params['language'] = self.default_language
        
        url = f"{self.base_url}/{endpoint}"
        
        # Tuân thủ rate limit
        self._respect_rate_limit()
        
        # Retry logic
        for attempt in range(self.max_retries + 1):
            try:
                logging.debug(f"API Request: {endpoint} (attempt {attempt+1}/{self.max_retries+1})")
                response = requests.get(url, params=request_params, timeout=self.timeout)
                self.last_request_time = time.time()
                
                response.raise_for_status()
                data = response.json()
                
                # Lưu vào cache nếu thành công
                self.cache.set(endpoint, params, data)
                return data
                
            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP error ({e.response.status_code}) khi gọi {endpoint}: {e}")
                if e.response.status_code in [400, 401, 403, 404]:
                    # Không cần retry với lỗi client
                    break
                
                # Nếu không phải lần thử cuối, chờ và thử lại
                if attempt < self.max_retries:
                    wait_time = 2 ** (attempt + 1)  # Exponential backoff
                    logging.warning(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    
            except (requests.exceptions.RequestException, Exception) as e:
                logging.error(f"Error khi gọi {endpoint}: {e}")
                
                # Nếu không phải lần thử cuối, chờ và thử lại
                if attempt < self.max_retries:
                    wait_time = 2 ** (attempt + 1)
                    logging.warning(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
        
        return None
    
    def get_netflix_movies(self, page: int = 1) -> Optional[Dict]:
        """
        Lấy phim Netflix sử dụng discover với Netflix provider ID.
        
        Args:
            page: Số trang
            
        Returns:
            Dict: API response với danh sách phim Netflix
        """
        endpoint = "discover/movie"
        params = {
            "page": page,
            "with_watch_providers": "8",  # Netflix provider ID
            "watch_region": "US"
        }
        return self.make_request(endpoint, params)
    
    def get_trending_movies(self, time_window: str = "week", page: int = 1) -> Optional[Dict]:
        """
        Lấy phim trending.
        
        Args:
            time_window: "day" hoặc "week"
            page: Số trang
            
        Returns:
            Dict: API response với danh sách phim trending
        """
        endpoint = f"trending/movie/{time_window}"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_popular_movies(self, page: int = 1) -> Optional[Dict]:
        """
        Lấy danh sách phim phổ biến.
        
        Args:
            page: Số trang
            
        Returns:
            Dict: API response với danh sách phim phổ biến
        """
        endpoint = "movie/popular"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_top_rated_movies(self, page: int = 1) -> Optional[Dict]:
        """
        Lấy danh sách phim được đánh giá cao nhất.
        
        Args:
            page: Số trang
            
        Returns:
            Dict: API response với danh sách phim top rated
        """
        endpoint = "movie/top_rated"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_now_playing_movies(self, page: int = 1) -> Optional[Dict]:
        """
        Lấy danh sách phim đang chiếu.
        
        Args:
            page: Số trang
            
        Returns:
            Dict: API response với danh sách phim đang chiếu
        """
        endpoint = "movie/now_playing"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_movie_details(self, movie_id: int) -> Optional[Dict]:
        """
        Lấy thông tin chi tiết của phim.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Thông tin chi tiết của phim
        """
        endpoint = f"movie/{movie_id}"
        return self.make_request(endpoint)
    
    def get_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """
        Lấy thông tin về cast và crew của phim.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Thông tin về cast và crew
        """
        endpoint = f"movie/{movie_id}/credits"
        return self.make_request(endpoint)
    
    def get_movie_keywords(self, movie_id: int) -> Optional[Dict]:
        """
        Lấy keywords/tags của phim.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Keywords của phim
        """
        endpoint = f"movie/{movie_id}/keywords"
        return self.make_request(endpoint)
    
    def get_movie_videos(self, movie_id: int) -> Optional[Dict]:
        """
        Lấy videos (trailers, teasers) của phim.
        
        Args:
            movie_id: ID của phim
            
        Returns:
            Dict: Videos của phim
        """
        endpoint = f"movie/{movie_id}/videos"
        return self.make_request(endpoint)
    
    def get_movie_reviews(self, movie_id: int, page: int = 1) -> Optional[Dict]:
        """
        Lấy reviews của phim.
        
        Args:
            movie_id: ID của phim
            page: Số trang
            
        Returns:
            Dict: Reviews của phim
        """
        endpoint = f"movie/{movie_id}/reviews"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_movie_similar(self, movie_id: int, page: int = 1) -> Optional[Dict]:
        """
        Lấy phim tương tự với phim đã cho.
        
        Args:
            movie_id: ID của phim
            page: Số trang
            
        Returns:
            Dict: Danh sách phim tương tự
        """
        endpoint = f"movie/{movie_id}/similar"
        params = {"page": page}
        return self.make_request(endpoint, params)
    
    def get_movie_poster_url(self, poster_path: str, size: str = "w500") -> str:
        """
        Lấy URL của poster phim.
        
        Args:
            poster_path: Path của poster (từ kết quả API)
            size: Kích thước poster (w92, w154, w185, w342, w500, w780, original)
            
        Returns:
            str: URL đầy đủ của poster
        """
        if not poster_path:
            return None
        return f"{self.image_base_url}{size}/{poster_path}"