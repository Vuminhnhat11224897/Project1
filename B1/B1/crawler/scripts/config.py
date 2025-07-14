"""
Config Management Module
Quản lý cấu hình cho Netflix Movie Crawler
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv

class Config:
    """Quản lý cấu hình từ file .env và cấu hình mặc định"""
    
    # Cấu hình mặc định (chỉ dùng khi không có biến môi trường)
    DEFAULT_CONFIG = {
        # API Settings
        "tmdb_api_key": None,
        "tmdb_base_url": "https://api.themoviedb.org/3",
        "tmdb_image_base_url": "https://image.tmdb.org/t/p/",
        "default_language": "vi-VN",
        
        # Crawler Settings
        "crawler_max_retries": 3,
        "crawler_delay": 0.3,
        "crawler_timeout": 30,
        "max_pages_per_source": 5,
        "max_movies_per_day": 100,
        
        # Cache Settings
        "use_cache": True,
        "cache_ttl": 86400,  # 24 hours
        
        # Data Sources
        "data_sources": {
            "netflix": {"enabled": True, "pages": 5},
            "popular": {"enabled": True, "pages": 5},
            "trending_day": {"enabled": True, "pages": 2},
            "trending_week": {"enabled": True, "pages": 2},
            "top_rated": {"enabled": True, "pages": 3},
        "batch_size": 25,
        "max_workers": 5,
        "max_retries": 3,
        "retry_backoff": 1.5,
        "rate_limit_wait": 0.25
        }
    }
    
    def __init__(self, env_path=None):
        """
        Khởi tạo cấu hình từ .env
        """
        # Load biến môi trường từ .env
        if env_path and os.path.exists(env_path):
            load_dotenv(env_path)
        else:
            load_dotenv()
        
        # Khởi tạo cấu hình từ mặc định
        self.config = self.DEFAULT_CONFIG.copy()
        
        # Cập nhật từ biến môi trường - nguồn sự thật duy nhất
        self._update_from_env()
            
        # Đảm bảo các đường dẫn thư mục được tạo đầy đủ
        self._setup_directories()
    
    def _update_from_env(self):
        """Cập nhật cấu hình từ biến môi trường - Nguồn sự thật"""
        # API settings
        self.config["tmdb_api_key"] = os.getenv("TMDB_API_KEY", self.config["tmdb_api_key"])
        self.config["tmdb_base_url"] = os.getenv("TMDB_BASE_URL", self.config["tmdb_base_url"])
        self.config["tmdb_image_base_url"] = os.getenv("TMDB_IMAGE_BASE_URL", self.config["tmdb_image_base_url"])
        self.config["default_language"] = os.getenv("DEFAULT_LANGUAGE", self.config["default_language"])
        
        # Crawler settings
        if os.getenv("CRAWLER_MAX_RETRIES"):
            self.config["crawler_max_retries"] = int(os.getenv("CRAWLER_MAX_RETRIES"))
        if os.getenv("CRAWLER_DELAY"):
            self.config["crawler_delay"] = float(os.getenv("CRAWLER_DELAY"))
        if os.getenv("CRAWLER_TIMEOUT"):
            self.config["crawler_timeout"] = int(os.getenv("CRAWLER_TIMEOUT"))
        if os.getenv("MAX_PAGES_PER_SOURCE"):
            self.config["max_pages_per_source"] = int(os.getenv("MAX_PAGES_PER_SOURCE"))
        if os.getenv("MAX_MOVIES_PER_DAY"):
            self.config["max_movies_per_day"] = int(os.getenv("MAX_MOVIES_PER_DAY"))
            
        # Cache settings
        if os.getenv("USE_CACHE"):
            self.config["use_cache"] = os.getenv("USE_CACHE").lower() == "true"
        if os.getenv("CACHE_TTL"):
            self.config["cache_ttl"] = int(os.getenv("CACHE_TTL"))
        
        # Đường dẫn dữ liệu - LUÔN ưu tiên biến môi trường
        # Trong Docker, đường dẫn cố định
        if os.environ.get("DOCKER_ENV", "").lower() == "true":
            base_dir = "/opt/crawler/data"
        else:
            base_dir = os.getenv("DATA_ROOT_DIR", "data")
        
        # Thiết lập các đường dẫn con
        self.config["data_root_dir"] = base_dir
        self.config["raw_data_dir"] = os.path.join(base_dir, os.getenv("RAW_DATA_DIR", "raw"))
        self.config["processed_data_dir"] = os.path.join(base_dir, os.getenv("PROCESSED_DATA_DIR", "processed"))
        self.config["cache_dir"] = os.path.join(base_dir, os.getenv("CACHE_DIR", "cache"))
        self.config["logs_dir"] = os.path.join(base_dir, os.getenv("LOGS_DIR", "logs"))
    
    def _setup_directories(self):
        """Tạo cấu trúc thư mục dữ liệu"""
        # Tạo thư mục nếu chưa tồn tại
        for directory in [
            self.config["data_root_dir"],
            self.config["raw_data_dir"],
            self.config["processed_data_dir"],
            self.config["cache_dir"],
            self.config["logs_dir"]
        ]:
            os.makedirs(directory, exist_ok=True)
    
    def get(self, key, default=None):
        """Lấy giá trị cấu hình theo khóa"""
        return self.config.get(key, default)
    
    def get_nested(self, *keys, default=None):
        """Lấy giá trị cấu hình lồng nhau"""
        current = self.config
        for key in keys:
            if key in current and isinstance(current, dict):
                current = current[key]
            else:
                return default
        return current