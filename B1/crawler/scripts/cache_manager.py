"""
Cache Manager Module
Quản lý cache cho API responses để tối ưu hiệu suất
"""

import os
import json
import time
import hashlib
import logging
from pathlib import Path

class CacheManager:
    """
    Quản lý cache cho API responses để giảm thiểu các lần gọi API không cần thiết.
    """
    
    def __init__(self, config):
        self.config = config
        self.cache_dir = config.get("cache_dir")
        self.use_cache = config.get("use_cache", True)
        self.cache_ttl = config.get("cache_ttl", 86400)  # 24 giờ mặc định
        
        # Tạo thư mục cache nếu chưa tồn tại
        if self.use_cache:
            os.makedirs(self.cache_dir, exist_ok=True)
            logging.debug(f"Cache sẵn sàng tại: {self.cache_dir}")
    
    def _get_cache_key(self, endpoint, params):
        """
        Tạo khóa cache duy nhất cho mỗi API request.
        
        Args:
            endpoint: API endpoint
            params: Dict các parameters
            
        Returns:
            str: MD5 hash làm cache key
        """
        # Sắp xếp params để đảm bảo key nhất quán
        param_str = json.dumps(params, sort_keys=True) if params else "{}"
        # Tạo hash của endpoint và parameters
        key = hashlib.md5(f"{endpoint}:{param_str}".encode()).hexdigest()
        return key
    
    def get(self, endpoint, params=None):
        """
        Thử lấy dữ liệu từ cache.
        
        Args:
            endpoint: API endpoint
            params: Dict các parameters
            
        Returns:
            Dict: Dữ liệu từ cache hoặc None nếu không có cache hợp lệ
        """
        if not self.use_cache:
            return None
            
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        # Kiểm tra file cache có tồn tại và chưa hết hạn
        if os.path.exists(cache_path):
            cache_time = os.path.getmtime(cache_path)
            if (time.time() - cache_time) <= self.cache_ttl:
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        logging.debug(f"Cache hit: {endpoint}")
                        return data
                except Exception as e:
                    logging.warning(f"Không thể đọc cache cho {endpoint}: {e}")
        
        return None
    
    def set(self, endpoint, params, data):
        """
        Lưu dữ liệu vào cache.
        
        Args:
            endpoint: API endpoint
            params: Dict các parameters
            data: Dữ liệu để cache
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        if not self.use_cache or data is None:
            return False
            
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logging.debug(f"Cache set: {endpoint}")
            return True
        except Exception as e:
            logging.warning(f"Không thể ghi cache cho {endpoint}: {e}")
            return False
    
    def clear(self, older_than=None):
        """
        Xóa cache files.
        
        Args:
            older_than: Xóa các file cũ hơn số giây này (None = xóa tất cả)
            
        Returns:
            int: Số file đã xóa
        """
        if not self.use_cache or not os.path.exists(self.cache_dir):
            return 0
        
        deleted_count = 0
        current_time = time.time()
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
                
            file_path = os.path.join(self.cache_dir, filename)
            file_time = os.path.getmtime(file_path)
            
            # Xóa tất cả hoặc chỉ các file cũ hơn older_than
            if older_than is None or (current_time - file_time) > older_than:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception as e:
                    logging.warning(f"Không thể xóa cache file {filename}: {e}")
        
        logging.info(f"Đã xóa {deleted_count} cache files")
        return deleted_count