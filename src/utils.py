import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional, Tuple
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import json

class DataValidator:
    """Utility class for data validation"""
    
    @staticmethod
    def validate_medical_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate medical dataset structure and content"""
        validation_results = {
            'is_valid': True,
            'issues': [],
            'warnings': [],
            'stats': {}
        }
        
        # Check for required medical columns
        expected_columns = ['patient_id', 'test', 'biomarker', 'result']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            validation_results['issues'].append(f"Missing expected columns: {missing_columns}")
            validation_results['is_valid'] = False
        
        # Check data quality
        validation_results['stats'] = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'duplicate_rows': df.duplicated().sum()
        }
        
        # Warnings for data quality issues
        if validation_results['stats']['missing_values_percentage'] > 50:
            validation_results['warnings'].append("High percentage of missing values (>50%)")
        
        if validation_results['stats']['duplicate_rows'] > len(df) * 0.1:
            validation_results['warnings'].append("High number of duplicate rows (>10%)")
        
        return validation_results

class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self):
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'api_calls': 0,
            'local_processing': 0,
            'cache_hits': 0,
            'errors': 0
        }
    
    def start_monitoring(self):
        """Start performance monitoring"""
        self.metrics['start_time'] = time.time()
    
    def end_monitoring(self):
        """End performance monitoring"""
        self.metrics['end_time'] = time.time()
    
    def record_api_call(self):
        """Record an API call"""
        self.metrics['api_calls'] += 1
    
    def record_local_processing(self):
        """Record local processing"""
        self.metrics['local_processing'] += 1
    
    def record_cache_hit(self):
        """Record cache hit"""
        self.metrics['cache_hits'] += 1
    
    def record_error(self):
        """Record an error"""
        self.metrics['errors'] += 1
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        if self.metrics['start_time'] and self.metrics['end_time']:
            duration = self.metrics['end_time'] - self.metrics['start_time']
        else:
            duration = 0
        
        total_operations = self.metrics['api_calls'] + self.metrics['local_processing']
        
        return {
            'duration_seconds': duration,
            'total_operations': total_operations,
            'operations_per_second': total_operations / duration if duration > 0 else 0,
            'api_call_ratio': self.metrics['api_calls'] / total_operations if total_operations > 0 else 0,
            'cache_hit_ratio': self.metrics['cache_hits'] / total_operations if total_operations > 0 else 0,
            'error_rate': self.metrics['errors'] / total_operations if total_operations > 0 else 0,
            'efficiency_score': (self.metrics['local_processing'] + self.metrics['cache_hits']) / total_operations if total_operations > 0 else 0
        }

class TextNormalizer:
    """Utility for text normalization and preprocessing"""
    
    @staticmethod
    def normalize_medical_text(text: str) -> str:
        """Normalize medical text for consistent processing"""
        if pd.isna(text) or not str(text).strip():
            return text
        
        normalized = str(text).strip()
        
        # Convert to lowercase for processing
        normalized = normalized.lower()
        
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove special characters except medical-relevant ones
        normalized = re.sub(r'[^\w\s\-\.\,\;\:\(\)]', '', normalized)
        
        # Fix common OCR errors in medical text
        ocr_corrections = {
            '0': 'o',  # Common OCR mistake in medical terms
            '1': 'l',  # Common OCR mistake
            '5': 's',  # Common OCR mistake
        }
        
        # Apply OCR corrections carefully (only in specific contexts)
        for wrong, correct in ocr_corrections.items():
            # Only replace if it's clearly wrong (e.g., numbers in the middle of words)
            pattern = r'(?<=[a-z])' + re.escape(wrong) + r'(?=[a-z])'
            normalized = re.sub(pattern, correct, normalized)
        
        return normalized
    
    @staticmethod
    def extract_medical_entities(text: str) -> Dict[str, List[str]]:
        """Extract medical entities from text"""
        entities = {
            'conditions': [],
            'medications': [],
            'procedures': [],
            'symptoms': []
        }
        
        # Simple pattern matching for medical entities
        condition_patterns = [
            r'\b(?:diabetes|hypertension|pneumonia|asthma|bronchitis)\b',
            r'\b\w*itis\b',  # inflammation conditions
            r'\b\w*osis\b',  # disease conditions
        ]
        
        text_lower = text.lower() if isinstance(text, str) else ""
        
        for pattern in condition_patterns:
            matches = re.findall(pattern, text_lower)
            entities['conditions'].extend(matches)
        
        return entities

class BatchProcessor:
    """Utility for efficient batch processing"""
    
    def __init__(self, batch_size: int = 10, max_workers: int = 4):
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    def process_in_batches(self, items: List[Any], process_func, *args, **kwargs) -> List[Any]:
        """Process items in batches synchronously"""
        results = []
        
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            batch_results = [process_func(item, *args, **kwargs) for item in batch]
            results.extend(batch_results)
            
            # Small delay to prevent overwhelming APIs
            time.sleep(0.1)
        
        return results
    
    async def process_in_batches_async(self, items: List[Any], process_func, *args, **kwargs) -> List[Any]:
        """Process items in batches asynchronously"""
        results = []
        
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            
            # Create tasks for the batch
            tasks = [self._async_process_item(item, process_func, *args, **kwargs) for item in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle exceptions
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"Error processing item: {result}")
                    results.append(None)
                else:
                    results.append(result)
            
            # Rate limiting
            await asyncio.sleep(0.1)
        
        return results
    
    async def _async_process_item(self, item: Any, process_func, *args, **kwargs):
        """Process a single item asynchronously"""
        loop = asyncio.get_event_loop()
        
        if asyncio.iscoroutinefunction(process_func):
            return await process_func(item, *args, **kwargs)
        else:
            return await loop.run_in_executor(None, process_func, item, *args, **kwargs)

class ConfigManager:
    """Manage configuration settings"""
    
    DEFAULT_CONFIG = {
        'api': {
            'openai_model': 'gpt-4o-mini',
            'max_tokens': 50,
            'temperature': 0,
            'rate_limit_delay': 1.0
        },
        'processing': {
            'batch_size': 10,
            'confidence_threshold': 0.7,
            'max_api_calls': 1000,
            'enable_caching': True
        },
        'medical': {
            'expand_abbreviations': True,
            'fix_typos': True,
            'standardize_terminology': True,
            'validate_medical_context': True
        }
    }
    
    def __init__(self, config_file: str = 'config.json'):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or create default"""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            # Merge with defaults
            return self._merge_configs(self.DEFAULT_CONFIG, config)
        except FileNotFoundError:
            # Create default config file
            self._save_config(self.DEFAULT_CONFIG)
            return self.DEFAULT_CONFIG.copy()
    
    def _merge_configs(self, default: Dict, custom: Dict) -> Dict:
        """Merge custom config with default config"""
        result = default.copy()
        
        for key, value in custom.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _save_config(self, config: Dict[str, Any]):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'api.openai_model')"""
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def set(self, key_path: str, value: Any):
        """Set configuration value using dot notation"""
        keys = key_path.split('.')
        config = self.config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
        self._save_config(self.config)
    
    def update_config(self, updates: Dict[str, Any]):
        """Update multiple configuration values"""
        self.config = self._merge_configs(self.config, updates)
        self._save_config(self.config)

class LogManager:
    """Simple logging utility for debugging and monitoring"""
    
    def __init__(self, log_file: str = 'medical_processor.log'):
        self.log_file = log_file
        self.logs = []
    
    def log(self, level: str, message: str, **kwargs):
        """Add log entry"""
        log_entry = {
            'timestamp': time.time(),
            'level': level.upper(),
            'message': message,
            'data': kwargs
        }
        
        self.logs.append(log_entry)
        
        # Also print to console for debugging
        if level.upper() in ['ERROR', 'WARNING']:
            print(f"[{level.upper()}] {message}")
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.log('INFO', message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.log('WARNING', message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.log('ERROR', message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.log('DEBUG', message, **kwargs)
    
    def save_logs(self):
        """Save logs to file"""
        with open(self.log_file, 'w') as f:
            json.dump(self.logs, f, indent=2)
    
    def get_logs(self, level: str = None) -> List[Dict]:
        """Get logs, optionally filtered by level"""
        if level:
            return [log for log in self.logs if log['level'] == level.upper()]
        return self.logs

# Helper functions
def calculate_similarity(text1: str, text2: str) -> float:
    """Calculate similarity between two texts"""
    from difflib import SequenceMatcher
    return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()

def is_medical_term(text: str) -> bool:
    """Simple check if text appears to be a medical term"""
    medical_indicators = [
        'diagnosis', 'syndrome', 'disease', 'infection', 'inflammation',
        'blood', 'heart', 'lung', 'kidney', 'liver', 'brain',
        'pressure', 'diabetes', 'hyper', 'hypo', 'itis', 'osis', 'emia'
    ]
    
    text_lower = text.lower() if isinstance(text, str) else ""
    return any(indicator in text_lower for indicator in medical_indicators)

def format_processing_time(seconds: float) -> str:
    """Format processing time in a human-readable way"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    else:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"