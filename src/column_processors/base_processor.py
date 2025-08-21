"""
Base Column Processor
Provides common functionality for all column-specific processors
"""

from abc import ABC, abstractmethod
import pandas as pd
import re
import warnings
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass

@dataclass
class ProcessingResult:
    """Result of column processing"""
    original_value: Any
    cleaned_value: Any
    confidence: float
    transformations_applied: List[str]
    issues_found: List[str]
    metadata: Dict[str, Any]

@dataclass
class ColumnAnalysis:
    """Analysis result for a column"""
    column_name: str
    total_rows: int
    null_count: int
    unique_count: int
    quality_score: float
    issues_summary: Dict[str, int]
    recommendations: List[str]
    sample_issues: List[Dict[str, Any]]

class BaseColumnProcessor(ABC):
    """
    Base class for all column-specific processors
    Each column processor inherits from this and implements specific logic
    """
    
    def __init__(self, column_name: str, column_description: str):
        self.column_name = column_name
        self.column_description = column_description
        self.processing_stats = {
            'total_processed': 0,
            'successful_cleanings': 0,
            'failed_cleanings': 0,
            'transformations_applied': {}
        }
    
    @abstractmethod
    def analyze_column(self, series: pd.Series, sample_size: int = 100) -> ColumnAnalysis:
        """Analyze the column and identify issues"""
        pass
    
    @abstractmethod
    def clean_value(self, value: Any, context: Dict[str, Any] = None) -> ProcessingResult:
        """Clean a single value from the column"""
        pass
    
    @abstractmethod
    def get_validation_rules(self) -> Dict[str, Any]:
        """Get validation rules specific to this column"""
        pass
    
    def process_column(self, series: pd.Series, 
                      processing_options: Dict[str, Any] = None) -> pd.Series:
        """Process entire column with the specified options"""
        if processing_options is None:
            processing_options = {}
        
        results = []
        context = self._build_context(series, processing_options)
        
        for idx, value in series.items():
            try:
                result = self.clean_value(value, context)
                results.append(result.cleaned_value)
                
                # Update stats
                self.processing_stats['total_processed'] += 1
                if result.confidence > 0.7:
                    self.processing_stats['successful_cleanings'] += 1
                
                # Track transformations
                for transform in result.transformations_applied:
                    self.processing_stats['transformations_applied'][transform] = \
                        self.processing_stats['transformations_applied'].get(transform, 0) + 1
                        
            except Exception as e:
                results.append(value)  # Keep original on error
                self.processing_stats['failed_cleanings'] += 1
                warnings.warn(f"Failed to process value {value}: {e}")
        
        return pd.Series(results, index=series.index)
    
    def _build_context(self, series: pd.Series, options: Dict[str, Any]) -> Dict[str, Any]:
        """Build context information for processing"""
        return {
            'column_name': self.column_name,
            'total_rows': len(series),
            'unique_values': series.nunique(),
            'null_count': series.isnull().sum(),
            'processing_options': options,
            'value_counts': series.value_counts().head(20).to_dict()
        }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.processing_stats.copy()
    
    def reset_stats(self):
        """Reset processing statistics"""
        self.processing_stats = {
            'total_processed': 0,
            'successful_cleanings': 0,
            'failed_cleanings': 0,
            'transformations_applied': {}
        }
    
    # Common utility methods
    def _normalize_text(self, text: str) -> str:
        """Normalize text (remove extra spaces, fix case, etc.)"""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Fix common punctuation issues
        text = re.sub(r'\s*,\s*', ', ', text)
        text = re.sub(r'\s*;\s*', '; ', text)
        text = re.sub(r'\s*:\s*', ': ', text)
        
        return text
    
    def _extract_numeric(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if pd.isna(text):
            return None
        
        # Remove common non-numeric characters but keep decimal point and negative
        cleaned = re.sub(r'[^\d\.-]', '', str(text))
        
        try:
            return float(cleaned)
        except ValueError:
            return None
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Check if string looks like a date"""
        if pd.isna(date_str) or not isinstance(date_str, str):
            return False
        
        # Common date patterns
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # M/D/YY or MM/DD/YYYY
        ]
        
        return any(re.search(pattern, date_str) for pattern in date_patterns)
    
    def _calculate_confidence(self, original: Any, cleaned: Any, 
                            transformations: List[str]) -> float:
        """Calculate confidence score for the transformation"""
        if pd.isna(original) and pd.isna(cleaned):
            return 1.0
        
        if original == cleaned:
            return 1.0
        
        # Base confidence based on number of transformations
        base_confidence = max(0.5, 1.0 - (len(transformations) * 0.1))
        
        # Adjust based on transformation types
        high_confidence_transforms = ['normalize_text', 'fix_spacing', 'standardize_case']
        medium_confidence_transforms = ['fix_spelling', 'expand_abbreviation']
        low_confidence_transforms = ['api_correction', 'complex_parsing']
        
        confidence_adjustment = 0
        for transform in transformations:
            if transform in high_confidence_transforms:
                confidence_adjustment += 0.1
            elif transform in medium_confidence_transforms:
                confidence_adjustment += 0.0
            elif transform in low_confidence_transforms:
                confidence_adjustment -= 0.2
        
        return max(0.1, min(1.0, base_confidence + confidence_adjustment))
