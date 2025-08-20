import pandas as pd
import re
from typing import List
from dataclasses import dataclass

@dataclass
class DataIssue:
    column: str
    row_index: int
    issue_type: str
    severity: str
    original_value: str
    suggested_fix: str
    confidence: float

class DataAnalysisAgent:
    """Agent for identifying data quality issues"""
    
    def __init__(self):
        self.issue_patterns = {
            'spelling_errors': r'[a-zA-Z]*[0-9]+[a-zA-Z]*|[a-zA-Z]*[^a-zA-Z0-9\s\-\.][a-zA-Z]*',
            'inconsistent_case': r'^[a-z]+$|^[A-Z]+$',
            'extra_spaces': r'\s{2,}',
            'special_chars': r'[^\w\s\-\.]',
            'incomplete_words': r'\b\w{1,2}\b(?!\s(?:of|in|at|to|is|be|or))',
            'medical_typos': r'diabetis|hypertention|pnemonia|asthama|bronchitus'
        }
        
        self.medical_abbreviations = {
            'htn', 'dm', 'mi', 'copd', 'bp', 'hr', 'rr', 'temp', 'wbc', 'rbc'
        }
    
    def analyze_column(self, series: pd.Series) -> List[DataIssue]:
        """Analyze a column for various data quality issues"""
        issues = []
        
        for idx, value in series.items():
            if pd.isna(value) or not str(value).strip():
                issues.append(DataIssue(
                    column=series.name, row_index=idx, issue_type='missing_value',
                    severity='medium', original_value='', suggested_fix='',
                    confidence=1.0
                ))
                continue
                
            text = str(value).strip()
            
            # Check for medical typos
            if re.search(self.issue_patterns['medical_typos'], text, re.IGNORECASE):
                issues.append(DataIssue(
                    column=series.name, row_index=idx, issue_type='medical_typo',
                    severity='high', original_value=text, suggested_fix='',
                    confidence=0.8
                ))
            
            # Check for formatting issues
            if re.search(self.issue_patterns['extra_spaces'], text):
                suggested = re.sub(r'\s+', ' ', text)
                issues.append(DataIssue(
                    column=series.name, row_index=idx, issue_type='formatting',
                    severity='low', original_value=text, suggested_fix=suggested,
                    confidence=0.9
                ))
            
            # Check for abbreviations that might need expansion
            words = text.lower().split()
            for word in words:
                if word in self.medical_abbreviations:
                    issues.append(DataIssue(
                        column=series.name, row_index=idx, issue_type='abbreviation',
                        severity='medium', original_value=text, suggested_fix='',
                        confidence=0.7
                    ))
                    break
        
        return issues
    
    def get_column_stats(self, series: pd.Series) -> dict:
        """Get statistical information about a column"""
        return {
            'total_rows': len(series),
            'missing_values': series.isna().sum(),
            'unique_values': series.nunique(),
            'avg_length': series.astype(str).str.len().mean(),
            'data_type': str(series.dtype)
        }