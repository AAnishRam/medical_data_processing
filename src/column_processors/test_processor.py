"""
Test Column Processor
Handles laboratory test/investigation name cleaning and standardization
"""

import pandas as pd
import re
from typing import Dict, List, Any
from .base_processor import BaseColumnProcessor, ProcessingResult, ColumnAnalysis

class TestColumnProcessor(BaseColumnProcessor):
    """
    Processor for 'test' column - laboratory test/investigation names
    Handles spelling variants, source suffixes, and standardization
    """
    
    def __init__(self):
        super().__init__(
            column_name="test",
            column_description="Name of the laboratory test/investigation performed"
        )
        
        # Common test name mappings and standardizations
        self.test_mappings = {
            # Blood tests
            'cbc': 'Complete Blood Count',
            'complete blood count': 'Complete Blood Count',
            'fbc': 'Full Blood Count',
            'full blood count': 'Full Blood Count',
            'hemogram': 'Complete Blood Count',
            'blood count': 'Complete Blood Count',
            
            # Chemistry tests
            'bmp': 'Basic Metabolic Panel',
            'cmp': 'Comprehensive Metabolic Panel',
            'lft': 'Liver Function Test',
            'liver function': 'Liver Function Test',
            'rft': 'Renal Function Test',
            'kidney function': 'Renal Function Test',
            'lipid profile': 'Lipid Panel',
            'lipid panel': 'Lipid Panel',
            
            # Cardiac tests
            'ecg': 'Electrocardiogram',
            'ekg': 'Electrocardiogram',
            'echo': 'Echocardiogram',
            'stress test': 'Cardiac Stress Test',
            'tmt': 'Treadmill Test',
            
            # Imaging
            'ct scan': 'CT Scan',
            'mri': 'MRI Scan',
            'x-ray': 'X-Ray',
            'xray': 'X-Ray',
            'ultrasound': 'Ultrasound',
            'usg': 'Ultrasound',
            
            # Endocrine
            'thyroid function': 'Thyroid Function Test',
            'tft': 'Thyroid Function Test',
            'hba1c': 'HbA1c',
            'glucose': 'Blood Glucose',
            'sugar': 'Blood Glucose',
        }
        
        # Common spelling errors
        self.spelling_corrections = {
            'compelte': 'complete',
            'blod': 'blood',
            'coutn': 'count',
            'functin': 'function',
            'functon': 'function',
            'tset': 'test',
            'pnarl': 'panel',
            'panle': 'panel',
            'scna': 'scan',
            'echocardiogrm': 'echocardiogram',
            'electrocardigrm': 'electrocardiogram'
        }
        
        # Source suffixes to remove
        self.source_suffixes = [
            '_lab', '_hospital', '_clinic', '_center', '_centre',
            '_test', '_investigation', '_exam', '_study'
        ]
    
    def analyze_column(self, series: pd.Series, sample_size: int = 100) -> ColumnAnalysis:
        """Analyze test column for issues and patterns"""
        
        # Basic statistics
        total_rows = len(series)
        null_count = series.isnull().sum()
        unique_count = series.nunique()
        
        # Sample for analysis
        sample_data = series.dropna().head(sample_size)
        
        # Issue detection
        issues_summary = {}
        sample_issues = []
        recommendations = []
        
        # Check for spelling issues
        spelling_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                value_lower = value.lower()
                for wrong, correct in self.spelling_corrections.items():
                    if wrong in value_lower:
                        spelling_issues += 1
                        sample_issues.append({
                            'type': 'spelling_error',
                            'value': value,
                            'issue': f"Contains '{wrong}' should be '{correct}'"
                        })
                        break
        
        issues_summary['spelling_errors'] = spelling_issues
        
        # Check for abbreviations that could be expanded
        abbreviation_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                value_lower = value.lower().strip()
                if value_lower in self.test_mappings and len(value_lower) <= 5:
                    abbreviation_issues += 1
                    sample_issues.append({
                        'type': 'abbreviation',
                        'value': value,
                        'suggestion': self.test_mappings[value_lower]
                    })
        
        issues_summary['abbreviations'] = abbreviation_issues
        
        # Check for source suffixes
        suffix_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                for suffix in self.source_suffixes:
                    if value.lower().endswith(suffix):
                        suffix_issues += 1
                        sample_issues.append({
                            'type': 'source_suffix',
                            'value': value,
                            'issue': f"Contains suffix '{suffix}'"
                        })
                        break
        
        issues_summary['source_suffixes'] = suffix_issues
        
        # Check for inconsistent formatting
        formatting_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                if re.search(r'\s{2,}', value) or value != value.strip():
                    formatting_issues += 1
                    sample_issues.append({
                        'type': 'formatting',
                        'value': value,
                        'issue': 'Inconsistent spacing or trimming needed'
                    })
        
        issues_summary['formatting_issues'] = formatting_issues
        
        # Calculate quality score
        total_issues = sum(issues_summary.values())
        quality_score = max(0, 1 - (total_issues / len(sample_data)))
        
        # Generate recommendations
        if spelling_issues > 0:
            recommendations.append("Apply spelling correction for common medical test names")
        if abbreviation_issues > 0:
            recommendations.append("Expand abbreviated test names to full forms")
        if suffix_issues > 0:
            recommendations.append("Remove source-specific suffixes from test names")
        if formatting_issues > 0:
            recommendations.append("Normalize text formatting and spacing")
        
        return ColumnAnalysis(
            column_name=self.column_name,
            total_rows=total_rows,
            null_count=null_count,
            unique_count=unique_count,
            quality_score=quality_score,
            issues_summary=issues_summary,
            recommendations=recommendations,
            sample_issues=sample_issues[:10]  # Limit to first 10 examples
        )
    
    def clean_value(self, value: Any, context: Dict[str, Any] = None) -> ProcessingResult:
        """Clean a single test name value"""
        original_value = value
        transformations_applied = []
        issues_found = []
        
        # Handle null/empty values
        if pd.isna(value) or value == '':
            return ProcessingResult(
                original_value=original_value,
                cleaned_value=value,
                confidence=1.0,
                transformations_applied=[],
                issues_found=['null_or_empty'],
                metadata={'processing_notes': 'No cleaning needed for null/empty value'}
            )
        
        # Convert to string and normalize
        cleaned_value = str(value)
        
        # Step 1: Basic text normalization
        normalized = self._normalize_text(cleaned_value)
        if normalized != cleaned_value:
            cleaned_value = normalized
            transformations_applied.append('normalize_text')
        
        # Step 2: Remove source suffixes
        original_cleaned = cleaned_value
        for suffix in self.source_suffixes:
            if cleaned_value.lower().endswith(suffix):
                cleaned_value = cleaned_value[:-len(suffix)].strip()
                transformations_applied.append('remove_source_suffix')
                issues_found.append(f'removed_suffix_{suffix}')
                break
        
        # Step 3: Fix spelling errors
        cleaned_lower = cleaned_value.lower()
        for wrong, correct in self.spelling_corrections.items():
            if wrong in cleaned_lower:
                cleaned_value = re.sub(re.escape(wrong), correct, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('fix_spelling')
                issues_found.append(f'spelling_error_{wrong}')
        
        # Step 4: Apply test name mappings
        cleaned_lower = cleaned_value.lower().strip()
        if cleaned_lower in self.test_mappings:
            cleaned_value = self.test_mappings[cleaned_lower]
            transformations_applied.append('standardize_test_name')
            issues_found.append('expanded_abbreviation')
        
        # Step 5: Final formatting
        cleaned_value = cleaned_value.strip()
        
        # Calculate confidence
        confidence = self._calculate_confidence(original_value, cleaned_value, transformations_applied)
        
        return ProcessingResult(
            original_value=original_value,
            cleaned_value=cleaned_value,
            confidence=confidence,
            transformations_applied=transformations_applied,
            issues_found=issues_found,
            metadata={
                'processing_notes': f'Applied {len(transformations_applied)} transformations',
                'original_length': len(str(original_value)),
                'cleaned_length': len(str(cleaned_value))
            }
        )
    
    def get_validation_rules(self) -> Dict[str, Any]:
        """Get validation rules for test column"""
        return {
            'data_type': 'string',
            'max_length': 200,
            'min_length': 2,
            'allowed_patterns': [
                r'^[A-Za-z0-9\s\-\(\)\/]+$',  # Alphanumeric with common punctuation
            ],
            'required_field': True,
            'standardized_values': list(self.test_mappings.values()),
            'common_abbreviations': list(self.test_mappings.keys())
        }
