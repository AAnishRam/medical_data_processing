"""
Biomarker Column Processor
Handles biomarker/parameter name cleaning and standardization
"""

import pandas as pd
import re
from typing import Dict, List, Any
from .base_processor import BaseColumnProcessor, ProcessingResult, ColumnAnalysis

class BiomarkerColumnProcessor(BaseColumnProcessor):
    """
    Processor for 'biomarker' column - specific biomarker/parameter names
    Handles abbreviations, non-standard names, and standardization
    """
    
    def __init__(self):
        super().__init__(
            column_name="biomarker",
            column_description="Specific biomarker/parameter measured within the test"
        )
        
        # Standard biomarker mappings
        self.biomarker_mappings = {
            # Hematology
            'hb': 'Hemoglobin',
            'hgb': 'Hemoglobin',
            'hemoglobin': 'Hemoglobin',
            'hct': 'Hematocrit',
            'hematocrit': 'Hematocrit',
            'wbc': 'White Blood Cell Count',
            'rbc': 'Red Blood Cell Count',
            'plt': 'Platelet Count',
            'platelets': 'Platelet Count',
            'mcv': 'Mean Corpuscular Volume',
            'mch': 'Mean Corpuscular Hemoglobin',
            'mchc': 'Mean Corpuscular Hemoglobin Concentration',
            
            # Chemistry - Basic
            'glu': 'Glucose',
            'glucose': 'Glucose',
            'sugar': 'Glucose',
            'bun': 'Blood Urea Nitrogen',
            'creatinine': 'Creatinine',
            'crea': 'Creatinine',
            'cr': 'Creatinine',
            'urea': 'Blood Urea Nitrogen',
            
            # Electrolytes
            'na': 'Sodium',
            'sodium': 'Sodium',
            'k': 'Potassium',
            'potassium': 'Potassium',
            'cl': 'Chloride',
            'chloride': 'Chloride',
            'co2': 'Carbon Dioxide',
            'hco3': 'Bicarbonate',
            'bicarbonate': 'Bicarbonate',
            
            # Liver Function
            'alt': 'Alanine Aminotransferase',
            'ast': 'Aspartate Aminotransferase',
            'alp': 'Alkaline Phosphatase',
            'alkphos': 'Alkaline Phosphatase',
            'bilirubin': 'Total Bilirubin',
            'bili': 'Total Bilirubin',
            'albumin': 'Albumin',
            'alb': 'Albumin',
            'protein': 'Total Protein',
            
            # Lipids
            'chol': 'Total Cholesterol',
            'cholesterol': 'Total Cholesterol',
            'hdl': 'HDL Cholesterol',
            'ldl': 'LDL Cholesterol',
            'trig': 'Triglycerides',
            'triglycerides': 'Triglycerides',
            
            # Cardiac
            'troponin': 'Troponin',
            'trop': 'Troponin',
            'ck': 'Creatine Kinase',
            'ckmb': 'CK-MB',
            'bnp': 'B-type Natriuretic Peptide',
            'ntprobnp': 'NT-proBNP',
            
            # Thyroid
            'tsh': 'Thyroid Stimulating Hormone',
            't3': 'Triiodothyronine',
            't4': 'Thyroxine',
            'ft3': 'Free T3',
            'ft4': 'Free T4',
            
            # Diabetes
            'hba1c': 'Hemoglobin A1c',
            'a1c': 'Hemoglobin A1c',
            'insulin': 'Insulin',
            'c-peptide': 'C-Peptide',
            
            # Inflammation
            'esr': 'Erythrocyte Sedimentation Rate',
            'crp': 'C-Reactive Protein',
            'sed rate': 'Erythrocyte Sedimentation Rate',
            
            # Vitamins
            'vit d': 'Vitamin D',
            'vitamin d': 'Vitamin D',
            'vit b12': 'Vitamin B12',
            'b12': 'Vitamin B12',
            'folate': 'Folate',
            'folic acid': 'Folate',
        }
        
        # Common spelling errors
        self.spelling_corrections = {
            'hemoglobine': 'hemoglobin',
            'creatinin': 'creatinine',
            'createnine': 'creatinine',
            'glucos': 'glucose',
            'cholestrol': 'cholesterol',
            'triglicerides': 'triglycerides',
            'billirubin': 'bilirubin',
            'albumen': 'albumin',
            'sodiums': 'sodium',
            'potasium': 'potassium',
            'cloride': 'chloride',
        }
        
        # Units to separate from biomarker names
        self.common_units = [
            'mg/dl', 'mg/dL', 'mmol/L', 'g/dl', 'g/dL', 'mEq/L', 'ng/ml',
            'pg/ml', 'IU/L', 'U/L', 'mU/L', 'ng/dL', 'ug/dl', 'mcg/dl',
            '%', 'ratio', 'index', '/hpf', '/lpf', 'cells/ul'
        ]
    
    def analyze_column(self, series: pd.Series, sample_size: int = 100) -> ColumnAnalysis:
        """Analyze biomarker column for issues and patterns"""
        
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
        
        # Check for abbreviations that could be expanded
        abbreviation_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                value_lower = value.lower().strip()
                if value_lower in self.biomarker_mappings and len(value_lower) <= 6:
                    abbreviation_issues += 1
                    sample_issues.append({
                        'type': 'abbreviation',
                        'value': value,
                        'suggestion': self.biomarker_mappings[value_lower]
                    })
        
        issues_summary['abbreviations'] = abbreviation_issues
        
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
        
        # Check for units mixed with biomarker names
        unit_mixing_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                for unit in self.common_units:
                    if unit.lower() in value.lower():
                        unit_mixing_issues += 1
                        sample_issues.append({
                            'type': 'unit_mixing',
                            'value': value,
                            'issue': f"Unit '{unit}' should be in result column, not biomarker"
                        })
                        break
        
        issues_summary['unit_mixing'] = unit_mixing_issues
        
        # Check for non-standard formatting
        formatting_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                # Check for issues like multiple spaces, weird characters
                if re.search(r'\s{2,}', value) or re.search(r'[^\w\s\-\(\)\/]', value):
                    formatting_issues += 1
                    sample_issues.append({
                        'type': 'formatting',
                        'value': value,
                        'issue': 'Non-standard formatting or characters'
                    })
        
        issues_summary['formatting_issues'] = formatting_issues
        
        # Check for case inconsistencies
        case_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                if value != value.strip() or (value.islower() and len(value) > 3):
                    case_issues += 1
                    sample_issues.append({
                        'type': 'case_inconsistency',
                        'value': value,
                        'issue': 'Inconsistent capitalization'
                    })
        
        issues_summary['case_issues'] = case_issues
        
        # Calculate quality score
        total_issues = sum(issues_summary.values())
        quality_score = max(0, 1 - (total_issues / len(sample_data)))
        
        # Generate recommendations
        if abbreviation_issues > 0:
            recommendations.append("Expand abbreviated biomarker names to standard forms")
        if spelling_issues > 0:
            recommendations.append("Fix spelling errors in biomarker names")
        if unit_mixing_issues > 0:
            recommendations.append("Separate units from biomarker names")
        if formatting_issues > 0:
            recommendations.append("Standardize biomarker name formatting")
        if case_issues > 0:
            recommendations.append("Apply consistent capitalization")
        
        return ColumnAnalysis(
            column_name=self.column_name,
            total_rows=total_rows,
            null_count=null_count,
            unique_count=unique_count,
            quality_score=quality_score,
            issues_summary=issues_summary,
            recommendations=recommendations,
            sample_issues=sample_issues[:10]
        )
    
    def clean_value(self, value: Any, context: Dict[str, Any] = None) -> ProcessingResult:
        """Clean a single biomarker value"""
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
        cleaned_value = str(value).strip()
        
        # Step 1: Basic text normalization
        normalized = self._normalize_text(cleaned_value)
        if normalized != cleaned_value:
            cleaned_value = normalized
            transformations_applied.append('normalize_text')
        
        # Step 2: Remove units if present (they should be in result column)
        for unit in self.common_units:
            unit_pattern = re.escape(unit)
            if re.search(unit_pattern, cleaned_value, re.IGNORECASE):
                cleaned_value = re.sub(unit_pattern, '', cleaned_value, flags=re.IGNORECASE).strip()
                transformations_applied.append('remove_unit')
                issues_found.append(f'removed_unit_{unit}')
        
        # Step 3: Fix spelling errors
        cleaned_lower = cleaned_value.lower()
        for wrong, correct in self.spelling_corrections.items():
            if wrong in cleaned_lower:
                cleaned_value = re.sub(re.escape(wrong), correct, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('fix_spelling')
                issues_found.append(f'spelling_error_{wrong}')
        
        # Step 4: Apply biomarker standardization
        cleaned_lower = cleaned_value.lower().strip()
        if cleaned_lower in self.biomarker_mappings:
            cleaned_value = self.biomarker_mappings[cleaned_lower]
            transformations_applied.append('standardize_biomarker')
            issues_found.append('expanded_abbreviation')
        else:
            # Try partial matching for common patterns
            for abbrev, full_name in self.biomarker_mappings.items():
                if abbrev in cleaned_lower and len(abbrev) >= 3:
                    # Be careful with partial matches
                    if cleaned_lower == abbrev or cleaned_lower.startswith(abbrev + ' '):
                        cleaned_value = full_name
                        transformations_applied.append('standardize_biomarker_partial')
                        issues_found.append('expanded_abbreviation_partial')
                        break
        
        # Step 5: Fix capitalization
        if cleaned_value.islower() and len(cleaned_value) > 2:
            # Apply title case for biomarker names
            cleaned_value = cleaned_value.title()
            transformations_applied.append('fix_capitalization')
            issues_found.append('case_inconsistency')
        
        # Final cleanup
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
        """Get validation rules for biomarker column"""
        return {
            'data_type': 'string',
            'max_length': 150,
            'min_length': 1,
            'allowed_patterns': [
                r'^[A-Za-z0-9\s\-\(\)]+$',  # Alphanumeric with basic punctuation
            ],
            'required_field': True,
            'standardized_values': list(self.biomarker_mappings.values()),
            'common_abbreviations': list(self.biomarker_mappings.keys()),
            'should_not_contain_units': True,
            'should_not_contain_values': True
        }
