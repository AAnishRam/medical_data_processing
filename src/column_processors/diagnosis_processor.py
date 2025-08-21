"""
Clinical Diagnosis Processor
Handles provisional and final diagnosis cleaning and standardization
"""

import pandas as pd
import re
from typing import Dict, List, Any
from .base_processor import BaseColumnProcessor, ProcessingResult, ColumnAnalysis

class ClinicalDiagnosisProcessor(BaseColumnProcessor):
    """
    Processor for diagnosis columns (provisional and final)
    Handles medical terminology, abbreviations, and clinical phrase standardization
    """
    
    def __init__(self, column_name: str = "provisionaldiagnosis"):
        description = {
            "provisionaldiagnosis": "Clinician's provisional diagnosis or notes at the visit",
            "finaldiagnosis": "Final diagnosis after evaluation"
        }
        
        super().__init__(
            column_name=column_name,
            column_description=description.get(column_name, "Clinical diagnosis information")
        )
        
        # Medical condition mappings
        self.condition_mappings = {
            # Cardiovascular
            'htn': 'Hypertension',
            'hypertention': 'Hypertension',
            'high bp': 'Hypertension',
            'high blood pressure': 'Hypertension',
            'mi': 'Myocardial Infarction',
            'heart attack': 'Myocardial Infarction',
            'cad': 'Coronary Artery Disease',
            'chf': 'Congestive Heart Failure',
            'heart failure': 'Congestive Heart Failure',
            'afib': 'Atrial Fibrillation',
            'a fib': 'Atrial Fibrillation',
            'dvt': 'Deep Vein Thrombosis',
            'pe': 'Pulmonary Embolism',
            
            # Endocrine
            'dm': 'Diabetes Mellitus',
            'dm2': 'Diabetes Mellitus Type 2',
            'diabetes': 'Diabetes Mellitus',
            'diabetis': 'Diabetes Mellitus',
            'diabetic': 'Diabetes Mellitus',
            'iddm': 'Insulin Dependent Diabetes Mellitus',
            'niddm': 'Non-Insulin Dependent Diabetes Mellitus',
            'hypothyroid': 'Hypothyroidism',
            'hyperthyroid': 'Hyperthyroidism',
            
            # Respiratory
            'copd': 'Chronic Obstructive Pulmonary Disease',
            'asthma': 'Asthma',
            'asthama': 'Asthma',
            'pneumonia': 'Pneumonia',
            'pneumnia': 'Pneumonia',
            'bronchitis': 'Bronchitis',
            'bronchitus': 'Bronchitis',
            'tb': 'Tuberculosis',
            'urti': 'Upper Respiratory Tract Infection',
            'uri': 'Upper Respiratory Tract Infection',
            
            # Gastrointestinal
            'gerd': 'Gastroesophageal Reflux Disease',
            'acid reflux': 'Gastroesophageal Reflux Disease',
            'ibs': 'Irritable Bowel Syndrome',
            'ibd': 'Inflammatory Bowel Disease',
            'gastritis': 'Gastritis',
            'peptic ulcer': 'Peptic Ulcer Disease',
            
            # Renal
            'ckd': 'Chronic Kidney Disease',
            'kidney disease': 'Chronic Kidney Disease',
            'renal failure': 'Renal Failure',
            'kidney failure': 'Renal Failure',
            'uti': 'Urinary Tract Infection',
            
            # Neurological
            'cva': 'Cerebrovascular Accident',
            'stroke': 'Cerebrovascular Accident',
            'tia': 'Transient Ischemic Attack',
            'seizure': 'Seizure Disorder',
            'epilepsy': 'Epilepsy',
            'migraine': 'Migraine',
            'headache': 'Headache',
            
            # Psychiatric
            'depression': 'Depression',
            'anxiety': 'Anxiety Disorder',
            'ptsd': 'Post-Traumatic Stress Disorder',
            'bipolar': 'Bipolar Disorder',
            
            # Musculoskeletal
            'oa': 'Osteoarthritis',
            'osteoarthritis': 'Osteoarthritis',
            'ra': 'Rheumatoid Arthritis',
            'rheumatoid arthritis': 'Rheumatoid Arthritis',
            'back pain': 'Back Pain',
            'joint pain': 'Arthralgia',
            
            # Infectious
            'covid': 'COVID-19',
            'covid-19': 'COVID-19',
            'sars-cov-2': 'COVID-19',
            'flu': 'Influenza',
            'influenza': 'Influenza',
        }
        
        # Medical abbreviations
        self.medical_abbreviations = {
            'w/': 'with',
            'w/o': 'without',
            'r/o': 'rule out',
            'h/o': 'history of',
            'f/u': 'follow up',
            'c/o': 'complains of',
            's/p': 'status post',
            'd/c': 'discontinue',
            'prn': 'as needed',
            'bid': 'twice daily',
            'tid': 'three times daily',
            'qid': 'four times daily',
            'qod': 'every other day',
            'hs': 'at bedtime',
            'ac': 'before meals',
            'pc': 'after meals',
        }
        
        # Common spelling errors in medical terminology
        self.medical_spelling_corrections = {
            'diabetis': 'diabetes',
            'hypertention': 'hypertension',
            'asthama': 'asthma',
            'bronchitus': 'bronchitis',
            'pneumnia': 'pneumonia',
            'infaction': 'infarction',
            'arthritus': 'arthritis',
            'gastritus': 'gastritis',
            'migrene': 'migraine',
            'anxeity': 'anxiety',
            'depresion': 'depression',
            'hipothyroid': 'hypothyroid',
            'hypotension': 'hypotension',
            'cronary': 'coronary',
            'pulmonery': 'pulmonary',
            'cerebal': 'cerebral',
            'cardio': 'cardiac',
        }
        
        # Severity indicators
        self.severity_terms = {
            'mild': 'Mild',
            'moderate': 'Moderate',
            'severe': 'Severe',
            'acute': 'Acute',
            'chronic': 'Chronic',
            'stable': 'Stable',
            'unstable': 'Unstable',
            'controlled': 'Controlled',
            'uncontrolled': 'Uncontrolled',
        }
    
    def analyze_column(self, series: pd.Series, sample_size: int = 100) -> ColumnAnalysis:
        """Analyze diagnosis column for issues and patterns"""
        
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
        
        # Check for spelling issues in medical terms
        spelling_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                value_lower = value.lower()
                for wrong, correct in self.medical_spelling_corrections.items():
                    if wrong in value_lower:
                        spelling_issues += 1
                        sample_issues.append({
                            'type': 'medical_spelling_error',
                            'value': value,
                            'issue': f"Contains '{wrong}' should be '{correct}'"
                        })
                        break
        
        issues_summary['medical_spelling_errors'] = spelling_issues
        
        # Check for abbreviations that could be expanded
        abbreviation_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                value_lower = value.lower()
                for abbrev in self.condition_mappings.keys():
                    if len(abbrev) <= 5 and f' {abbrev} ' in f' {value_lower} ':
                        abbreviation_issues += 1
                        sample_issues.append({
                            'type': 'medical_abbreviation',
                            'value': value,
                            'suggestion': f"Expand '{abbrev}' to '{self.condition_mappings[abbrev]}'"
                        })
                        break
        
        issues_summary['medical_abbreviations'] = abbreviation_issues
        
        # Check for non-medical abbreviations
        clinical_abbreviation_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                for abbrev in self.medical_abbreviations.keys():
                    if abbrev in value.lower():
                        clinical_abbreviation_issues += 1
                        sample_issues.append({
                            'type': 'clinical_abbreviation',
                            'value': value,
                            'suggestion': f"Expand '{abbrev}' to '{self.medical_abbreviations[abbrev]}'"
                        })
                        break
        
        issues_summary['clinical_abbreviations'] = clinical_abbreviation_issues
        
        # Check for formatting issues
        formatting_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                # Multiple spaces, inconsistent punctuation
                if re.search(r'\s{2,}', value) or re.search(r'[,;]{2,}', value):
                    formatting_issues += 1
                    sample_issues.append({
                        'type': 'formatting',
                        'value': value,
                        'issue': 'Inconsistent spacing or punctuation'
                    })
        
        issues_summary['formatting_issues'] = formatting_issues
        
        # Check for incomplete or very short diagnoses
        incomplete_issues = 0
        for value in sample_data:
            if isinstance(value, str):
                if len(value.strip()) < 3 or value.lower().strip() in ['na', 'n/a', 'nil', 'none', '?']:
                    incomplete_issues += 1
                    sample_issues.append({
                        'type': 'incomplete_diagnosis',
                        'value': value,
                        'issue': 'Incomplete or placeholder diagnosis'
                    })
        
        issues_summary['incomplete_diagnoses'] = incomplete_issues
        
        # Calculate quality score
        total_issues = sum(issues_summary.values())
        quality_score = max(0, 1 - (total_issues / len(sample_data)))
        
        # Generate recommendations
        if spelling_issues > 0:
            recommendations.append("Fix spelling errors in medical terminology")
        if abbreviation_issues > 0:
            recommendations.append("Expand medical condition abbreviations")
        if clinical_abbreviation_issues > 0:
            recommendations.append("Expand clinical abbreviations for clarity")
        if formatting_issues > 0:
            recommendations.append("Standardize diagnosis formatting and punctuation")
        if incomplete_issues > 0:
            recommendations.append("Review and complete incomplete diagnoses")
        
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
        """Clean a single diagnosis value"""
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
        
        # Skip if it's a placeholder
        if cleaned_value.lower() in ['na', 'n/a', 'nil', 'none', '?', 'unknown']:
            issues_found.append('placeholder_value')
            return ProcessingResult(
                original_value=original_value,
                cleaned_value=cleaned_value,
                confidence=0.3,
                transformations_applied=[],
                issues_found=issues_found,
                metadata={'processing_notes': 'Placeholder value detected'}
            )
        
        # Step 1: Basic text normalization
        normalized = self._normalize_text(cleaned_value)
        if normalized != cleaned_value:
            cleaned_value = normalized
            transformations_applied.append('normalize_text')
        
        # Step 2: Fix medical spelling errors
        for wrong, correct in self.medical_spelling_corrections.items():
            if wrong in cleaned_value.lower():
                cleaned_value = re.sub(re.escape(wrong), correct, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('fix_medical_spelling')
                issues_found.append(f'medical_spelling_error_{wrong}')
        
        # Step 3: Expand clinical abbreviations
        for abbrev, expansion in self.medical_abbreviations.items():
            pattern = r'\b' + re.escape(abbrev) + r'\b'
            if re.search(pattern, cleaned_value, re.IGNORECASE):
                cleaned_value = re.sub(pattern, expansion, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('expand_clinical_abbreviation')
                issues_found.append(f'clinical_abbreviation_{abbrev}')
        
        # Step 4: Standardize medical conditions
        for condition, standard in self.condition_mappings.items():
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(condition) + r'\b'
            if re.search(pattern, cleaned_value, re.IGNORECASE):
                cleaned_value = re.sub(pattern, standard, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('standardize_medical_condition')
                issues_found.append(f'medical_condition_standardized_{condition}')
        
        # Step 5: Standardize severity terms
        for severity, standard in self.severity_terms.items():
            pattern = r'\b' + re.escape(severity) + r'\b'
            if re.search(pattern, cleaned_value, re.IGNORECASE):
                cleaned_value = re.sub(pattern, standard, cleaned_value, flags=re.IGNORECASE)
                transformations_applied.append('standardize_severity')
        
        # Step 6: Clean up punctuation and spacing
        # Fix multiple commas/semicolons
        cleaned_value = re.sub(r'[,;]{2,}', ',', cleaned_value)
        # Fix spacing around punctuation
        cleaned_value = re.sub(r'\s*,\s*', ', ', cleaned_value)
        cleaned_value = re.sub(r'\s*;\s*', '; ', cleaned_value)
        # Remove trailing punctuation if it doesn't make sense
        cleaned_value = re.sub(r'[,;]\s*$', '', cleaned_value)
        
        if cleaned_value != str(value).strip():
            transformations_applied.append('fix_punctuation')
        
        # Final cleanup
        cleaned_value = cleaned_value.strip()
        
        # Calculate confidence
        confidence = self._calculate_confidence(original_value, cleaned_value, transformations_applied)
        
        # Adjust confidence based on content quality
        if len(cleaned_value) < 5:
            confidence *= 0.7  # Lower confidence for very short diagnoses
        
        return ProcessingResult(
            original_value=original_value,
            cleaned_value=cleaned_value,
            confidence=confidence,
            transformations_applied=transformations_applied,
            issues_found=issues_found,
            metadata={
                'processing_notes': f'Applied {len(transformations_applied)} transformations',
                'original_length': len(str(original_value)),
                'cleaned_length': len(str(cleaned_value)),
                'condition_count': len([c for c in self.condition_mappings.values() 
                                      if c.lower() in cleaned_value.lower()])
            }
        )
    
    def get_validation_rules(self) -> Dict[str, Any]:
        """Get validation rules for diagnosis column"""
        return {
            'data_type': 'string',
            'max_length': 500,
            'min_length': 3,
            'allowed_patterns': [
                r'^[A-Za-z0-9\s\-\(\),;\.\/]+$',  # Medical text with common punctuation
            ],
            'required_field': True,
            'standardized_conditions': list(self.condition_mappings.values()),
            'common_abbreviations': list(self.condition_mappings.keys()),
            'should_not_be_placeholder': True,
            'medical_terminology_focused': True
        }
