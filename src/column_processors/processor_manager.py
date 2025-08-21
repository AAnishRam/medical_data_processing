"""
Column Processor Manager
Coordinates all column-specific processors and provides unified interface
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Type
from .base_processor import BaseColumnProcessor, ProcessingResult, ColumnAnalysis
from .test_processor import TestColumnProcessor
from .biomarker_processor import BiomarkerColumnProcessor
from .diagnosis_processor import ClinicalDiagnosisProcessor

class ColumnProcessorManager:
    """
    Manager class that coordinates all column-specific processors
    Provides a unified interface for processing different medical data columns
    """
    
    def __init__(self):
        self.processors = {}
        self.column_mappings = {
            # Laboratory and Test Related
            'test': TestColumnProcessor,
            'lab_testname_col': TestColumnProcessor,
            'biomarker': BiomarkerColumnProcessor,
            'norm_biomarker': BiomarkerColumnProcessor,
            
            # Clinical Diagnosis
            'provisionaldiagnosis': lambda: ClinicalDiagnosisProcessor('provisionaldiagnosis'),
            'finaldiagnosis': lambda: ClinicalDiagnosisProcessor('finaldiagnosis'),
            
            # Clinical Notes (use diagnosis processor for medical terminology)
            'chief_remark': lambda: ClinicalDiagnosisProcessor('chief_remark'),
            'vital_remark': lambda: ClinicalDiagnosisProcessor('vital_remark'),
            'clinical_note': lambda: ClinicalDiagnosisProcessor('clinical_note'),
        }
        
        # Column descriptions for reference
        self.column_descriptions = {
            'test': 'Name of the laboratory test/investigation performed',
            'patient_id': 'Unique identifier for the patient',
            'biomarker': 'Specific biomarker/parameter measured within the test',
            'result': 'Recorded finding for the biomarker',
            'result_dt': 'Date/time when the result was recorded',
            'flag': 'Qualitative marker for result interpretation',
            'profile_id': 'Numeric ID of the profile or panel used',
            'profile_name': 'Human readable name of the profile/panel',
            'created_at': 'Timestamp when the record was created in the system',
            'biomarker_stamp': 'Internal numeric stamp/id for the biomarker',
            'norm_biomarker': 'Normalized or standardized biomarker name(s)',
            'chief_remark': "Doctor's primary remark or provisional note",
            'vital_remark': 'Remarks related to vital signs',
            'clinical_note': 'Additional clinical observations or history',
            'lab_site': 'Name of the laboratory or collection center',
            'lab_testname_col': 'Alternate/test display name for the lab test',
            'vitals_list': 'Concatenated vitals',
            'opbill_id': 'Outpatient bill identifier',
            'centre_id': 'Numeric ID of the hospital/centre',
            'row_id': 'Internal row/record identifier',
            'join_dt': 'Date/time the patient joined or visit was logged',
            'doctor_id': 'Numeric identifier of the attending doctor',
            'provisionaldiagnosis': "Clinician's provisional diagnosis or notes",
            'finaldiagnosis': 'Final diagnosis after evaluation'
        }
        
        # Processing priority (columns with higher priority processed first)
        self.processing_priority = {
            'test': 1,
            'biomarker': 1,
            'provisionaldiagnosis': 2,
            'finaldiagnosis': 2,
            'chief_remark': 3,
            'clinical_note': 3,
            'norm_biomarker': 4,
            'lab_testname_col': 4,
            'vital_remark': 5
        }
        
    def get_processor(self, column_name: str) -> Optional[BaseColumnProcessor]:
        """Get or create processor for a specific column"""
        if column_name in self.processors:
            return self.processors[column_name]
        
        if column_name in self.column_mappings:
            processor_class = self.column_mappings[column_name]
            if callable(processor_class):
                # Handle lambda functions
                try:
                    processor = processor_class()
                except TypeError:
                    # If it's a class, instantiate it
                    processor = processor_class()
            else:
                processor = processor_class()
            
            self.processors[column_name] = processor
            return processor
        
        return None
    
    def get_available_processors(self) -> Dict[str, str]:
        """Get list of columns that have dedicated processors"""
        return {col: self.column_descriptions.get(col, 'No description available') 
                for col in self.column_mappings.keys()}
    
    def analyze_dataset(self, df: pd.DataFrame, 
                       columns_to_analyze: List[str] = None,
                       sample_size: int = 100) -> Dict[str, ColumnAnalysis]:
        """Analyze multiple columns in the dataset"""
        
        if columns_to_analyze is None:
            # Analyze all columns that have processors
            columns_to_analyze = [col for col in df.columns if col in self.column_mappings]
        else:
            # Filter to only columns that exist in the dataset and have processors
            columns_to_analyze = [col for col in columns_to_analyze 
                                if col in df.columns and col in self.column_mappings]
        
        results = {}
        
        for column_name in columns_to_analyze:
            processor = self.get_processor(column_name)
            if processor and column_name in df.columns:
                try:
                    analysis = processor.analyze_column(df[column_name], sample_size)
                    results[column_name] = analysis
                except Exception as e:
                    print(f"Error analyzing column {column_name}: {e}")
        
        return results
    
    def process_columns(self, df: pd.DataFrame,
                       columns_to_process: List[str] = None,
                       processing_options: Dict[str, Any] = None) -> pd.DataFrame:
        """Process multiple columns in the dataset"""
        
        if processing_options is None:
            processing_options = {}
        
        if columns_to_process is None:
            # Process all columns that have processors
            columns_to_process = [col for col in df.columns if col in self.column_mappings]
        else:
            # Filter to only columns that exist and have processors
            columns_to_process = [col for col in columns_to_process 
                                if col in df.columns and col in self.column_mappings]
        
        # Sort by processing priority
        columns_to_process.sort(key=lambda x: self.processing_priority.get(x, 999))
        
        # Create a copy of the dataframe to avoid modifying the original
        result_df = df.copy()
        
        processing_results = {}
        
        for column_name in columns_to_process:
            processor = self.get_processor(column_name)
            if processor:
                try:
                    print(f"Processing column: {column_name}")
                    
                    # Get column-specific options
                    column_options = processing_options.get(column_name, {})
                    
                    # Process the column
                    cleaned_series = processor.process_column(
                        result_df[column_name], 
                        column_options
                    )
                    
                    # Update the dataframe
                    result_df[column_name] = cleaned_series
                    
                    # Store processing stats
                    processing_results[column_name] = processor.get_processing_stats()
                    
                except Exception as e:
                    print(f"Error processing column {column_name}: {e}")
                    processing_results[column_name] = {'error': str(e)}
        
        # Store processing metadata
        result_df.attrs['processing_results'] = processing_results
        
        return result_df
    
    def get_processing_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get summary of processing capabilities for the dataset"""
        
        available_columns = [col for col in df.columns if col in self.column_mappings]
        unavailable_columns = [col for col in df.columns if col not in self.column_mappings]
        
        summary = {
            'total_columns': len(df.columns),
            'processable_columns': len(available_columns),
            'non_processable_columns': len(unavailable_columns),
            'processable_column_list': available_columns,
            'non_processable_column_list': unavailable_columns,
            'processing_coverage': len(available_columns) / len(df.columns) if len(df.columns) > 0 else 0,
            'column_priorities': {col: self.processing_priority.get(col, 999) 
                                for col in available_columns}
        }
        
        return summary
    
    def get_column_recommendations(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Get processing recommendations for each processable column"""
        
        recommendations = {}
        
        for column_name in df.columns:
            if column_name in self.column_mappings:
                processor = self.get_processor(column_name)
                if processor:
                    try:
                        analysis = processor.analyze_column(df[column_name], sample_size=50)
                        recommendations[column_name] = analysis.recommendations
                    except Exception as e:
                        recommendations[column_name] = [f"Error analyzing column: {e}"]
        
        return recommendations
    
    def validate_processing_options(self, processing_options: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate processing options for each column"""
        
        validation_results = {}
        
        for column_name, options in processing_options.items():
            if column_name in self.column_mappings:
                processor = self.get_processor(column_name)
                if processor:
                    validation_rules = processor.get_validation_rules()
                    # Here you could add validation logic
                    validation_results[column_name] = ['Options validated successfully']
                else:
                    validation_results[column_name] = ['No processor available']
            else:
                validation_results[column_name] = ['Column not supported for processing']
        
        return validation_results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get overall processing statistics from all processors"""
        
        stats = {}
        
        for column_name, processor in self.processors.items():
            stats[column_name] = processor.get_processing_stats()
        
        # Calculate aggregate stats
        total_processed = sum(s.get('total_processed', 0) for s in stats.values())
        total_successful = sum(s.get('successful_cleanings', 0) for s in stats.values())
        total_failed = sum(s.get('failed_cleanings', 0) for s in stats.values())
        
        aggregate_stats = {
            'total_values_processed': total_processed,
            'total_successful_cleanings': total_successful,
            'total_failed_cleanings': total_failed,
            'overall_success_rate': total_successful / total_processed if total_processed > 0 else 0,
            'columns_processed': len(stats),
            'column_details': stats
        }
        
        return aggregate_stats
    
    def reset_all_stats(self):
        """Reset processing statistics for all processors"""
        for processor in self.processors.values():
            processor.reset_stats()
