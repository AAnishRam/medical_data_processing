"""
Column Processors Package
Modular column-specific processing for medical data
"""

from .base_processor import BaseColumnProcessor, ProcessingResult, ColumnAnalysis
from .test_processor import TestColumnProcessor
from .biomarker_processor import BiomarkerColumnProcessor
from .diagnosis_processor import ClinicalDiagnosisProcessor
from .processor_manager import ColumnProcessorManager

__all__ = [
    'BaseColumnProcessor',
    'ProcessingResult', 
    'ColumnAnalysis',
    'TestColumnProcessor',
    'BiomarkerColumnProcessor', 
    'ClinicalDiagnosisProcessor',
    'ColumnProcessorManager'
]

__version__ = '1.0.0'
__author__ = 'Multi-Agent Medical Data Processing System'
