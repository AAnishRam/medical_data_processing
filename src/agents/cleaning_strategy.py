import re
import pandas as pd
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass

@dataclass
class CleaningAction:
    action_type: str
    original_value: str
    cleaned_value: str
    confidence: float
    method_used: str

class CleaningStrategyAgent:
    """Agent for determining optimal cleaning approaches"""
    
    def __init__(self):
        self.strategies = {
            'spelling_error': self._fix_spelling_errors,
            'formatting': self._fix_formatting_issues,
            'abbreviation': self._expand_abbreviations,
            'standardization': self._standardize_medical_terms,
            'missing_value': self._handle_missing_values,
            'medical_typo': self._fix_medical_typos
        }
        
        self.priority_order = [
            'formatting',          # Quick fixes first
            'medical_typo',        # Fix obvious medical typos
            'abbreviation',        # Expand abbreviations
            'spelling_error',      # General spelling
            'standardization',     # Standardize terminology
            'missing_value'        # Handle missing last
        ]
        
        # Common medical typo corrections
        self.medical_typo_corrections = {
            'diabetis': 'diabetes',
            'hypertention': 'hypertension',
            'infaction': 'infarction',
            'pnemonia': 'pneumonia',
            'asthama': 'asthma',
            'bronchitus': 'bronchitis',
            'arthritus': 'arthritis',
            'apendix': 'appendix',
            'rhumatism': 'rheumatism',
            'stomache': 'stomach',
            'kydney': 'kidney',
            'hart': 'heart',
            'brane': 'brain'
        }
    
    def determine_strategy(self, issues: List) -> Dict[str, List]:
        """Group issues by cleaning strategy and prioritize them"""
        strategy_groups = {}
        
        for issue in issues:
            issue_type = getattr(issue, 'issue_type', 'unknown')
            if issue_type not in strategy_groups:
                strategy_groups[issue_type] = []
            strategy_groups[issue_type].append(issue)
        
        # Sort by priority
        prioritized_strategies = {}
        for strategy_type in self.priority_order:
            if strategy_type in strategy_groups:
                prioritized_strategies[strategy_type] = strategy_groups[strategy_type]
        
        # Add any remaining strategies
        for strategy_type, issues in strategy_groups.items():
            if strategy_type not in prioritized_strategies:
                prioritized_strategies[strategy_type] = issues
        
        return prioritized_strategies
    
    def apply_cleaning_strategy(self, text: str, strategy_type: str) -> CleaningAction:
        """Apply a specific cleaning strategy to text"""
        if strategy_type in self.strategies:
            return self.strategies[strategy_type](text)
        else:
            return CleaningAction(
                action_type='no_action',
                original_value=text,
                cleaned_value=text,
                confidence=1.0,
                method_used='none'
            )
    
    def _fix_formatting_issues(self, text: str) -> CleaningAction:
        """Fix basic formatting issues like extra spaces, punctuation"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction('formatting', text, text, 1.0, 'no_change')
        
        original = str(text)
        cleaned = original
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Fix common punctuation issues
        cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
        cleaned = re.sub(r'\s*\.\s*', '. ', cleaned)
        cleaned = re.sub(r'\s*;\s*', '; ', cleaned)
        
        # Remove trailing punctuation if inappropriate
        cleaned = re.sub(r'[,;]+$', '', cleaned)
        
        confidence = 0.95 if cleaned != original else 1.0
        
        return CleaningAction(
            action_type='formatting',
            original_value=original,
            cleaned_value=cleaned,
            confidence=confidence,
            method_used='regex_formatting'
        )
    
    def _fix_medical_typos(self, text: str) -> CleaningAction:
        """Fix known medical typos"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction('medical_typo', text, text, 1.0, 'no_change')
        
        original = str(text)
        cleaned = original.lower()
        changes_made = 0
        
        # Apply known corrections
        for typo, correction in self.medical_typo_corrections.items():
            pattern = r'\b' + re.escape(typo) + r'\b'
            if re.search(pattern, cleaned, re.IGNORECASE):
                cleaned = re.sub(pattern, correction, cleaned, flags=re.IGNORECASE)
                changes_made += 1
        
        # Preserve original casing style if possible
        if changes_made > 0 and original.isupper():
            cleaned = cleaned.upper()
        elif changes_made > 0 and original.istitle():
            cleaned = cleaned.title()
        
        confidence = 0.9 if changes_made > 0 else 1.0
        
        return CleaningAction(
            action_type='medical_typo',
            original_value=original,
            cleaned_value=cleaned,
            confidence=confidence,
            method_used=f'typo_correction_{changes_made}_changes'
        )
    
    def _expand_abbreviations(self, text: str) -> CleaningAction:
        """Expand medical abbreviations"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction('abbreviation', text, text, 1.0, 'no_change')
        
        original = str(text)
        cleaned = original
        expansions_made = 0
        
        abbreviation_map = {
            r'\bhtn\b': 'hypertension',
            r'\bdm\b': 'diabetes mellitus',
            r'\bmi\b': 'myocardial infarction',
            r'\bcopd\b': 'chronic obstructive pulmonary disease',
            r'\bbp\b': 'blood pressure',
            r'\bhr\b': 'heart rate',
            r'\brr\b': 'respiratory rate',
            r'\btemp\b': 'temperature',
            r'\bwbc\b': 'white blood cell count',
            r'\brbc\b': 'red blood cell count'
        }
        
        for pattern, expansion in abbreviation_map.items():
            if re.search(pattern, cleaned, re.IGNORECASE):
                cleaned = re.sub(pattern, expansion, cleaned, flags=re.IGNORECASE)
                expansions_made += 1
        
        confidence = 0.85 if expansions_made > 0 else 1.0
        
        return CleaningAction(
            action_type='abbreviation',
            original_value=original,
            cleaned_value=cleaned,
            confidence=confidence,
            method_used=f'abbreviation_expansion_{expansions_made}_changes'
        )
    
    def _fix_spelling_errors(self, text: str) -> CleaningAction:
        """Fix general spelling errors using pattern matching"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction('spelling_error', text, text, 1.0, 'no_change')
        
        original = str(text)
        cleaned = original
        
        # Basic spell checking patterns
        spelling_patterns = {
            r'\bteh\b': 'the',
            r'\band\b': 'and',
            r'\bwith\b': 'with',
            r'\bpatient\b': 'patient',
            r'\btreatment\b': 'treatment'
        }
        
        changes_made = 0
        for pattern, correction in spelling_patterns.items():
            if re.search(pattern, cleaned, re.IGNORECASE):
                cleaned = re.sub(pattern, correction, cleaned, flags=re.IGNORECASE)
                changes_made += 1
        
        confidence = 0.7 if changes_made > 0 else 1.0
        
        return CleaningAction(
            action_type='spelling_error',
            original_value=original,
            cleaned_value=cleaned,
            confidence=confidence,
            method_used=f'pattern_spelling_{changes_made}_changes'
        )
    
    def _standardize_medical_terms(self, text: str) -> CleaningAction:
        """Standardize medical terminology"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction('standardization', text, text, 1.0, 'no_change')
        
        original = str(text)
        cleaned = original.lower()
        
        # Standardization patterns
        standardization_map = {
            r'high blood pressure': 'hypertension',
            r'high bp': 'hypertension',
            r'heart attack': 'myocardial infarction',
            r'sugar diabetes': 'diabetes mellitus',
            r'sugar disease': 'diabetes mellitus'
        }
        
        changes_made = 0
        for pattern, standard_term in standardization_map.items():
            if re.search(pattern, cleaned, re.IGNORECASE):
                cleaned = re.sub(pattern, standard_term, cleaned, flags=re.IGNORECASE)
                changes_made += 1
        
        confidence = 0.8 if changes_made > 0 else 1.0
        
        return CleaningAction(
            action_type='standardization',
            original_value=original,
            cleaned_value=cleaned,
            confidence=confidence,
            method_used=f'term_standardization_{changes_made}_changes'
        )
    
    def _handle_missing_values(self, text: str) -> CleaningAction:
        """Handle missing or empty values"""
        if pd.isna(text) or not str(text).strip():
            return CleaningAction(
                action_type='missing_value',
                original_value=str(text) if not pd.isna(text) else 'NaN',
                cleaned_value='',  # or could be 'Unknown' depending on requirements
                confidence=1.0,
                method_used='missing_value_handling'
            )
        
        return CleaningAction(
            action_type='missing_value',
            original_value=str(text),
            cleaned_value=str(text),
            confidence=1.0,
            method_used='no_change'
        )
    
    def generate_cleaning_report(self, actions: List[CleaningAction]) -> Dict[str, Any]:
        """Generate a comprehensive cleaning report"""
        report = {
            'total_actions': len(actions),
            'actions_by_type': {},
            'average_confidence': 0,
            'methods_used': {},
            'changes_made': 0
        }
        
        total_confidence = 0
        for action in actions:
            # Count by action type
            if action.action_type not in report['actions_by_type']:
                report['actions_by_type'][action.action_type] = 0
            report['actions_by_type'][action.action_type] += 1
            
            # Count by method used
            if action.method_used not in report['methods_used']:
                report['methods_used'][action.method_used] = 0
            report['methods_used'][action.method_used] += 1
            
            # Track confidence and changes
            total_confidence += action.confidence
            if action.original_value != action.cleaned_value:
                report['changes_made'] += 1
        
        if actions:
            report['average_confidence'] = total_confidence / len(actions)
        
        report['change_percentage'] = (report['changes_made'] / len(actions) * 100) if actions else 0
        
        return report