import pandas as pd
import re
import sqlite3
from typing import Tuple, Optional, Dict
from difflib import SequenceMatcher

class MedicalTermsCache:
    """Local cache for medical terms to reduce API calls"""
    
    def __init__(self, cache_file='medical_cache.db'):
        self.cache_file = cache_file
        self.init_cache()
        self.load_common_terms()
    
    def init_cache(self):
        conn = sqlite3.connect(self.cache_file)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS medical_terms 
            (original TEXT PRIMARY KEY, cleaned TEXT, confidence REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        ''')
        conn.commit()
        conn.close()
    
    def load_common_terms(self):
        """Pre-populate with common medical abbreviations and corrections"""
        common_terms = {
            # Common abbreviations
            'htn': 'hypertension',
            'dm': 'diabetes mellitus', 
            'dm2': 'diabetes mellitus type 2',
            'mi': 'myocardial infarction',
            'copd': 'chronic obstructive pulmonary disease',
            'bp': 'blood pressure',
            'hr': 'heart rate',
            'rr': 'respiratory rate',
            'temp': 'temperature',
            'wbc': 'white blood cell count',
            'rbc': 'red blood cell count',
            'hgb': 'hemoglobin',
            'hct': 'hematocrit',
            
            # Common misspellings
            'diabetis': 'diabetes',
            'hypertention': 'hypertension',
            'infaction': 'infarction',
            'pnemonia': 'pneumonia',
            'asthama': 'asthma',
            'bronchitus': 'bronchitis',
            'arthritus': 'arthritis',
            'apendix': 'appendix',
            'rhumatism': 'rheumatism',
            
            # Variations
            'high bp': 'hypertension',
            'high blood pressure': 'hypertension',
            'sugar': 'diabetes mellitus',
            'heart attack': 'myocardial infarction'
        }
        
        conn = sqlite3.connect(self.cache_file)
        for original, cleaned in common_terms.items():
            conn.execute(
                'INSERT OR REPLACE INTO medical_terms VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
                (original.lower(), cleaned, 0.95)
            )
        conn.commit()
        conn.close()
    
    def get(self, term: str) -> Optional[Tuple[str, float]]:
        conn = sqlite3.connect(self.cache_file)
        result = conn.execute(
            'SELECT cleaned, confidence FROM medical_terms WHERE original = ?',
            (term.lower(),)
        ).fetchone()
        conn.close()
        return result
    
    def set(self, original: str, cleaned: str, confidence: float):
        conn = sqlite3.connect(self.cache_file)
        conn.execute(
            'INSERT OR REPLACE INTO medical_terms VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
            (original.lower(), cleaned, confidence)
        )
        conn.commit()
        conn.close()
    
    def get_cache_stats(self) -> Dict:
        """Get statistics about the cache"""
        conn = sqlite3.connect(self.cache_file)
        stats = {
            'total_terms': conn.execute('SELECT COUNT(*) FROM medical_terms').fetchone()[0],
            'high_confidence': conn.execute('SELECT COUNT(*) FROM medical_terms WHERE confidence > 0.8').fetchone()[0]
        }
        conn.close()
        return stats

class MedicalKnowledgeAgent:
    """Agent for medical terminology validation and standardization"""
    
    def __init__(self, cache: MedicalTermsCache = None):
        self.cache = cache or MedicalTermsCache()
        
        self.medical_patterns = {
            'abbreviations': {
                r'\bhtn\b': 'hypertension',
                r'\bdm\b': 'diabetes mellitus',
                r'\bdm2\b': 'diabetes mellitus type 2',
                r'\bmi\b': 'myocardial infarction',
                r'\bcopd\b': 'chronic obstructive pulmonary disease',
                r'\bbp\b': 'blood pressure',
                r'\bhr\b': 'heart rate',
                r'\brr\b': 'respiratory rate'
            },
            'common_phrases': {
                r'high bp': 'hypertension',
                r'high blood pressure': 'hypertension',
                r'heart attack': 'myocardial infarction',
                r'sugar diabetes': 'diabetes mellitus'
            }
        }
        
        # Known medical terms for similarity matching
        self.known_medical_terms = [
            'diabetes', 'hypertension', 'pneumonia', 'asthma', 'bronchitis',
            'myocardial infarction', 'chronic obstructive pulmonary disease',
            'arthritis', 'appendicitis', 'gastritis', 'nephritis', 'hepatitis',
            'dermatitis', 'sinusitis', 'tonsillitis', 'conjunctivitis'
        ]
    
    def standardize_term(self, term: str) -> Tuple[str, float]:
        """Standardize medical term with confidence score"""
        if pd.isna(term) or not str(term).strip():
            return term, 0.0
        
        original_text = str(term).strip()
        text = original_text.lower()
        
        # Check cache first
        cached = self.cache.get(text)
        if cached:
            return cached[0], cached[1]
        
        # Apply local standardization
        standardized = text
        confidence = 0.5
        
        # Step 1: Apply abbreviation expansion
        for pattern, replacement in self.medical_patterns['abbreviations'].items():
            if re.search(pattern, standardized, re.IGNORECASE):
                standardized = re.sub(pattern, replacement, standardized, flags=re.IGNORECASE)
                confidence = 0.85
        
        # Step 2: Apply common phrase replacements
        for pattern, replacement in self.medical_patterns['common_phrases'].items():
            if re.search(pattern, standardized, re.IGNORECASE):
                standardized = re.sub(pattern, replacement, standardized, flags=re.IGNORECASE)
                confidence = 0.8
        
        # Step 3: Basic spell checking for common terms
        if confidence < 0.7:
            spell_checked, spell_confidence = self._spell_check_medical_term(text)
            if spell_confidence > confidence:
                standardized = spell_checked
                confidence = spell_confidence
        
        # Step 4: Clean up formatting
        standardized = self._clean_formatting(standardized)
        
        # Cache the result
        self.cache.set(text, standardized, confidence)
        return standardized, confidence
    
    def _spell_check_medical_term(self, term: str) -> Tuple[str, float]:
        """Check spelling against known medical terms"""
        max_similarity = 0
        best_match = term
        
        for known_term in self.known_medical_terms:
            similarity = SequenceMatcher(None, term.lower(), known_term).ratio()
            if similarity > max_similarity and similarity > 0.8:  # 80% similarity threshold
                max_similarity = similarity
                best_match = known_term
        
        return best_match, max_similarity
    
    def _clean_formatting(self, text: str) -> str:
        """Clean up basic formatting issues"""
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Fix common punctuation issues
        text = re.sub(r'\s*,\s*', ', ', text)
        text = re.sub(r'\s*\.\s*', '. ', text)
        
        # Capitalize first letter of each major word
        words = text.split()
        if len(words) > 0:
            # Keep medical abbreviations in uppercase if they were originally
            formatted_words = []
            for word in words:
                if word.upper() in ['BP', 'HR', 'RR', 'COPD', 'MI', 'HTN', 'DM']:
                    formatted_words.append(word.upper())
                else:
                    formatted_words.append(word.lower())
            text = ' '.join(formatted_words)
        
        return text
    
    def validate_medical_context(self, term: str) -> Dict:
        """Validate if a term appears to be medical in nature"""
        medical_indicators = [
            'diagnosis', 'disease', 'syndrome', 'infection', 'inflammation',
            'blood', 'heart', 'lung', 'kidney', 'liver', 'brain',
            'pressure', 'diabetes', 'hyper', 'hypo', 'itis', 'osis'
        ]
        
        term_lower = term.lower()
        medical_score = 0
        
        for indicator in medical_indicators:
            if indicator in term_lower:
                medical_score += 1
        
        return {
            'is_likely_medical': medical_score > 0,
            'medical_score': medical_score,
            'total_indicators': len(medical_indicators)
        }