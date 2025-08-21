import streamlit as st
import pandas as pd
import os
import time
import re
import sqlite3
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from difflib import SequenceMatcher
from dotenv import load_dotenv
from openai import OpenAI

# Load API key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

@dataclass
class DataIssue:
    column: str
    row_index: int
    issue_type: str
    severity: str
    original_value: str
    suggested_fix: str
    confidence: float

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
            (original TEXT PRIMARY KEY, cleaned TEXT, confidence REAL)
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
            
            # Common misspellings
            'diabetis': 'diabetes',
            'hypertention': 'hypertension',
            'infaction': 'infarction',
            'pnemonia': 'pneumonia',
            'asthama': 'asthma',
            'bronchitus': 'bronchitis',
            
            # Variations
            'high bp': 'hypertension',
            'high blood pressure': 'hypertension',
            'sugar': 'diabetes mellitus',
            'heart attack': 'myocardial infarction'
        }
        
        conn = sqlite3.connect(self.cache_file)
        for original, cleaned in common_terms.items():
            conn.execute(
                'INSERT OR REPLACE INTO medical_terms VALUES (?, ?, ?)',
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
            'INSERT OR REPLACE INTO medical_terms VALUES (?, ?, ?)',
            (original.lower(), cleaned, confidence)
        )
        conn.commit()
        conn.close()

class DataAnalysisAgent:
    """Agent for identifying data quality issues"""
    
    def __init__(self):
        self.issue_patterns = {
            'medical_typos': r'diabetis|hypertention|pnemonia|asthama|bronchitus',
            'extra_spaces': r'\s{2,}',
            'special_chars': r'[^\w\s\-\.]'
        }
        
        self.medical_abbreviations = {
            'htn', 'dm', 'mi', 'copd', 'bp', 'hr', 'rr'
        }
    
    def analyze_column(self, series: pd.Series) -> List[DataIssue]:
        """Analyze a column for various data quality issues"""
        issues = []
        
        for idx, value in series.items():
            if pd.isna(value) or not str(value).strip():
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
        
        return issues

class MedicalKnowledgeAgent:
    """Agent for medical terminology validation and standardization"""
    
    def __init__(self, cache: MedicalTermsCache):
        self.cache = cache
        self.medical_patterns = {
            'abbreviations': {
                r'\bhtn\b': 'hypertension',
                r'\bdm\b': 'diabetes mellitus',
                r'\bmi\b': 'myocardial infarction',
                r'\bcopd\b': 'chronic obstructive pulmonary disease',
                r'\bbp\b': 'blood pressure',
                r'\bhr\b': 'heart rate'
            }
        }
        
        self.known_medical_terms = [
            'diabetes', 'hypertension', 'pneumonia', 'asthma', 'bronchitis',
            'myocardial infarction', 'chronic obstructive pulmonary disease'
        ]
    
    def standardize_term(self, term: str) -> Tuple[str, float]:
        """Standardize medical term with confidence score"""
        if pd.isna(term) or not str(term).strip():
            return term, 0.0
        
        text = str(term).strip().lower()
        
        # Check cache first
        cached = self.cache.get(text)
        if cached:
            return cached[0], cached[1]
        
        # Apply local abbreviation expansion
        standardized = text
        confidence = 0.5
        
        for pattern, replacement in self.medical_patterns['abbreviations'].items():
            if re.search(pattern, standardized, re.IGNORECASE):
                standardized = re.sub(pattern, replacement, standardized, flags=re.IGNORECASE)
                confidence = 0.8
        
        # Basic spell checking for common terms
        if confidence < 0.7:
            best_match, similarity = self._find_best_match(text)
            if similarity > 0.8:
                standardized = best_match
                confidence = similarity
        
        self.cache.set(text, standardized, confidence)
        return standardized, confidence
    
    def _find_best_match(self, term: str) -> Tuple[str, float]:
        """Find best matching medical term"""
        max_similarity = 0
        best_match = term
        
        for known_term in self.known_medical_terms:
            similarity = SequenceMatcher(None, term.lower(), known_term).ratio()
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = known_term
        
        return best_match, max_similarity

class EnhancedMedicalCleaner:
    """Main orchestrator for the enhanced medical data cleaning system"""
    
    def __init__(self):
        self.cache = MedicalTermsCache()
        self.data_analysis_agent = DataAnalysisAgent()
        self.medical_knowledge_agent = MedicalKnowledgeAgent(self.cache)
        
    def clean_text_enhanced(self, text: str) -> Tuple[str, float, Dict]:
        """Enhanced text cleaning with local processing first"""
        if pd.isna(text) or not str(text).strip():
            return text, 0.0, {}
        
        original_text = str(text).strip()
        metadata = {'processing_steps': []}
        
        # Step 1: Local preprocessing
        cleaned_text = self._local_preprocessing(original_text)
        metadata['processing_steps'].append('local_preprocessing')
        
        # Step 2: Medical knowledge standardization
        standardized_text, confidence = self.medical_knowledge_agent.standardize_term(cleaned_text)
        metadata['processing_steps'].append('medical_standardization')
        
        # Step 3: Only use API if confidence is low
        if confidence < 0.7:
            try:
                api_result = self._api_cleaning(standardized_text)
                if api_result and api_result != standardized_text:
                    standardized_text = api_result
                    confidence = 0.9
                    metadata['processing_steps'].append('api_cleaning')
            except Exception as e:
                # Continue with local result if API fails
                pass
        
        return standardized_text, confidence, metadata
    
    def _local_preprocessing(self, text: str) -> str:
        """Local preprocessing without API calls"""
        # Fix obvious formatting issues
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Fix common medical typos
        typo_corrections = {
            'diabetis': 'diabetes',
            'hypertention': 'hypertension',
            'pnemonia': 'pneumonia',
            'asthama': 'asthma'
        }
        
        text_lower = text.lower()
        for typo, correction in typo_corrections.items():
            if typo in text_lower:
                text = re.sub(rf'\b{typo}\b', correction, text, flags=re.IGNORECASE)
        
        return text
    
    def _api_cleaning(self, text: str) -> str:
        """API-based cleaning as fallback"""
        prompt = f"""Fix spelling and expand medical abbreviations in this text. Keep it concise:
        Text: {text}
        Output:"""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=50
        )
        return response.choices[0].message.content.strip()
    
    def process_dataframe(self, df: pd.DataFrame, column: str, limit: int, progress_callback=None) -> pd.DataFrame:
        """Process dataframe with progress tracking"""
        subset = df[column].head(limit).fillna('')
        results = []
        
        for i, value in enumerate(subset):
            if progress_callback:
                progress_callback(i / len(subset))
            
            cleaned, confidence, metadata = self.clean_text_enhanced(value)
            results.append({
                'original': value,
                'cleaned': cleaned,
                'confidence': confidence,
                'method': 'api' if 'api_cleaning' in metadata.get('processing_steps', []) else 'local'
            })
        
        # Update dataframe
        df_copy = df.copy()
        df_copy[f"cleaned_{column}"] = pd.NA
        df_copy[f"confidence_{column}"] = pd.NA
        
        for i, result in enumerate(results):
            df_copy.loc[i, f"cleaned_{column}"] = result['cleaned']
            df_copy.loc[i, f"confidence_{column}"] = result['confidence']
        
        return df_copy, results

# Streamlit UI
def main():
    st.set_page_config(
        page_title="Enhanced Medical Data Cleaner", 
        page_icon="🏥", 
        layout="wide"
    )
    
    st.title("🏥 Enhanced Healthcare Data Cleaning Tool")
    st.markdown("*Faster, more accurate medical data processing with smart caching*")
    
    # Set confidence threshold to maximum
    confidence_threshold = 1.0
    enable_cache = True
    
    # Initialize cleaner
    if 'cleaner' not in st.session_state:
        with st.spinner("Initializing medical knowledge base..."):
            st.session_state.cleaner = EnhancedMedicalCleaner()
        st.success("✅ System initialized!")
    
    uploaded_file = st.file_uploader("Upload medical data (CSV/XLSX)", type=["csv", "xlsx"])
    
    if uploaded_file:
        try:
            # Load data
            df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith("csv") else pd.read_excel(uploaded_file)
            st.success(f"📊 Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            st.error(f"❌ Error loading file: {e}")
            return
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write("### 📋 Data Preview")
            st.dataframe(df.head(), use_container_width=True)
        
        with col2:
            st.write("### 📈 Data Info")
            st.metric("Rows", len(df))
            st.metric("Columns", len(df.columns))
            missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            st.metric("Missing Data", f"{missing_pct:.1f}%")
        
        # Column selection and processing options
        target_column = st.selectbox("🎯 Select column to clean", df.columns)
        
        # Set default value based on dataset size
        default_rows = min(50, len(df))
        row_limit = st.number_input("📊 Number of rows to process", min_value=1, max_value=len(df), value=default_rows, step=min(10, len(df)))
        
        # Quick analysis preview
        if st.button("🔍 Quick Analysis (Sample)"):
            with st.spinner("Analyzing sample data..."):
                sample_data = df[target_column].head(10)
                issues = st.session_state.cleaner.data_analysis_agent.analyze_column(sample_data)
                
                if issues:
                    st.write("### ⚠️ Issues Detected (Sample)")
                    issue_data = []
                    for issue in issues[:5]:
                        issue_data.append({
                            'Row': issue.row_index,
                            'Issue Type': issue.issue_type,
                            'Severity': issue.severity,
                            'Original': issue.original_value[:50] + "..." if len(issue.original_value) > 50 else issue.original_value
                        })
                    
                    if issue_data:
                        st.dataframe(pd.DataFrame(issue_data), use_container_width=True)
                else:
                    st.info("✅ No obvious issues detected in the sample.")
        
        # Main processing
        if st.button("🚀 Process Data", type="primary"):
            start_time = time.time()
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                with st.spinner("Processing data..."):
                    # Define progress callback
                    def update_progress(progress):
                        progress_bar.progress(progress)
                        status_text.text(f"Processing... {progress:.1%} complete")
                    
                    # Process data
                    processed_df, results = st.session_state.cleaner.process_dataframe(
                        df, target_column, row_limit, update_progress
                    )
                    
                    processing_time = time.time() - start_time
                    
                    st.success(f"✅ Processing complete!")
                    
                    # Show comparison
                    st.write("### 🔄 Results Comparison")
                    if results:
                        comparison_data = []
                        for i, result in enumerate(results[:10]):
                            comparison_data.append({
                                'Row': i + 1,
                                'Original': result['original'][:50] + "..." if len(str(result['original'])) > 50 else result['original'],
                                'Result': result['cleaned'][:50] + "..." if len(str(result['cleaned'])) > 50 else result['cleaned']
                            })
                        
                        st.dataframe(pd.DataFrame(comparison_data), use_container_width=True)
                    
                    # Download option
                    output_file = "enhanced_cleaned_data.xlsx"
                    processed_df.to_excel(output_file, index=False)
                    
                    with open(output_file, "rb") as f:
                        st.download_button(
                            "📥 Download Enhanced Cleaned Data",
                            f.read(),
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
            
            except Exception as e:
                st.error(f"❌ Processing error: {e}")
                import traceback
                with st.expander("Error Details"):
                    st.code(traceback.format_exc())

if __name__ == "__main__":
    main()