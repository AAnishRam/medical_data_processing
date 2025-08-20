import os
import sys
import pandas as pd
import asyncio
from pathlib import Path

# Add the src directory to the path so we can import our agents
sys.path.append(str(Path(__file__).parent))

from agents.data_analysis import DataAnalysisAgent
from agents.medical_knowledge import MedicalKnowledgeAgent, MedicalTermsCache
from agents.cleaning_strategy import CleaningStrategyAgent
from openai import OpenAI
from dotenv import load_dotenv
import time

# Load API Key from .env
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class EnhancedMedicalProcessor:
    """Enhanced medical data processor with multi-agent architecture"""
    
    def __init__(self):
        self.cache = MedicalTermsCache()
        self.data_agent = DataAnalysisAgent()
        self.medical_agent = MedicalKnowledgeAgent(self.cache)
        self.strategy_agent = CleaningStrategyAgent()
        
        print("🤖 Multi-Agent Medical Data Processor Initialized")
        print(f"📊 Cache Stats: {self.cache.get_cache_stats()}")
    
    def analyze_dataset(self, df: pd.DataFrame, sample_size: int = 100) -> dict:
        """Analyze the dataset and identify issues"""
        print(f"🔍 Analyzing dataset (sample size: {sample_size})...")
        
        analysis_results = {}
        
        # Analyze key medical columns
        medical_columns = [col for col in df.columns if any(keyword in col.lower() 
                          for keyword in ['diagnosis', 'remark', 'note', 'biomarker', 'test'])]
        
        for col in medical_columns[:3]:  # Analyze first 3 medical columns
            if col in df.columns:
                print(f"   Analyzing column: {col}")
                sample_data = df[col].head(sample_size)
                issues = self.data_agent.analyze_column(sample_data)
                stats = self.data_agent.get_column_stats(sample_data)
                
                analysis_results[col] = {
                    'issues': issues,
                    'stats': stats,
                    'issue_count': len(issues)
                }
        
        return analysis_results
    
    def process_column_enhanced(self, series: pd.Series, limit: int = 50) -> pd.DataFrame:
        """Process a column with enhanced multi-agent approach"""
        print(f"🔄 Processing column '{series.name}' (limit: {limit})...")
        
        results = []
        subset = series.head(limit)
        
        start_time = time.time()
        api_calls = 0
        local_processing = 0
        
        for idx, value in subset.items():
            # Step 1: Local analysis
            cleaned_value, confidence, metadata = self._process_single_value(value)
            
            # Track processing method
            if 'api_cleaning' in metadata.get('processing_steps', []):
                api_calls += 1
            else:
                local_processing += 1
            
            results.append({
                'original': value,
                'cleaned': cleaned_value,
                'confidence': confidence,
                'processing_method': 'api' if 'api_cleaning' in metadata.get('processing_steps', []) else 'local',
                'steps_taken': len(metadata.get('processing_steps', []))
            })
        
        processing_time = time.time() - start_time
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Print summary
        print(f"✅ Processing complete!")
        print(f"   ⏱️  Time taken: {processing_time:.2f} seconds")
        print(f"   🏠 Local processing: {local_processing} items")
        print(f"   🌐 API calls: {api_calls} items")
        print(f"   📊 Average confidence: {results_df['confidence'].mean():.2%}")
        
        return results_df
    
    def _process_single_value(self, value):
        """Process a single value through the multi-agent pipeline"""
        if pd.isna(value) or not str(value).strip():
            return value, 1.0, {'processing_steps': []}
        
        original_value = str(value).strip()
        metadata = {'processing_steps': [], 'original_length': len(original_value)}
        
        # Step 1: Apply cleaning strategy (local processing)
        formatting_action = self.strategy_agent.apply_cleaning_strategy(original_value, 'formatting')
        cleaned_value = formatting_action.cleaned_value
        confidence = formatting_action.confidence
        metadata['processing_steps'].append('formatting')
        
        # Step 2: Fix medical typos
        typo_action = self.strategy_agent.apply_cleaning_strategy(cleaned_value, 'medical_typo')
        cleaned_value = typo_action.cleaned_value
        if typo_action.confidence < confidence:
            confidence = typo_action.confidence
        metadata['processing_steps'].append('medical_typo_check')
        
        # Step 3: Medical knowledge standardization
        standardized_value, med_confidence = self.medical_agent.standardize_term(cleaned_value)
        if med_confidence > confidence:
            cleaned_value = standardized_value
            confidence = med_confidence
        metadata['processing_steps'].append('medical_standardization')
        
        # Step 4: API call only if confidence is low
        if confidence < 0.7 and os.getenv("OPENAI_API_KEY"):
            try:
                api_result = self._api_cleaning(cleaned_value)
                if api_result and api_result.strip() != cleaned_value:
                    cleaned_value = api_result
                    confidence = 0.9
                    metadata['processing_steps'].append('api_cleaning')
            except Exception as e:
                print(f"   ⚠️  API error: {e}")
                # Continue with local result
        
        metadata['final_confidence'] = confidence
        return cleaned_value, confidence, metadata
    
    def _api_cleaning(self, text: str) -> str:
        """API-based cleaning as fallback"""
        prompt = f"""Clean this medical term. Fix only obvious spelling errors and expand standard abbreviations.
        Keep it concise and medical.
        
        Text: {text}
        
        Cleaned text:"""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=50
        )
        return response.choices[0].message.content.strip()
    
    def generate_comprehensive_report(self, analysis_results: dict, processing_results: dict) -> dict:
        """Generate a comprehensive processing report"""
        report = {
            'dataset_analysis': {
                'columns_analyzed': len(analysis_results),
                'total_issues_found': sum(result['issue_count'] for result in analysis_results.values()),
                'issue_breakdown': {}
            },
            'processing_performance': {
                'total_processed': 0,
                'api_calls_made': 0,
                'local_processing': 0,
                'average_confidence': 0,
                'processing_efficiency': 0
            },
            'recommendations': []
        }
        
        # Analyze processing results
        if processing_results:
            total_processed = len(processing_results)
            api_calls = sum(1 for result in processing_results if result.get('processing_method') == 'api')
            local_processing = total_processed - api_calls
            avg_confidence = sum(result.get('confidence', 0) for result in processing_results) / total_processed
            
            report['processing_performance'].update({
                'total_processed': total_processed,
                'api_calls_made': api_calls,
                'local_processing': local_processing,
                'average_confidence': avg_confidence,
                'processing_efficiency': (local_processing / total_processed) * 100 if total_processed > 0 else 0
            })
        
        # Generate recommendations
        if report['processing_performance']['processing_efficiency'] > 80:
            report['recommendations'].append("✅ Excellent local processing efficiency - low API dependency")
        elif report['processing_performance']['processing_efficiency'] > 60:
            report['recommendations'].append("⚠️ Good local processing - consider expanding medical knowledge base")
        else:
            report['recommendations'].append("🔄 High API dependency - recommend building more comprehensive local rules")
        
        return report

def main():
    """Main function to demonstrate the enhanced medical processor"""
    print("🏥 Enhanced Medical Data Processing System")
    print("=" * 50)
    
    # Initialize processor
    processor = EnhancedMedicalProcessor()
    
    # Check if data file exists
    data_file = "data/Aug_hackathon_medical_data.xlsx"
    if not os.path.exists(data_file):
        print(f"❌ Data file not found: {data_file}")
        print("Please ensure the data file is in the correct location.")
        return
    
    try:
        # Load dataset
        print(f"📂 Loading dataset: {data_file}")
        df = pd.read_excel(data_file)
        print(f"✅ Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
        
        # Analyze dataset
        analysis_results = processor.analyze_dataset(df, sample_size=50)
        
        print(f"\n📊 Analysis Results:")
        for col, results in analysis_results.items():
            print(f"   {col}: {results['issue_count']} issues found")
        
        # Process a specific column (example: provisionaldiagnosis)
        target_column = 'provisionaldiagnosis'
        if target_column in df.columns:
            print(f"\n🔄 Processing '{target_column}' column...")
            processing_results = processor.process_column_enhanced(df[target_column], limit=25)
            
            # Show sample results
            print(f"\n📋 Sample Results:")
            for i, row in processing_results.head(5).iterrows():
                print(f"   Original: {row['original']}")
                print(f"   Cleaned:  {row['cleaned']}")
                print(f"   Method:   {row['processing_method']} (confidence: {row['confidence']:.2%})")
                print()
            
            # Save results
            output_file = "cleaned_medical_data_enhanced.xlsx"
            df_output = df.copy()
            df_output[f"cleaned_{target_column}"] = processing_results['cleaned']
            df_output[f"confidence_{target_column}"] = processing_results['confidence']
            df_output.to_excel(output_file, index=False)
            print(f"💾 Results saved to: {output_file}")
            
            # Generate comprehensive report
            report = processor.generate_comprehensive_report(
                analysis_results, 
                processing_results.to_dict('records')
            )
            
            print(f"\n📈 Final Report:")
            print(f"   Processing Efficiency: {report['processing_performance']['processing_efficiency']:.1f}%")
            print(f"   Average Confidence: {report['processing_performance']['average_confidence']:.2%}")
            print(f"   API Calls Saved: {report['processing_performance']['local_processing']}/{report['processing_performance']['total_processed']}")
            
            for recommendation in report['recommendations']:
                print(f"   {recommendation}")
        
        else:
            print(f"❌ Column '{target_column}' not found in dataset")
            print(f"Available columns: {list(df.columns[:10])}...")  # Show first 10 columns
    
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()