"""
Enhanced Modular Dashboard
Uses the new column-specific processing system
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import time
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent / "src"))

from src.column_processors import ColumnProcessorManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Modular Medical Data Processor",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .processor-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin-bottom: 1rem;
    }
    .column-card {
        background-color: #fff;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        margin-bottom: 0.5rem;
    }
    .quality-high { color: #28a745; font-weight: bold; }
    .quality-medium { color: #ffc107; font-weight: bold; }
    .quality-low { color: #dc3545; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if 'processor_manager' not in st.session_state:
        st.session_state.processor_manager = ColumnProcessorManager()
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'processing_results' not in st.session_state:
        st.session_state.processing_results = None

def display_header():
    """Display the main header"""
    st.markdown('<h1 class="main-header">🏥 Modular Medical Data Processing System</h1>', unsafe_allow_html=True)
    st.markdown("### Column-Specific Intelligent Healthcare Data Enhancement")
    st.markdown("---")

def display_system_overview():
    """Display system overview and available processors"""
    with st.expander("🔧 System Overview - Modular Column Processing", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🎯 Modular Architecture Benefits:**")
            st.markdown("""
            - **Column-Specific Logic**: Each column has dedicated processing rules
            - **Easy Enhancement**: Add new processors for specific needs
            - **Maintainable Code**: Isolated logic for each data type
            - **Scalable Design**: Handle any number of medical data columns
            - **Specialized Processing**: Medical terminology, lab values, diagnoses
            """)
        
        with col2:
            st.markdown("**📋 Available Column Processors:**")
            available_processors = st.session_state.processor_manager.get_available_processors()
            
            for column, description in available_processors.items():
                st.markdown(f"- **{column}**: {description[:50]}...")

def upload_and_analyze_data():
    """Handle data upload and column analysis"""
    st.subheader("📂 Data Upload & Column Detection")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload medical dataset",
        type=['xlsx', 'csv'],
        help="Upload your medical dataset for column-specific processing"
    )
    
    # Sample data option
    use_sample = st.checkbox("Use sample medical data for testing")
    
    if uploaded_file or use_sample:
        if uploaded_file:
            # Load uploaded file
            if uploaded_file.name.endswith('.xlsx'):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_csv(uploaded_file)
            st.success(f"✅ Loaded {uploaded_file.name}")
        else:
            # Create sample data
            df = create_sample_medical_data()
            st.info("📊 Using sample medical data")
        
        st.session_state.data = df
        st.session_state.data_loaded = True
        
        # Display basic info
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Rows", f"{len(df):,}")
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            st.metric("Missing Values", f"{df.isnull().sum().sum():,}")
        with col4:
            missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            st.metric("Missing %", f"{missing_pct:.1f}%")
        
        # Column processor analysis
        st.subheader("🔍 Column Processor Analysis")
        
        processor_manager = st.session_state.processor_manager
        processing_summary = processor_manager.get_processing_summary(df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Processable Columns", processing_summary['processable_columns'])
        with col2:
            st.metric("Coverage", f"{processing_summary['processing_coverage']:.1%}")
        with col3:
            st.metric("Non-Processable", processing_summary['non_processable_columns'])
        
        # Display column details
        st.subheader("📋 Column Processing Status")
        
        processable_cols = processing_summary['processable_column_list']
        non_processable_cols = processing_summary['non_processable_column_list']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**✅ Columns with Dedicated Processors:**")
            for col in processable_cols:
                priority = processing_summary['column_priorities'].get(col, 999)
                st.markdown(f"- **{col}** (Priority: {priority})")
        
        with col2:
            st.markdown("**⚠️ Columns without Dedicated Processors:**")
            for col in non_processable_cols[:10]:  # Show first 10
                st.markdown(f"- {col}")
            if len(non_processable_cols) > 10:
                st.markdown(f"... and {len(non_processable_cols) - 10} more")
        
        # Data preview
        st.subheader("📊 Data Preview")
        st.dataframe(df.head(10), use_container_width=True)

def create_sample_medical_data():
    """Create sample medical data for testing"""
    import random
    
    sample_data = {
        'patient_id': [f'PAT{i:05d}' for i in range(1, 101)],
        'test': [
            'cbc', 'Complete Blood Count', 'lft', 'Liver Function Test', 'rft',
            'ecg', 'Electrocardiogram', 'ct scan', 'xray', 'usg abdomen'
        ] * 10,
        'biomarker': [
            'hb', 'Hemoglobin', 'wbc', 'alt', 'ast', 'creatinine', 'glucose',
            'cholesterol', 'hba1c', 'tsh'
        ] * 10,
        'result': [f'{random.uniform(5, 200):.1f}' for _ in range(100)],
        'provisionaldiagnosis': [
            'diabetis mellitus', 'htn', 'Hypertension', 'mi', 'asthma',
            'copd', 'pneumonia', 'uti', 'gastritis', 'migraine'
        ] * 10,
        'finaldiagnosis': [
            'Diabetes Mellitus Type 2', 'Hypertension', 'Myocardial Infarction',
            'Asthma', 'COPD', 'Pneumonia', 'UTI', 'Gastritis', 'Migraine', 'Depression'
        ] * 10,
        'chief_remark': [
            'c/o chest pain', 'h/o diabetes', 'r/o MI', 'SOB since 2 days',
            'fever w/ cough', 'headache severe', 'abdominal pain', 'joint pain',
            'fatigue', 'dizziness'
        ] * 10
    }
    
    return pd.DataFrame(sample_data)

def perform_column_analysis():
    """Perform detailed analysis of each processable column"""
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please load data first")
        return
    
    st.subheader("🔍 Column-by-Column Analysis")
    
    df = st.session_state.data
    processor_manager = st.session_state.processor_manager
    
    # Get processable columns
    available_processors = processor_manager.get_available_processors()
    processable_columns = [col for col in df.columns if col in available_processors]
    
    if not processable_columns:
        st.warning("⚠️ No processable columns found in the dataset")
        return
    
    # Column selection
    selected_columns = st.multiselect(
        "Select columns to analyze:",
        options=processable_columns,
        default=processable_columns[:5],
        help="Choose which columns to analyze for data quality issues"
    )
    
    sample_size = st.slider("Sample size for analysis", 50, 200, 100, 25)
    
    if st.button("🚀 Analyze Selected Columns", type="primary"):
        with st.spinner("Analyzing columns..."):
            analysis_results = processor_manager.analyze_dataset(
                df, 
                columns_to_analyze=selected_columns,
                sample_size=sample_size
            )
            
            st.session_state.analysis_results = analysis_results
            
            # Display results
            st.success("✅ Analysis completed!")
            
            # Overall metrics
            total_issues = sum(sum(result.issues_summary.values()) 
                             for result in analysis_results.values())
            avg_quality = sum(result.quality_score for result in analysis_results.values()) / len(analysis_results)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Issues Found", total_issues)
            with col2:
                st.metric("Average Quality Score", f"{avg_quality:.1%}")
            with col3:
                st.metric("Columns Analyzed", len(analysis_results))
            
            # Detailed results
            st.subheader("📊 Detailed Analysis Results")
            
            for column_name, analysis in analysis_results.items():
                with st.expander(f"📋 {column_name} - Quality: {analysis.quality_score:.1%}"):
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**📈 Column Statistics:**")
                        st.write(f"• Total rows: {analysis.total_rows:,}")
                        st.write(f"• Null values: {analysis.null_count:,}")
                        st.write(f"• Unique values: {analysis.unique_count:,}")
                        st.write(f"• Quality score: {analysis.quality_score:.1%}")
                        
                        # Quality indicator
                        if analysis.quality_score >= 0.8:
                            quality_class = "quality-high"
                            quality_text = "High Quality"
                        elif analysis.quality_score >= 0.6:
                            quality_class = "quality-medium"
                            quality_text = "Medium Quality"
                        else:
                            quality_class = "quality-low"
                            quality_text = "Low Quality"
                        
                        st.markdown(f'<p class="{quality_class}">Quality Level: {quality_text}</p>', 
                                  unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown("**🔍 Issues Summary:**")
                        for issue_type, count in analysis.issues_summary.items():
                            if count > 0:
                                st.write(f"• {issue_type.replace('_', ' ').title()}: {count}")
                        
                        if not any(analysis.issues_summary.values()):
                            st.write("• No issues detected")
                    
                    # Recommendations
                    if analysis.recommendations:
                        st.markdown("**💡 Recommendations:**")
                        for i, rec in enumerate(analysis.recommendations, 1):
                            st.write(f"{i}. {rec}")
                    
                    # Sample issues
                    if analysis.sample_issues:
                        st.markdown("**🔍 Sample Issues:**")
                        for issue in analysis.sample_issues[:3]:
                            issue_type = issue.get('type', 'Unknown')
                            value = issue.get('value', 'N/A')
                            suggestion = issue.get('suggestion', issue.get('issue', 'No details'))
                            st.write(f"• **{issue_type}**: '{value}' → {suggestion}")

def column_processing_interface():
    """Interface for configuring and executing column processing"""
    if not st.session_state.analysis_results:
        st.warning("⚠️ Please complete column analysis first")
        return
    
    st.subheader("🎯 Column Processing Configuration")
    
    analysis_results = st.session_state.analysis_results
    processor_manager = st.session_state.processor_manager
    
    # Processing options
    st.markdown("### 🔧 Processing Options")
    
    # Column selection for processing
    columns_to_process = st.multiselect(
        "Select columns to process:",
        options=list(analysis_results.keys()),
        default=list(analysis_results.keys()),
        help="Choose which columns to apply cleaning operations to"
    )
    
    # Processing configuration
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**⚙️ General Settings:**")
        batch_size = st.slider("Batch processing size", 100, 1000, 500, 100)
        confidence_threshold = st.slider("Confidence threshold", 0.5, 1.0, 0.7, 0.05)
        preserve_original = st.checkbox("Preserve original values", value=True)
    
    with col2:
        st.markdown("**🎯 Processing Strategy:**")
        processing_mode = st.selectbox("Processing mode", 
                                     ["Conservative", "Moderate", "Aggressive"])
        apply_all_transformations = st.checkbox("Apply all transformations", value=True)
        validate_results = st.checkbox("Validate results", value=True)
    
    # Column-specific options
    if columns_to_process:
        st.markdown("### 📋 Column-Specific Configuration")
        
        column_options = {}
        
        for column in columns_to_process:
            with st.expander(f"⚙️ {column} Settings"):
                analysis = analysis_results[column]
                
                # Show column info
                st.write(f"**Quality Score:** {analysis.quality_score:.1%}")
                st.write(f"**Total Issues:** {sum(analysis.issues_summary.values())}")
                
                # Column-specific options
                col_options = {}
                
                # Different options based on column type
                if 'spelling' in str(analysis.issues_summary):
                    col_options['fix_spelling'] = st.checkbox(f"Fix spelling errors in {column}", value=True)
                
                if 'abbreviation' in str(analysis.issues_summary):
                    col_options['expand_abbreviations'] = st.checkbox(f"Expand abbreviations in {column}", value=True)
                
                if 'formatting' in str(analysis.issues_summary):
                    col_options['normalize_formatting'] = st.checkbox(f"Normalize formatting in {column}", value=True)
                
                # Advanced options
                col_options['sample_size'] = st.slider(f"Sample size for {column}", 50, 500, 100, 50, 
                                                      key=f"sample_{column}")
                col_options['confidence_threshold'] = st.slider(f"Confidence threshold for {column}", 
                                                              0.5, 1.0, 0.7, 0.05, key=f"conf_{column}")
                
                column_options[column] = col_options
    
    # Processing execution
    st.markdown("### 🚀 Execute Processing")
    
    if st.button("🔄 Process Selected Columns", type="primary", use_container_width=True):
        if not columns_to_process:
            st.error("❌ Please select at least one column to process")
            return
        
        with st.spinner("Processing columns with dedicated processors..."):
            try:
                df = st.session_state.data
                
                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_columns = len(columns_to_process)
                
                # Process columns
                for i, column in enumerate(columns_to_process):
                    status_text.text(f"Processing {column}...")
                    progress_bar.progress((i + 1) / total_columns)
                    
                    # Simulate processing time
                    time.sleep(0.5)
                
                # Final processing
                status_text.text("Finalizing results...")
                processed_df = processor_manager.process_columns(
                    df,
                    columns_to_process=columns_to_process,
                    processing_options=column_options
                )
                
                progress_bar.progress(1.0)
                status_text.text("✅ Processing completed!")
                
                # Store results
                st.session_state.processed_data = processed_df
                st.session_state.processing_results = processor_manager.get_processing_stats()
                
                st.success("🎉 Column processing completed successfully!")
                
                # Display results summary
                display_processing_results()
                
            except Exception as e:
                st.error(f"❌ Error during processing: {e}")

def display_processing_results():
    """Display processing results and statistics"""
    if 'processed_data' not in st.session_state:
        return
    
    st.subheader("📊 Processing Results")
    
    processing_stats = st.session_state.processing_results
    
    # Overall metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Values Processed", f"{processing_stats['total_values_processed']:,}")
    with col2:
        st.metric("Successful Cleanings", f"{processing_stats['total_successful_cleanings']:,}")
    with col3:
        st.metric("Success Rate", f"{processing_stats['overall_success_rate']:.1%}")
    with col4:
        st.metric("Columns Processed", processing_stats['columns_processed'])
    
    # Detailed results
    st.subheader("📋 Column Processing Details")
    
    for column, stats in processing_stats['column_details'].items():
        with st.expander(f"📊 {column} Processing Results"):
            if 'error' in stats:
                st.error(f"Error: {stats['error']}")
            else:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Total processed:** {stats.get('total_processed', 0):,}")
                    st.write(f"**Successful:** {stats.get('successful_cleanings', 0):,}")
                    st.write(f"**Failed:** {stats.get('failed_cleanings', 0):,}")
                
                with col2:
                    transformations = stats.get('transformations_applied', {})
                    if transformations:
                        st.write("**Transformations applied:**")
                        for transform, count in transformations.items():
                            st.write(f"• {transform.replace('_', ' ').title()}: {count}")
    
    # Download options
    st.subheader("📥 Download Processed Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Download Processed Data", use_container_width=True):
            # Here you would implement actual download
            st.success("✅ Processed data would be downloaded")
    
    with col2:
        if st.button("📋 Download Processing Report", use_container_width=True):
            # Here you would implement report download
            st.success("✅ Processing report would be downloaded")

def main():
    """Main dashboard function"""
    initialize_session_state()
    display_header()
    display_system_overview()
    
    # Main workflow tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📂 Data Upload",
        "🔍 Column Analysis", 
        "🎯 Processing Config",
        "📊 Results & Download"
    ])
    
    with tab1:
        upload_and_analyze_data()
    
    with tab2:
        perform_column_analysis()
    
    with tab3:
        column_processing_interface()
    
    with tab4:
        if 'processing_results' in st.session_state:
            display_processing_results()
        else:
            st.info("⏳ Complete the processing workflow to view results here.")
    
    # Sidebar with processor info
    with st.sidebar:
        st.subheader("🔧 Processor Status")
        
        if st.session_state.data_loaded:
            processor_manager = st.session_state.processor_manager
            df = st.session_state.data
            summary = processor_manager.get_processing_summary(df)
            
            st.metric("Processable Columns", summary['processable_columns'])
            st.metric("Processing Coverage", f"{summary['processing_coverage']:.1%}")
            
            # Show available processors
            st.subheader("📋 Available Processors")
            available = processor_manager.get_available_processors()
            for col in available.keys():
                if col in df.columns:
                    st.write(f"✅ {col}")
                else:
                    st.write(f"⏸️ {col} (not in data)")
    
    # Footer
    st.markdown("---")
    st.markdown("🏥 **Modular Medical Data Processing System** | Column-Specific Intelligence for Healthcare Data")

if __name__ == "__main__":
    main()
