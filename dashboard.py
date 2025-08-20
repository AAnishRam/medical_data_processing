import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from openai import OpenAI
import time

# Load API key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def clean_text(text):
    if pd.isna(text) or not str(text).strip():
        return text
    
    prompt = f"""
    You are a medical data cleaner.
    Clean and standardize the following term into proper medical terminology:
    - Correct spelling errors
    - Expand abbreviations
    - Replace local terms with global medical terms
    
    Text: {text}
    Output:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"

st.title("🏥 Healthcare Data Cleaning Tool")

uploaded_file = st.file_uploader("Upload medical data (CSV/XLSX)", type=["csv", "xlsx"])

if uploaded_file:
    df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith("csv") else pd.read_excel(uploaded_file)
    st.write("### Raw Data Preview", df.head())

    target_column = st.selectbox("Select column to clean", df.columns)

    # 🔹 Add row limit input
    row_limit = st.number_input("How many rows do you want to clean?", min_value=1, max_value=len(df), value=50, step=10)

    if st.button("🔄 Clean Data"):
        st.write(f"Cleaning first {row_limit} rows in **{target_column}**... ⏳ This may take a while.")
        progress_bar = st.progress(0)
        cleaned_rows = []

        total = row_limit
        batch_size = 50  # adjust as needed

        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = df[target_column].iloc[start:end].apply(clean_text)
            cleaned_rows.extend(batch)

            progress_bar.progress(int(end / total * 100))
            time.sleep(0.5)  # avoid hitting API limits too fast

        # Store results only for selected rows
        df[f"cleaned_{target_column}"] = pd.NA
        df.loc[:row_limit-1, f"cleaned_{target_column}"] = cleaned_rows

        st.success("✅ Cleaning complete!")

        st.write("### Cleaned Data Sample", df[[target_column, f"cleaned_{target_column}"]].head(row_limit))

        cleaned_file = "cleaned_data_limited.xlsx"
        df.to_excel(cleaned_file, index=False)
        with open(cleaned_file, "rb") as f:
            st.download_button("📥 Download Cleaned File", f, file_name="cleaned_data_limited.xlsx")
