import os
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# Load API Key from .env
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

if __name__ == "__main__":
    # Load your dataset (adjust path if needed)
    df = pd.read_excel("data/Aug_hackathon_medical_data.xlsx")

    # Just test first 5 rows of 'provisionaldiagnosis'
    df["cleaned_provisionaldiagnosis"] = df["provisionaldiagnosis"].head(5).apply(clean_text)

    print(df[["provisionaldiagnosis", "cleaned_provisionaldiagnosis"]])
