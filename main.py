# main.py - SIMPLIFIED VERSION
import pandas as pd
from src.data_validator import DataValidator
from src.data_preprocessing import LoanDataPreprocessor
import os

def main():
    """Main data preprocessing pipeline"""
    print("Starting Bank Loan Data Preprocessing Pipeline...")
    
    # Define paths directly
    raw_file = r'C:\Users\LENOVO\Documents\LOAN DATA PROJECT\databook\raw\financial_loan.csv'
    clean_file = r'C:\Users\LENOVO\Documents\LOAN DATA PROJECT\data\processed\loan_data_clean.csv'
    powerbi_file = r'C:\Users\LENOVO\Documents\LOAN DATA PROJECT\data\exports\powerbi_data.csv'
    
    # Create directories if they don't exist
    os.makedirs(os.path.dirname(clean_file), exist_ok=True)
    os.makedirs(os.path.dirname(powerbi_file), exist_ok=True)
    
    # Step 1: Load raw data
    print("Step 1: Loading raw data...")
    df = pd.read_csv(raw_file)
    print(f"Loaded {len(df)} records with {len(df.columns)} columns")
    
    # Step 2: Data Preprocessing
    print("\nStep 2: Starting data preprocessing...")
    preprocessor = LoanDataPreprocessor(df)
    
    # Run preprocessing pipeline
    clean_df = (preprocessor
                .clean_column_names()
                .handle_missing_values()
                .convert_data_types()
                .create_derived_features()
                .remove_outliers()
                .get_clean_data())
    
    # Step 3: Save processed data
    print("\nStep 3: Saving processed data...")
    clean_df.to_csv(clean_file, index=False)
    clean_df.to_csv(powerbi_file, index=False)
    
    # Step 4: Generate summary
    summary = preprocessor.get_preprocessing_summary()
    print("\nPreprocessing Summary:")
    print(f"  Final records: {summary['total_records']}")
    print(f"  Total columns: {summary['total_columns']}")
    
    print("\nProcessing Steps:")
    for step in summary['processing_steps']:
        print(f"  - {step}")
    
    print(f"\n✅ Data preprocessing completed!")
    print(f"📁 Files saved:")
    print(f"   - {clean_file}")
    print(f"   - {powerbi_file}")

if __name__ == "__main__":
    main()
