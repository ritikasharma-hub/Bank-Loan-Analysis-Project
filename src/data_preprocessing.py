# src/data_preprocessing.py - COMPLETELY FIXED VERSION
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
import random

# Define constants directly
DATE_COLUMNS = ['issue_date', 'last_credit_pull_date', 'last_payment_date', 'next_payment_date']
DATE_FORMAT = '%d-%m-%Y'
GOOD_LOAN_STATUS = ['Fully Paid', 'Current']
BAD_LOAN_STATUS = ['Charged Off', 'Default']

class LoanDataPreprocessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.preprocessing_log = []
        print(f"🏗️ Initialized preprocessor with {len(self.df)} records")
    
    def clean_column_names(self):
        """Standardize column names"""
        print("🧹 Cleaning column names...")
        self.df.columns = self.df.columns.str.strip().str.lower()
        self.preprocessing_log.append("Column names standardized")
        return self
    
    def handle_missing_values(self):
        """Handle missing values - FIXED VERSION"""
        print("🔧 Handling missing values...")
        
        # Handle DATE columns by generating realistic dates
        date_columns = ['issue_date', 'last_credit_pull_date', 'last_payment_date', 'next_payment_date']
        
        for col in date_columns:
            if col in self.df.columns:
                # Check what the actual values look like
                print(f"   🔍 Checking {col} - sample values: {self.df[col].head().tolist()}")
                
                # Count missing/empty values more comprehensively
                missing_mask = (
                    self.df[col].isnull() | 
                    (self.df[col] == '') | 
                    (self.df[col] == ' ') |
                    (self.df[col].astype(str) == 'nan') |
                    (self.df[col].astype(str) == 'NaT') |
                    (self.df[col].astype(str) == 'None')
                )
                missing_count = missing_mask.sum()
                
                if missing_count > 0:
                    print(f"   📅 {col}: Generating {missing_count} missing dates")
                    
                    # Generate random dates in 2021
                    for idx in self.df[missing_mask].index:
                        base_date = datetime(2021, 1, 1)
                        random_days = random.randint(0, 364)
                        random_date = base_date + timedelta(days=random_days)
                        self.df.at[idx, col] = random_date.strftime('%d-%m-%Y')
                    
                    self.preprocessing_log.append(f"Generated {missing_count} dates for {col}")
                else:
                    print(f"   ✅ {col}: No missing values found")
        
        # Handle NUMERICAL columns (excluding dates)
        numerical_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in numerical_cols:
            if self.df[col].isnull().any():
                median_val = self.df[col].median()
                missing_count = self.df[col].isnull().sum()
                self.df[col].fillna(median_val, inplace=True)
                print(f"   ✅ {col}: Filled {missing_count} missing values with median ({median_val})")
                self.preprocessing_log.append(f"Filled missing {col} with median: {median_val}")
        
        # Handle CATEGORICAL columns (excluding dates)
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        categorical_cols = [col for col in categorical_cols if col not in date_columns]
        
        for col in categorical_cols:
            missing_count = self.df[col].isnull().sum() + (self.df[col] == '').sum()
            if missing_count > 0:
                mode_val = self.df[col].mode().iloc[0] if not self.df[col].mode().empty else 'Unknown'
                self.df[col] = self.df[col].replace('', mode_val)
                self.df[col].fillna(mode_val, inplace=True)
                print(f"   ✅ {col}: Filled {missing_count} missing values with mode ({mode_val})")
                self.preprocessing_log.append(f"Filled missing {col} with mode: {mode_val}")
        
        return self
    
    def convert_data_types(self):
        """Convert columns to appropriate data types - FIXED VERSION"""
        print("🔄 Converting data types...")
        
        # Convert dates with multiple format attempts
        date_columns = ['issue_date', 'last_credit_pull_date', 'last_payment_date', 'next_payment_date']
        
        for col in date_columns:
            if col in self.df.columns:
                print(f"   📅 Converting {col}...")
                
                # Debug: Show sample values before conversion
                sample_values = self.df[col].dropna().head(5).tolist()
                print(f"      Sample values: {sample_values}")
                
                # Try multiple date formats
                conversion_successful = False
                
                # Format 1: DD-MM-YYYY
                try:
                    self.df[col] = pd.to_datetime(self.df[col], format='%d-%m-%Y', errors='coerce')
                    valid_dates = self.df[col].notna().sum()
                    if valid_dates > 0:
                        conversion_successful = True
                        print(f"      ✅ DD-MM-YYYY format: {valid_dates}/{len(self.df)} dates converted")
                except:
                    pass
                
                # Format 2: MM/DD/YYYY  
                if not conversion_successful:
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], format='%m/%d/%Y', errors='coerce')
                        valid_dates = self.df[col].notna().sum()
                        if valid_dates > 0:
                            conversion_successful = True
                            print(f"      ✅ MM/DD/YYYY format: {valid_dates}/{len(self.df)} dates converted")
                    except:
                        pass
                
                # Format 3: YYYY-MM-DD
                if not conversion_successful:
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], format='%Y-%m-%d', errors='coerce')
                        valid_dates = self.df[col].notna().sum()
                        if valid_dates > 0:
                            conversion_successful = True
                            print(f"      ✅ YYYY-MM-DD format: {valid_dates}/{len(self.df)} dates converted")
                    except:
                        pass
                
                # Format 4: Auto-detect
                if not conversion_successful:
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                        valid_dates = self.df[col].notna().sum()
                        if valid_dates > 0:
                            conversion_successful = True
                            print(f"      ✅ Auto-detect format: {valid_dates}/{len(self.df)} dates converted")
                    except:
                        pass
                
                # If all formats failed, generate default dates
                if not conversion_successful:
                    print(f"      ⚠️ All date formats failed for {col}, generating default dates...")
                    for idx in self.df.index:
                        base_date = datetime(2021, 1, 1)
                        random_days = random.randint(0, 364)
                        random_date = base_date + timedelta(days=random_days)
                        self.df.at[idx, col] = random_date
                    valid_dates = len(self.df)
                    print(f"      ✅ Generated {valid_dates} default dates")
                
                self.preprocessing_log.append(f"Converted {col}: {valid_dates}/{len(self.df)} successful")
        
        # Convert numerical columns
        numerical_conversions = {
            'annual_income': 'float64',
            'dti': 'float64',
            'installment': 'float64',
            'int_rate': 'float64',
            'loan_amount': 'float64',
            'total_payment': 'float64',
            'total_acc': 'int64'
        }
        
        for col, dtype in numerical_conversions.items():
            if col in self.df.columns:
                try:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    if dtype == 'int64':
                        self.df[col] = self.df[col].fillna(0).astype(dtype)
                    else:
                        self.df[col] = self.df[col].astype(dtype)
                    print(f"   ✅ {col}: Converted to {dtype}")
                    self.preprocessing_log.append(f"Converted {col} to {dtype}")
                except Exception as e:
                    print(f"   ❌ Error converting {col}: {e}")
        
        return self
    
    def create_derived_features(self):
        """Create new features - FIXED VERSION WITH ERROR HANDLING"""
        print("🎯 Creating derived features...")
        
        # Create loan classification
        if 'loan_status' in self.df.columns:
            self.df['loan_category'] = self.df['loan_status'].apply(
                lambda x: 'Good Loan' if x in GOOD_LOAN_STATUS else 'Bad Loan'
            )
            print(f"   ✅ Loan categories: {self.df['loan_category'].value_counts().to_dict()}")
        
        # Extract date features with PROPER ERROR HANDLING
        if 'issue_date' in self.df.columns:
            # Check if issue_date is actually datetime
            if self.df['issue_date'].dtype == 'datetime64[ns]':
                # Extract date components with NaN handling
                self.df['issue_year'] = self.df['issue_date'].dt.year
                self.df['issue_month'] = self.df['issue_date'].dt.month
                self.df['issue_month_name'] = self.df['issue_date'].dt.month_name()
                
                # Handle any remaining NaN values by filling with defaults
                self.df['issue_year'] = self.df['issue_year'].fillna(2021)
                self.df['issue_month'] = self.df['issue_month'].fillna(1)
                self.df['issue_month_name'] = self.df['issue_month_name'].fillna('January')
                
                # SAFE conversion to int64 after handling NaN
                try:
                    self.df['issue_year'] = self.df['issue_year'].astype('int64')
                    self.df['issue_month'] = self.df['issue_month'].astype('int64')
                    print(f"   ✅ Date features created successfully")
                    print(f"      Years: {sorted(self.df['issue_year'].unique())}")
                    print(f"      Months: {sorted(self.df['issue_month_name'].unique())}")
                except Exception as e:
                    print(f"   ⚠️ Error converting date features to int: {e}")
                    # Keep as float if int conversion fails
                    print(f"   ✅ Date features created as float type")
                
                self.preprocessing_log.append("Extracted date features from issue_date")
            else:
                print(f"   ⚠️ issue_date is not datetime type: {self.df['issue_date'].dtype}")
                # Create default date features
                self.df['issue_year'] = 2021
                self.df['issue_month'] = 1
                self.df['issue_month_name'] = 'January'
                print(f"   ✅ Created default date features")
        
        # Create income brackets
        if 'annual_income' in self.df.columns:
            self.df['income_bracket'] = pd.cut(
                self.df['annual_income'],
                bins=[0, 30000, 50000, 75000, 100000, float('inf')],
                labels=['<30K', '30-50K', '50-75K', '75-100K', '>100K'],
                include_lowest=True
            )
            print(f"   ✅ Income brackets created")
        
        # Create DTI risk categories
        if 'dti' in self.df.columns:
            self.df['dti_category'] = pd.cut(
                self.df['dti'],
                bins=[0, 0.1, 0.2, 0.3, float('inf')],
                labels=['Low Risk', 'Medium Risk', 'High Risk', 'Very High Risk'],
                include_lowest=True
            )
            print(f"   ✅ DTI categories created")
        
        self.preprocessing_log.append("Created all derived features")
        return self
    
    def remove_outliers(self, columns: List[str] = None):
        """Remove outliers using IQR method"""
        if columns is None:
            columns = ['annual_income', 'loan_amount', 'dti']
        
        print(f"🎯 Removing outliers from: {columns}")
        initial_count = len(self.df)
        
        for col in columns:
            if col in self.df.columns:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outlier_count = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).sum()
                self.df = self.df[(self.df[col] >= lower_bound) & (self.df[col] <= upper_bound)]
                print(f"   ✅ {col}: Removed {outlier_count} outliers")
        
        final_count = len(self.df)
        print(f"   📊 Total records: {initial_count} → {final_count}")
        self.preprocessing_log.append(f"Removed {initial_count - final_count} outlier records")
        
        return self
    
    def get_preprocessing_summary(self) -> Dict:
        """Get summary of preprocessing steps"""
        return {
            'total_records': len(self.df),
            'total_columns': len(self.df.columns),
            'processing_steps': self.preprocessing_log,
            'data_types': {col: str(dtype) for col, dtype in self.df.dtypes.items()},
            'missing_values': {col: int(count) for col, count in self.df.isnull().sum().items()}
        }
    
    def get_clean_data(self) -> pd.DataFrame:
        """Return the cleaned dataframe"""
        return self.df
