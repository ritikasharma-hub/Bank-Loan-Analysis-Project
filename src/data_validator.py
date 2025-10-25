# src/data_validator.py
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from .config import REQUIRED_COLUMNS, GOOD_LOAN_STATUS, BAD_LOAN_STATUS

class DataValidator:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.validation_report = {}
    
    def check_missing_columns(self) -> List[str]:
        """Check for missing required columns"""
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in self.df.columns]
        self.validation_report['missing_columns'] = missing_cols
        return missing_cols
    
    def check_missing_values(self) -> Dict[str, int]:
        """Check for missing values in each column"""
        missing_values = self.df.isnull().sum()
        self.validation_report['missing_values'] = missing_values.to_dict()
        return missing_values.to_dict()
    
    def check_data_types(self) -> Dict[str, str]:
        """Check current data types"""
        data_types = self.df.dtypes.astype(str).to_dict()
        self.validation_report['data_types'] = data_types
        return data_types
    
    def check_outliers(self, columns: List[str]) -> Dict[str, Dict]:
        """Detect outliers using IQR method"""
        outliers = {}
        for col in columns:
            if col in self.df.columns and pd.api.types.is_numeric_dtype(self.df[col]):
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outlier_mask = (self.df[col] < lower_bound) | (self.df[col] > upper_bound)
                outliers[col] = {
                    'count': outlier_mask.sum(),
                    'percentage': (outlier_mask.sum() / len(self.df)) * 100,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound
                }
        
        self.validation_report['outliers'] = outliers
        return outliers
    
    def generate_report(self) -> Dict:
        """Generate comprehensive validation report"""
        self.check_missing_columns()
        self.check_missing_values()
        self.check_data_types()
        self.check_outliers(['annual_income', 'loan_amount', 'int_rate', 'dti'])
        
        return self.validation_report
