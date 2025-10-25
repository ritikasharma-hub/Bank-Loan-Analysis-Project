# src/kpi_calculator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any

class LoanKPICalculator:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.current_date = datetime.now()
        
    def calculate_primary_kpis(self) -> Dict[str, Any]:
        """Calculate main KPIs from problem statement"""
        return {
            'total_loan_applications': len(self.df),
            'total_funded_amount': self.df['loan_amount'].sum(),
            'total_amount_received': self.df['total_payment'].sum(),
            'average_interest_rate': self.df['int_rate'].mean(),
            'average_dti': self.df['dti'].mean()
        }
    
    def calculate_good_bad_loans(self) -> Dict[str, Dict]:
        """Calculate Good vs Bad loan metrics"""
        good_loans = self.df[self.df['loan_category'] == 'Good Loan']
        bad_loans = self.df[self.df['loan_category'] == 'Bad Loan']
        
        return {
            'good_loans': {
                'percentage': (len(good_loans) / len(self.df)) * 100,
                'applications': len(good_loans),
                'funded_amount': good_loans['loan_amount'].sum(),
                'received_amount': good_loans['total_payment'].sum()
            },
            'bad_loans': {
                'percentage': (len(bad_loans) / len(self.df)) * 100,
                'applications': len(bad_loans),
                'funded_amount': bad_loans['loan_amount'].sum(),
                'received_amount': bad_loans['total_payment'].sum()
            }
        }
