# src/config.py - UPDATED VERSION
import os
from pathlib import Path

# Project paths - Updated for your directory structure
BASE_DIR = Path(r'C:\Users\LENOVO\Documents\LOAN DATA PROJECT')
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = BASE_DIR / "databook" / "raw"  # Your actual raw data location
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXPORTS_DIR = DATA_DIR / "exports"

# File paths
RAW_DATA_FILE = RAW_DATA_DIR / "financial_loan.csv"  # Your actual file
CLEAN_DATA_FILE = PROCESSED_DATA_DIR / "loan_data_clean.csv"
POWERBI_EXPORT = EXPORTS_DIR / "powerbi_data.csv"

# Data validation rules
REQUIRED_COLUMNS = [
    'id', 'address_state', 'application_type', 'emp_length', 'grade',
    'home_ownership', 'issue_date', 'loan_status', 'purpose', 'term',
    'annual_income', 'dti', 'installment', 'int_rate', 'loan_amount',
    'total_payment'
]

# Good vs Bad loan classification
GOOD_LOAN_STATUS = ['Fully Paid', 'Current']   
BAD_LOAN_STATUS = ['Charged Off', 'Default']

# Date formats
DATE_COLUMNS = ['issue_date', 'last_credit_pull_date', 'last_payment_date']
DATE_FORMAT = '%d-%m-%Y'
