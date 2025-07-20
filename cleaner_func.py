import pandas as pd
import re
import io
import logging
from typing import Union, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_cleaned_data(file: Union[str, io.BytesIO]) -> pd.DataFrame:
    """
    Load and clean financial data from CSV file.
    
    Args:
        file: File path (string) or file-like object
        
    Returns:
        pd.DataFrame: Cleaned dataframe ready for database insertion
    """
    encoding = 'ISO-8859-1'

    # Handle different file input types
    file_obj = None
    if isinstance(file, str):  # file path
        file_obj = file
    else:  # uploaded file-like object
        try:
            file_content = file.read()
            file_obj = io.BytesIO(file_content)
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            raise

    # Step 1: Detect Header Structure
    try:
        header_preview = pd.read_csv(file_obj, encoding=encoding, nrows=3, header=None)
        # Only seek if it's a file-like object, not a string path
        if hasattr(file_obj, 'seek'):
            file_obj.seek(0)
    except Exception as e:
        logger.error(f"Error reading CSV preview: {e}")
        raise
    
    row0 = header_preview.iloc[0]
    row1 = header_preview.iloc[1]

    # Count non-empty, non-unnamed columns in each row
    row0_named = sum(~row0.isna() & ~row0.astype(str).str.lower().str.startswith('unnamed'))
    row1_named = sum(~row1.isna() & ~row1.astype(str).str.lower().str.startswith('unnamed'))

    use_double_header = (row0_named > 2) and (row1_named > 2)
    logger.info(f"Using double header: {use_double_header}")

    # Step 2: Load data based on header structure
    if use_double_header:
        combined_headers = []
        for a, b in zip(row0, row1):
            if pd.notna(a) and not str(a).startswith("Unnamed"):
                if pd.notna(b) and not str(b).startswith("Unnamed"):
                    combined = f"{str(a).strip()} {str(b).strip()}"
                else:
                    combined = str(a).strip()
            else:
                combined = str(b).strip() if pd.notna(b) else ""
            combined_headers.append(combined)

        df = pd.read_csv(file_obj, encoding=encoding, skiprows=2, header=None)
        df.columns = combined_headers
    else:
        df = pd.read_csv(file_obj, encoding=encoding)

    logger.info(f"Initial dataset shape: {df.shape}")
    logger.info(f"Initial columns: {df.columns.tolist()}")

    # Step 3: Rename columns to match database schema
    column_mapping = {
        "Sector Code": "sector",
        "Vessel": "vessel",
        "Total Income (In Lacs) Debit/Credit Amount": "Total_Income",
        "DOE (In Lacs) Debit/Credit Amount": "DOE",
        "IOE (In Lacs) Debit/Credit Amount": "IOE",
        "GOP (In Lacs) Debit/Credit Amount": "GOP",
        "Profit before Int. & Dep. (In Lacs) Debit/Credit Amount": "PBT",
        "financial_year": "year",
        "financial_month": "month"
    }
    
    df.rename(columns=column_mapping, inplace=True)

    # Step 4: Remove blank columns
    df = df.loc[:, df.columns.str.strip() != '']

    # Step 5: Process fiscal year/period data
    fin_month_map = {
        '001': 'April', '002': 'May', '003': 'June', '004': 'July',
        '005': 'August', '006': 'September', '007': 'October',
        '008': 'November', '009': 'December', '010': 'January',
        '011': 'February', '012': 'March',
    }

    if 'Fiscal year/period' in df.columns:
        # Extract fiscal month code and year
        df['fiscal_month_code'] = df['Fiscal year/period'].astype(str).str.extract(r'/(\d{3})')
        df['financial_year'] = df['Fiscal year/period'].astype(str).str.extract(r'\.(\d{4})')
        df['financial_month'] = df['fiscal_month_code'].map(fin_month_map)

        # Handle missing values
        df['financial_month'] = df['financial_month'].fillna('Unknown')
        df['financial_year'] = df['financial_year'].fillna('Unknown')

        # Clean up temporary column
        df.drop(columns=['fiscal_month_code'], inplace=True)

        # Filter out unknown values and convert year to int
        valid_data = (df['financial_year'] != 'Unknown') & (df['financial_month'] != 'Unknown')
        df = df[valid_data]
        df['financial_year'] = df['financial_year'].astype(int)
        
        # Rename to match expected column names
        df.rename(columns={'financial_year': 'year', 'financial_month': 'month'}, inplace=True)

    # Step 6: Clean sector and vessel columns
    for col in ['sector', 'vessel']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace('nan', '')
            df[col] = df[col].replace('', pd.NA)

    # Step 7: Clean and convert KPI columns
    kpi_cols = ["Total_Income", "DOE", "IOE", "PBT", "GOP"]
    for col in kpi_cols:
        if col in df.columns:
            # Handle different number formats
            df[col] = df[col].astype(str).str.replace(',', '')  # Remove commas
            df[col] = df[col].str.replace(r'[^\d.-]', '', regex=True)  # Keep only digits, dots, and minus
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0).round(2)  # Fill NaN with 0

    # Step 8: Drop unnecessary columns
    cols_to_drop = [
        "Segment", "Voyage Number", "Fiscal year/period",
        "Depreciation (In Lacs) Debit/Credit Amount",
        "Profit After Depreciation (In Lacs) Debit/Credit Amount",
        "Finance Cost (In Lacs) Debit/Credit Amount",
        "Exchange Gain/Loss (In Lacs) Debit/Credit Amount"
    ]
    
    for col in cols_to_drop:
        if col in df.columns:
            df.drop(columns=col, inplace=True)

    # Step 9: Remove rows with missing critical data
    required_columns = ['year', 'sector', 'vessel']
    existing_required = [col for col in required_columns if col in df.columns]
    
    if existing_required:
        df = df.dropna(subset=existing_required)

    # Step 10: Final validation and logging
    logger.info(f"Final dataset shape: {df.shape}")
    if 'year' in df.columns:
        logger.info(f"Unique years: {sorted(df['year'].unique())}")
    if 'sector' in df.columns:
        logger.info(f"Unique sectors: {sorted(df['sector'].unique())}")
    if 'vessel' in df.columns:
        logger.info(f"Unique vessels: {len(df['vessel'].unique())} vessels")

    logger.info(f"Final columns: {df.columns.tolist()}")
    
    return df

def validate_data(df: pd.DataFrame) -> bool:
    """
    Validate the cleaned dataframe for common issues.
    
    Args:
        df: Cleaned dataframe
        
    Returns:
        bool: True if validation passes
    """
    issues = []
    
    # Check for required columns
    required_cols = ['year', 'sector', 'vessel']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")
    
    # Check for empty dataframe
    if df.empty:
        issues.append("Dataframe is empty")
    
    # Check for duplicate rows
    if df.duplicated().sum() > 0:
        issues.append(f"Found {df.duplicated().sum()} duplicate rows")
    
    # Check for negative values in income columns (might be valid, but worth noting)
    kpi_cols = ["Total_Income", "DOE", "IOE", "PBT", "GOP"]
    for col in kpi_cols:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                logger.warning(f"Column '{col}' has {negative_count} negative values")
    
    if issues:
        for issue in issues:
            logger.error(issue)
        return False
    
    logger.info("Data validation passed")
    return True

# Example usage
if __name__ == "__main__":
    try:
        # Load and clean data
        df_cleaned = load_cleaned_data("data/data_file.csv")
        
        # Validate the cleaned data
        if validate_data(df_cleaned):
            print("Data cleaning and validation successful!")
            print(f"Shape: {df_cleaned.shape}")
            print("\nFirst few rows:")
            print(df_cleaned.head())
        else:
            print("Data validation failed. Please check the logs.")
            
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise