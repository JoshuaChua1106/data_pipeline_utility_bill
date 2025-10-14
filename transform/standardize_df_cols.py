import numpy as np
import pandas as pd
import yaml
from pathlib import Path

def standardize_column_names(df, final_labels):
    # Add missing columns with NaN
    for col in final_labels:
        if col not in df.columns:
            df[col] = np.nan
    # Reorder columns
    df = df[final_labels]
    return df

def parse_water_dates(series: pd.Series) -> pd.Series:
    """
    Parse water invoice/step dates with mixed formats:
    - Short month format: 1May2024, 2Feb2025
    - Standard format: 30/06/2024
    - 'NULL' -> NaT
    """
    series_clean = series.replace("NULL", np.nan)

    # Try short month format first
    parsed = pd.to_datetime(series_clean, format='%d%b%Y', errors='coerce')

    # Fill NaT with standard format (dd/mm/yyyy)
    parsed = parsed.fillna(pd.to_datetime(series_clean, dayfirst=True, errors='coerce'))

    return parsed


def standardize_column_datatypes(df: pd.DataFrame, column_dtypes: dict, utility_type: str) -> pd.DataFrame:
    """
    Standardize a DataFrame by converting columns to the datatypes
    defined in column_dtypes. Handles water utility dates separately.

    Parameters:
        df (pd.DataFrame): Input DataFrame
        column_dtypes (dict): Mapping of column names to target dtypes
        utility_type (str): Type of utility ('Water', 'Electricity', 'Gas')

    Returns:
        pd.DataFrame: Standardized DataFrame
    """
    df = df.copy()

    for col, dtype in column_dtypes.items():
        if col in df.columns:
            print(f"Standardizing column: {col} → {dtype}")

            if dtype == "datetime":
                if utility_type.lower() == "water" and col in ["invoice_date", "invoice_start", "invoice_end", "step_start", "step_end"]:
                    df[col] = parse_water_dates(df[col])
                else:
                    # electricity/gas: infer format
                    df[col] = pd.to_datetime(df[col].replace("NULL", np.nan), dayfirst=True, errors='coerce')
            elif dtype == "string":
                df[col] = df[col].astype(str, errors='ignore')
            elif dtype in ["float", "int"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    return df