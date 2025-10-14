import pandas as pd
import yaml
from pathlib import Path
import pandas as pd


def fill_gas_invoice_start_end(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill 'invoice_start' and 'invoice_end' based on earliest and latest
    'step_start' and 'step_end' per invoice_number **but preserve original
    string formats** of the step columns.
    
    Parameters:
        df (pd.DataFrame): Must have ['invoice_number', 'step_start', 'step_end', 'invoice_start', 'invoice_end']
        
    Returns:
        pd.DataFrame: Updated df with 'invoice_start' and 'invoice_end' filled
    """
    # Create temporary datetime copies for computation
    step_start_temp = pd.to_datetime(df['step_start'], errors='coerce', dayfirst=True)
    step_end_temp   = pd.to_datetime(df['step_end'], errors='coerce', dayfirst=True)

    # Find index of earliest step_start and latest step_end per invoice_number
    idx_start = df.groupby('invoice_number')['step_start'].apply(
        lambda x: x[step_start_temp[x.index].idxmin()]
    )
    idx_end = df.groupby('invoice_number')['step_end'].apply(
        lambda x: x[step_end_temp[x.index].idxmax()]
    )

    # Map back the original strings
    df['invoice_start'] = df['invoice_number'].map(idx_start)
    df['invoice_end']   = df['invoice_number'].map(idx_end)

    return df

def clean_gas_season(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes the 'Total' prefix from the 'season' column in the gas dataframe.
    e.g. 'TotalWinter' -> 'Winter', 'TotalSummer' -> 'Summer'

    Parameters:
        df (pd.DataFrame): DataFrame with a 'season' column
    
    Returns:
        pd.DataFrame: Updated DataFrame with cleaned 'season' values
    """

    df = df.copy()
    df['season'] = df['season'].str.replace(r'^Total', '', regex=True).str.strip()
    return df

def fill_electricity_step_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills step_number, step_start, and step_end for electricity invoices.
    - step_number is always 1
    - step_start = invoice_start
    - step_end = invoice_end

    Parameters:
        df (pd.DataFrame): DataFrame with columns 
            ['invoice_start', 'invoice_end', 'step_start', 'step_end', 'step_number']
    
    Returns:
        pd.DataFrame: Updated DataFrame
    """

    df = df.copy()  # Avoid modifying original DataFrame

    df['step_number'] = 1
    df['step_start'] = df['invoice_start']
    df['step_end'] = df['invoice_end']

    return df

def classify_season(date: pd.Timestamp, summer_months: dict) -> str:
    """
    Classify a date into Lumo Energy's gas billing seasons.
    - Season 1 (Summer): start_month → end_month (wraps year)
    - Season 2 (Winter): all other months
    """
    if pd.isna(date):
        return None

    month = date.month
    # Summer wraps the year: e.g., Oct (10) → May (5)
    if month >= summer_months["start_month"] or month <= summer_months["end_month"]:
        return "Summer"
    return "Winter"
    
def fill_missing_service_columns_for_water(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure 'service_days', 'service_rate', 'service_charge' columns exist,
    and fill them with 0 if missing or NaN.
    
    Parameters:
        df (pd.DataFrame)
    
    Returns:
        pd.DataFrame with the columns filled
    """
    df = df.copy()
    service_cols = ["service_days", "service_rate", "service_charge"]
    
    for col in service_cols:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = df[col].fillna(0)
    
    return df

def fill_water_step_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    For water_df, fill missing step_start and step_end dates 
    with invoice_start and invoice_end respectively.
    
    Parameters:
        df (pd.DataFrame): Water dataframe with columns 
            ['invoice_start', 'invoice_end', 'step_start', 'step_end', ...]
    
    Returns:
        pd.DataFrame: DataFrame with step_start and step_end filled
    """
    # Ensure date columns are datetime
    date_cols = ["invoice_start", "invoice_end", "step_start", "step_end"]
    for col in date_cols:
        df[col] = df[col]
    
    # Fill step_start / step_end if null
    df["step_start"] = df["step_start"].fillna(df["invoice_start"])
    df["step_end"]   = df["step_end"].fillna(df["invoice_end"])
    
    return df
