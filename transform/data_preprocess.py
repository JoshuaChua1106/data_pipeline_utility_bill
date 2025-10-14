import pandas as pd

def fill_gas_invoice_start_end(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills 'invoice_start' and 'invoice_end' in a DataFrame based on
    the earliest 'step_start' and latest 'step_end' per invoice_number.
    
    Parameters:
        df (pd.DataFrame): DataFrame with columns 
            ['invoice_number', 'step_start', 'step_end', 'invoice_start', 'invoice_end']
    
    Returns:
        pd.DataFrame: Updated DataFrame with 'invoice_start' and 'invoice_end' filled
    """

    # Ensure date columns are in datetime format
    df['step_start'] = pd.to_datetime(df['step_start'])
    df['step_end'] = pd.to_datetime(df['step_end'])

    # Group by invoice_number to find earliest and latest step dates
    invoice_dates = (
        df.groupby('invoice_number')
        .agg(
            invoice_start=('step_start', 'min'),
            invoice_end=('step_end', 'max')
        )
    )

    # Fill the empty invoice_start and invoice_end columns
    df['invoice_start'] = df['invoice_number'].map(invoice_dates['invoice_start'])
    df['invoice_end'] = df['invoice_number'].map(invoice_dates['invoice_end'])

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