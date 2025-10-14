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

def standardize_column_datatypes(df: pd.DataFrame, config_path: str) -> pd.DataFrame:
    """
    Standardize a DataFrame by converting columns to the datatypes
    defined in config.yaml.

    Parameters:
        df (pd.DataFrame): Input DataFrame
        config_path (str): Path to config.yaml containing column_dtypes

    Returns:
        pd.DataFrame: Standardized DataFrame
    """
    df = df.copy()

    # Load column datatypes from config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    dtypes = config["column_dtypes"]

    # Apply datatypes
    for col, dtype in dtypes.items():
        if col in df.columns:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col])
            else:
                df[col] = df[col].astype(dtype)

    return df