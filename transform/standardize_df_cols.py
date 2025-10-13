import numpy as np

def standardize_columns(df, final_labels):
    # Add missing columns with NaN
    for col in final_labels:
        if col not in df.columns:
            df[col] = np.nan
    # Reorder columns
    df = df[final_labels]
    return df