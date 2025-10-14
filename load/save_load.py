import pandas as pd
from pathlib import Path

def save_dataframe_to_csv(df: pd.DataFrame, output_path: str, index: bool = False, sep: str = ",") -> None:
    """
    Save a pandas DataFrame to a CSV file. Throws an error if the output folder does not exist.

    Parameters:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): Full path to the output CSV file (including filename).
        index (bool): Whether to write row names (default: False).
        sep (str): CSV delimiter (default: ",").

    Raises:
        FileNotFoundError: If the folder containing output_path does not exist.
    """
    output_path = Path(output_path)
    output_folder = output_path.parent

    if not output_folder.exists():
        raise FileNotFoundError(f"Output folder '{output_folder}' does not exist. Please create it first.")

    # Save the CSV
    df.to_csv(output_path, index=index, sep=sep)
    print(f"DataFrame saved to {output_path}")