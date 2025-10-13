from parse.parse_electricity import parse_electricity_pdf
from parse.parse_water import parse_water_pdf
from parse.parse_gas import parse_gas_pdf

import os
import glob
import pandas as pd
import pdfplumber

def parse_all_pdfs(folder_path, utility_type, pattern="*.pdf"):
    """
    Parses all PDFs in a folder and returns a Pandas DataFrame.
    Chooses parser based on utility_type.

    Args:
        folder_path (str): Path to the folder containing PDFs
        utility_type (str): 'water' or 'elec' (case-insensitive)
        pattern (str): Glob pattern to match files (default: *.pdf)

    Returns:
        pd.DataFrame: Each row is a table entry with invoice info
    """
    all_table_data = []

    pdf_files = glob.glob(os.path.join(folder_path, pattern))
    for pdf_path in pdf_files:
        # Extract full text from PDF
        with pdfplumber.open(pdf_path) as pdf:
            full_text = "\n".join(
                page.extract_text() for page in pdf.pages if page.extract_text()
            )

        # Call the correct parser based on utility type
        if utility_type.lower() == "water":
            table_data = parse_water_pdf(full_text, pdf_path)
        elif utility_type.lower() == "elec":
            table_data = parse_electricity_pdf(full_text, pdf_path)
        elif utility_type.lower() == "gas":
            table_data = parse_gas_pdf(full_text, pdf_path)
        else:
            raise ValueError(f"Unsupported utility type: {utility_type}")

        all_table_data.extend(table_data)  # Add each table row to the list

    # Convert to Pandas DataFrame
    df = pd.DataFrame(all_table_data)
    return df