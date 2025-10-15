import os
import yaml
import pandas as pd
import argparse
from dotenv import load_dotenv
from pathlib import Path

from googleapiclient.errors import HttpError

from extract.gmail_connector import connect_gmail
from extract.email_filter import search_emails
from extract.pdf_downloader import download_pdf_attachments

from parse.pdf_parser_base import parse_all_pdfs
from transform.standardize_df_cols import standardize_column_names, standardize_column_datatypes
from transform.data_preprocess import fill_gas_invoice_start_end,fill_electricity_step_fields, clean_gas_season, classify_season, fill_missing_service_columns_for_water, fill_water_step_dates

from load.save_load import save_dataframe_to_csv


def run_extract_stage():
    """Stage 1: Connect to Gmail and download PDFs"""
    print("=== EXTRACT STAGE ===")
    
    # Gmail connection
    credentials_relative = os.getenv("GMAIL_CREDENTIALS_PATH")
    token_relative = os.getenv("GMAIL_TOKEN_PATH")
    
    credentials_path = BASE_DIR / credentials_relative
    token_path = BASE_DIR / token_relative
    
    service = connect_gmail(credentials_file=credentials_path, token_file=token_path)
    
    

    # Verify connection
    try:
        profile = service.users().getProfile(userId='me').execute()
        print("✓ Gmail connection successful!")
        print(f"✓ Email address: {profile.get('emailAddress')}")
        
        messages = service.users().messages().list(userId='me', maxResults=5).execute()
        print(f"✓ Found {len(messages.get('messages', []))} messages")
        
    except HttpError as error:
        print(f"✗ Gmail connection failed: {error}")
        return False
    
    # Search for emails
    elec_search_query = config["gmail_queries"]["elec"]
    water_search_query = config["gmail_queries"]["water"]
    gas_search_query = config["gmail_queries"]["gas"]
    
    elec_emails = search_emails(service, elec_search_query)
    water_emails = search_emails(service, water_search_query)
    gas_emails = search_emails(service, gas_search_query)
    
    print(f"✓ Found {len(elec_emails)} electricity emails")
    print(f"✓ Found {len(water_emails)} water emails") 
    print(f"✓ Found {len(gas_emails)} gas emails")
    
    # Download PDFs
    elec_pdf_filepath = BASE_DIR / config["paths"]["elec_pdf_raw"]
    water_pdf_filepath = BASE_DIR / config["paths"]["water_pdf_raw"]
    gas_pdf_filepath = BASE_DIR / config["paths"]["gas_pdf_raw"]
    
    download_pdf_attachments(service, elec_emails, save_folder=elec_pdf_filepath)
    download_pdf_attachments(service, gas_emails, save_folder=gas_pdf_filepath)
    download_pdf_attachments(service, water_emails, save_folder=water_pdf_filepath)
    
    print("✓ Extract stage completed!")
    return True


def run_parse_stage():
    """Stage 2: Parse PDFs to CSV"""
    print("=== PARSE STAGE ===")
    
    elec_pdf_filepath = BASE_DIR / config["paths"]["elec_pdf_raw"]
    water_pdf_filepath = BASE_DIR / config["paths"]["water_pdf_raw"]
    gas_pdf_filepath = BASE_DIR / config["paths"]["gas_pdf_raw"]
    
    # Parse PDFs
    elec_df = parse_all_pdfs(elec_pdf_filepath, "elec")
    water_df = parse_all_pdfs(water_pdf_filepath, "water")
    gas_df = parse_all_pdfs(gas_pdf_filepath, "gas")
    
    print(f"✓ Parsed {len(elec_df)} electricity records")
    print(f"✓ Parsed {len(water_df)} water records")
    print(f"✓ Parsed {len(gas_df)} gas records")
    
    # Save raw CSVs
    elec_raw_df_output_path = BASE_DIR / config["paths"]["elec_df_raw"]
    water_raw_df_output_path = BASE_DIR / config["paths"]["water_df_raw"]
    gas_raw_df_output_path = BASE_DIR / config["paths"]["gas_df_raw"]
    
    save_dataframe_to_csv(elec_df, elec_raw_df_output_path)
    save_dataframe_to_csv(water_df, water_raw_df_output_path)
    save_dataframe_to_csv(gas_df, gas_raw_df_output_path)
    
    print("✓ Parse stage completed!")
    return True


def run_transform_stage():
    """Stage 3: Transform data to silver layer"""
    print("=== TRANSFORM STAGE ===")
    
    # Load raw data
    elec_raw_df_output_path = BASE_DIR / config["paths"]["elec_df_raw"]
    water_raw_df_output_path = BASE_DIR / config["paths"]["water_df_raw"]
    gas_raw_df_output_path = BASE_DIR / config["paths"]["gas_df_raw"]
    
    elec_df_silver = pd.read_csv(elec_raw_df_output_path)
    water_df_silver = pd.read_csv(water_raw_df_output_path)
    gas_df_silver = pd.read_csv(gas_raw_df_output_path)
    
    # Rename columns
    final_labels = config["columns"]["final_labels"]
    elec_rename = config["columns"]["elec_rename"]
    water_rename = config["columns"]["water_rename"]
    gas_rename = config["columns"]["gas_rename"]
    
    elec_df_silver.columns = elec_rename
    water_df_silver.columns = water_rename
    gas_df_silver.columns = gas_rename
    
    # Standardize columns
    elec_df_silver = standardize_column_names(elec_df_silver, final_labels)
    water_df_silver = standardize_column_names(water_df_silver, final_labels)
    gas_df_silver = standardize_column_names(gas_df_silver, final_labels)
    
    # Process missing values
    elec_df_silver = fill_electricity_step_fields(elec_df_silver)
    water_df_silver = fill_missing_service_columns_for_water(water_df_silver)
    gas_df_silver = fill_gas_invoice_start_end(gas_df_silver)
    gas_df_silver = clean_gas_season(gas_df_silver)
    
    # Standardize data types
    dtypes = config["column_dtypes"]
    elec_df_silver = standardize_column_datatypes(elec_df_silver, dtypes, "elec")
    water_df_silver = standardize_column_datatypes(water_df_silver, dtypes, "water")
    gas_df_silver = standardize_column_datatypes(gas_df_silver, dtypes, "gas")
    
    # Add seasons
    summer_months = config["seasons"]["summer"]
    elec_df_silver["season"] = elec_df_silver["invoice_start"].apply(
        lambda x: classify_season(x, summer_months)
    )
    water_df_silver["season"] = water_df_silver["invoice_start"].apply(
        lambda x: classify_season(x, summer_months)
    )
    
    # Fill water step dates
    water_df_silver = fill_water_step_dates(water_df_silver)
    
    # Save silver layer
    elec_silver_output_path = BASE_DIR / config["paths"]["elec_silver_output_path"]
    water_silver_output_path = BASE_DIR / config["paths"]["water_silver_output_path"]
    gas_silver_output_path = BASE_DIR / config["paths"]["gas_silver_output_path"]
    
    save_dataframe_to_csv(elec_df_silver, elec_silver_output_path)
    save_dataframe_to_csv(water_df_silver, water_silver_output_path)
    save_dataframe_to_csv(gas_df_silver, gas_silver_output_path)
    
    print("✓ Transform stage completed!")
    return True


def run_load_stage():
    """Stage 4: Combine data to gold layer"""
    print("=== LOAD STAGE ===")
    
    # Load silver data
    elec_silver_output_path = BASE_DIR / config["paths"]["elec_silver_output_path"]
    water_silver_output_path = BASE_DIR / config["paths"]["water_silver_output_path"]
    gas_silver_output_path = BASE_DIR / config["paths"]["gas_silver_output_path"]
    
    elec_df_combine = pd.read_csv(elec_silver_output_path)
    water_df_combine = pd.read_csv(water_silver_output_path)
    gas_df_combine = pd.read_csv(gas_silver_output_path)
    
    # Combine datasets
    utilities_gold_df = pd.concat([elec_df_combine, water_df_combine, gas_df_combine], ignore_index=True)
    
    print(f"✓ Combined {len(utilities_gold_df)} total records")
    
    # Save gold layer
    utilities_gold_output_path = BASE_DIR / config["paths"]["utiltities_gold_output_path"]
    save_dataframe_to_csv(utilities_gold_df, utilities_gold_output_path)
    
    print("✓ Load stage completed!")
    return True


def run_full_pipeline():
    """Run all stages in sequence"""
    print("🚀 Starting full utility bill pipeline...")
    
    stages = [
        ("extract", run_extract_stage),
        ("parse", run_parse_stage), 
        ("transform", run_transform_stage),
        ("load", run_load_stage)
    ]
    
    for stage_name, stage_func in stages:
        try:
            success = stage_func()
            if not success:
                print(f"✗ {stage_name} stage failed!")
                return False
        except Exception as e:
            print(f"✗ {stage_name} stage error: {e}")
            return False
    
    print("🎉 Pipeline completed successfully!")
    return True


if __name__ == "__main__":

    # Configuration
    load_dotenv()
    BASE_DIR = Path(__file__).resolve().parent
    config_path = BASE_DIR / "config/config.yaml"

    with open(config_path) as f:
        config = yaml.safe_load(f)

    parser = argparse.ArgumentParser(description="Utility Bill Data Pipeline")
    parser.add_argument(
        "--stage", 
        choices=["extract", "parse", "transform", "load", "all"],
        default="all",
        help="Run specific stage or full pipeline (default: all)"
    )
    
    args = parser.parse_args()
    
    if args.stage == "extract":
        run_extract_stage()
    elif args.stage == "parse":
        run_parse_stage()
    elif args.stage == "transform":
        run_transform_stage()
    elif args.stage == "load":
        run_load_stage()
    else:
        run_full_pipeline()

