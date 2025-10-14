import os
import yaml
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from googleapiclient.errors import HttpError

from extract.gmail_connector import connect_gmail
from extract.email_filter import search_emails
from extract.pdf_downloader import download_pdf_attachments

from parse.pdf_parser_base import parse_all_pdfs
from transform.standardize_df_cols import standardize_column_names, standardize_column_datatypes
from transform.data_preprocess import fill_gas_invoice_start_end,fill_electricity_step_fields, clean_gas_season, classify_season, fill_missing_service_columns_for_water, fill_water_step_dates

# Configuration
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
config_path = BASE_DIR / "config/config.yaml"

with open(config_path) as f:
    config = yaml.safe_load(f)



# Stage 1: Connect to Gmail
credentials_relative = os.getenv("GMAIL_CREDENTIALS_PATH")
token_relative = os.getenv("GMAIL_TOKEN_PATH")

credentials_path = BASE_DIR / credentials_relative
token_path = BASE_DIR / token_relative

service = connect_gmail(credentials_file=credentials_path, token_file=token_path)

# Verify if connection is successful
try:
    # Simple test: get your Gmail profile
    profile = service.users().getProfile(userId='me').execute()
    print("Connection successful!")
    print("Email address:", profile.get('emailAddress'))
    
    # Optional: list first 5 messages
    messages = service.users().messages().list(userId='me', maxResults=5).execute()
    print(f"Found {len(messages.get('messages', []))} messages")
    
except HttpError as error:
    print(f"An error occurred: {error}")

# Step 2: Filter for relevant emails
elec_search_query = config["gmail_queries"]["elec"]
water_search_query = config["gmail_queries"]["water"]
gas_search_query = config["gmail_queries"]["gas"]

elec_emails = search_emails(service, elec_search_query)
water_emails = search_emails(service, water_search_query)
gas_emails = search_emails(service, gas_search_query)


# Step 3: Download Invoice PDF from emails
elec_pdf_filepath = BASE_DIR / config["paths"]["elec_pdf_raw"]
water_pdf_filepath = BASE_DIR / config["paths"]["water_pdf_raw"]
gas_pdf_filepath = BASE_DIR / config["paths"]["gas_pdf_raw"]

download_pdf_attachments(service, elec_emails, save_folder= elec_pdf_filepath)
download_pdf_attachments(service, gas_emails, save_folder=gas_pdf_filepath)
download_pdf_attachments(service, water_emails, save_folder=water_pdf_filepath)


# Step 4: Parse PDFs using PDF Plumber and save as a dataframe
elec_df = parse_all_pdfs(elec_pdf_filepath, "elec")
water_df = parse_all_pdfs(water_pdf_filepath, "water")
gas_df = parse_all_pdfs(gas_pdf_filepath, "gas")

    # Step 4.1: Save as dataframe
elec_raw_dataframe = BASE_DIR / config["paths"]["elec_df_raw"]
water_raw_dataframe = BASE_DIR / config["paths"]["water_df_raw"]
gas_raw_dataframe = BASE_DIR / config["paths"]["gas_df_raw"]

elec_df.to_csv(elec_raw_dataframe, index=False)
water_df.to_csv(water_raw_dataframe, index=False)
gas_df.to_csv(gas_raw_dataframe, index=False)


# Step 5: Preprocess (Align columns)
    # Step 5.1: Rename existing columns to match
elec_df_silver = elec_df.copy()
water_df_silver = water_df.copy()
gas_df_silver = gas_df.copy()


final_labels = config["columns"]["final_labels"]

elec_rename = config["columns"]["elec_rename"]
water_rename = config["columns"]["water_rename"]
gas_rename = config["columns"]["gas_rename"]

elec_df_silver.columns = elec_rename
water_df_silver.columns = water_rename
gas_df_silver.columns = gas_rename


    # Step 5.2: Add in missing columns to reach a standard df structure
elec_df_silver = standardize_column_names(elec_df_silver, final_labels)
water_df_silver = standardize_column_names(water_df_silver, final_labels)
gas_df_silver = standardize_column_names(gas_df_silver, final_labels)



    # Step 5.3: Pre-process missing values from df (incl. invoice_date/step_date/step_number)
elec_df_silver = fill_electricity_step_fields(elec_df_silver)
water_df_silver = fill_water_step_dates(water_df_silver)
water_df_silver = fill_missing_service_columns_for_water(water_df_silver)
gas_df_silver = fill_gas_invoice_start_end(gas_df_silver)
gas_df_silver = clean_gas_season(gas_df_silver)


    # Step 5.4 Standardize column datatypes
dtypes = config["column_dtypes"]

elec_df_silver = standardize_column_datatypes(elec_df_silver, dtypes, "elec")
water_df_silver = standardize_column_datatypes(water_df_silver, dtypes, "water")
gas_df_silver = standardize_column_datatypes(gas_df_silver, dtypes, "gas")

    # Step 5.5 Add in season to elec, water
summer_months = config["seasons"]["summer"]  # e.g., {"start_month": 10, "end_month": 5}

elec_df_silver["season"] = elec_df_silver["invoice_start"].apply(
    lambda x: classify_season(x, summer_months)
)

water_df_silver["season"] = water_df_silver["invoice_start"].apply(
    lambda x: classify_season(x, summer_months)
)


    # Step 5.6: Output as silver dataframe
elec_silver_output_path = BASE_DIR / config["paths"]["elec_silver_output_path"]
water_silver_output_path = BASE_DIR / config["paths"]["water_silver_output_path"]
gas_silver_output_path = BASE_DIR / config["paths"]["gas_silver_output_path"]


elec_df_silver.to_csv(elec_silver_output_path, index=False)
water_df_silver.to_csv(water_silver_output_path, index=False)
gas_df_silver.to_csv(gas_silver_output_path, index=False)

