data_pipeline_utility_bill/
|
├── config/
│   ├── config.yaml              # Main configuration (gitignored)
│   ├── config.example.yaml      # Example configuration template
│   ├── credentials.json         # Gmail OAuth credentials (gitignored)
│   └── token.json               # Gmail OAuth tokens (gitignored)
│
├── data/                        # Data storage (files gitignored, structure preserved)
│   ├── raw/                     # Raw PDF files from email downloads
│   │   ├── elec/                # Electricity bill PDFs
│   │   ├── gas/                 # Gas bill PDFs
│   │   └── water/               # Water bill PDFs
│   ├── raw_csv (bronze)/        # Raw CSV extractions from PDFs
│   │   ├── elec_raw.csv
│   │   ├── gas_raw.csv
│   │   └── water_raw.csv
│   ├── silver/                  # Processed/standardized data
│   │   ├── elec (silver).csv
│   │   ├── gas (silver).csv
│   │   └── water (silver).csv
│   └── gold/                    # Final combined dataset
│       └── utilities (gold).csv
│
├── extract/
│   ├── gmail_connector.py       # Handles Gmail OAuth + message fetching
│   ├── email_filter.py          # Logic to filter relevant emails
│   └── pdf_downloader.py        # Downloads PDFs from Gmail attachments
│
├── parse/
│   ├── pdf_parser_base.py       # Common PDF parsing logic
│   ├── parse_electricity.py     # Regex & parsing logic for electricity bills
│   ├── parse_water.py           # Regex & parsing logic for water bills
│   └── parse_gas.py             # Regex & parsing logic for gas bills
│
├── transform/
│   ├── standardize_df_cols.py   # Standardize column names and data types
│   └── data_preprocess.py       # Handle missing values, date processing, seasons
│
├── load/
│   └── save_load.py             # Saves DataFrames to CSV files
│
├── main.py                      # Orchestrates the full ETL pipeline
└── code_structure.md            # This file

