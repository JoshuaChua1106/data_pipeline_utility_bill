utilities_pipeline/
|
├── config/
│   ├── settings.py             # API keys, file paths, constants
│
├── extract/
│   ├── gmail_connector.py      # Handles Gmail OAuth + message fetching
│   ├── email_filter.py         # Logic to filter relevant emails
│   ├── pdf_downloader.py       # Downloads PDFs from Gmail attachments
│
├── parse/
│   ├── pdf_parser_base.py      # Common logic (shared pdfplumber helpers)
│   ├── parse_electricity.py    # Regex & parsing logic for electricity bills
│   ├── parse_water.py          # Regex & parsing logic for water bills
│   ├── parse_gas.py            # Regex & parsing logic for gas bills
│
├── transform/
│   ├── preprocess_datatypes.py # Change data types, rename columns
│   ├── preprocess_merge.py     # Combine the three dataframes
│   ├── preprocess_missing.py   # Handle missing values
│
├── load/
│   ├── load_to_csv.py          # Saves combined DataFrame to CSV
│   ├── load_to_postgres.py     # (Future) Direct upload to PostgreSQL
│
├── utils/
│   ├── db_connection.py        # PostgreSQL connection setup
│   ├── logging_utils.py        # Custom logger for pipeline runs
│   ├── file_utils.py           # File path helpers, etc.
│
├── main.py                     # Orchestrates the full pipeline
└── requirements.txt

