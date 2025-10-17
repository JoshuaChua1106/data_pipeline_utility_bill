# Utility Bill Data Pipeline

An automated ETL pipeline that extracts utility bill data from Gmail attachments, parses PDF invoices, and transforms the data into a structured format for analysis.

## Overview

This pipeline automates the process of:
1. **Extracting** utility bills (electricity, gas, water) from Gmail attachments
2. **Parsing** PDF invoices using regex patterns 
3. **Transforming** data into standardized formats with proper data types
4. **Loading** the final dataset into CSV files for analysis

## Features

- 🔐 **Secure Gmail OAuth integration** - No passwords stored
- 📄 **Multi-utility support** - Handles electricity, gas, and water bills
- 🏗️ **Data lake architecture** - Bronze/Silver/Gold data layers
- 🔄 **Automated data processing** - Handles missing values, date parsing, seasonal classification
- 📊 **Standardized output** - Consistent column names and data types across utilities
- ⚙️ **Modular pipeline** - Run individual stages or full pipeline with CLI commands
- 🛠️ **Error handling** - Graceful failure handling with clear error messages

## Quick Start

### Prerequisites

- Python 3.8+
- Gmail account with API access enabled
- Google Cloud Console project with Gmail API enabled

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data_pipeline_utility_bill
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up configuration**
   ```bash
   cp config/config.example.yaml config/config.yaml
   ```
   Edit `config/config.yaml` with your utility provider email addresses.

4. **Set up Gmail OAuth**
   - Download OAuth credentials from Google Cloud Console
   - Save as `config/credentials.json`
   - Set environment variables:
     ```bash
     export GMAIL_CREDENTIALS_PATH="config/credentials.json"
     export GMAIL_TOKEN_PATH="config/token.json"
     ```

5. **Run the pipeline**
   ```bash
   # Run full pipeline
   python main.py
   
   # Or run specific stages
   python main.py --stage extract     # Download PDFs only
   python main.py --stage parse       # Parse PDFs to CSV  
   python main.py --stage transform   # Process to silver layer
   python main.py --stage load        # Combine to gold layer
   ```

## Project Structure

```
data_pipeline_utility_bill/
├── config/                      # Configuration files
│   ├── config.yaml              # Main configuration (gitignored)
│   ├── config.example.yaml      # Example configuration template
│   ├── credentials.json         # Gmail OAuth credentials (gitignored)
│   └── token.json               # Gmail OAuth tokens (gitignored)
├── data/                        # Data storage (files gitignored)
│   ├── raw/                     # Raw PDF files from email downloads
│   ├── raw_csv (bronze)/        # Raw CSV extractions from PDFs
│   ├── silver/                  # Processed/standardized data
│   └── gold/                    # Final combined dataset
├── extract/                     # Data extraction modules
├── parse/                       # PDF parsing modules
├── transform/                   # Data transformation modules
├── load/                        # Data loading modules
└── main.py                      # Main pipeline orchestrator
```

## Configuration

### Gmail Queries

Configure email search queries in `config/config.yaml`:

```yaml
gmail_queries:
  elec: "from:your-electricity-provider@example.com subject:electricity has:attachment"
  water: "from:your-water-provider@example.com subject:water has:attachment"  
  gas: "from:your-gas-provider@example.com subject:gas has:attachment"
```

### Data Processing

The pipeline uses a medallion architecture:

- **Bronze (Raw)**: Direct PDF extractions with minimal processing
- **Silver (Processed)**: Standardized column names, data types, missing value handling
- **Gold (Curated)**: Combined dataset ready for analysis

## Modular Usage

The pipeline is designed with modular stages that can be run independently:

```bash
# Full pipeline (all stages)
python main.py --stage all

# Individual stages
python main.py --stage extract     # Gmail connection + PDF download
python main.py --stage parse       # PDF parsing to CSV (Bronze layer)
python main.py --stage transform   # Data standardization (Silver layer)  
python main.py --stage load        # Data combination (Gold layer)
```

**Benefits of modular approach:**
- **Debug easily** - Isolate issues to specific stages
- **Reprocess data** - Re-run transform/load without re-downloading
- **Development** - Test individual components
- **Flexibility** - Skip stages based on data availability

### Seasonal Classification

Automatically classifies usage periods into seasons:

```yaml
seasons:
  summer:
    start_month: 11  # November
    end_month: 4     # End of April
  winter:
    start_month: 5   # Start of May
    end_month: 10    # End of October
```

## Data Schema

The final dataset includes standardized columns:

| Column | Type | Description |
|--------|------|-------------|
| `invoice_number` | string | Unique invoice identifier |
| `utility_type` | string | Type of utility (elec/gas/water) |
| `invoice_date` | datetime | Invoice issue date |
| `invoice_total` | float | Total amount charged |
| `invoice_start` | datetime | Billing period start |
| `invoice_end` | datetime | Billing period end |
| `usage_amount` | float | Amount of utility consumed |
| `usage_rate` | float | Rate per unit |
| `usage_charge` | float | Total usage charges |
| `service_charge` | float | Fixed service charges |
| `season` | string | Season classification |

## Security

- **OAuth Authentication**: Uses secure OAuth flow, no passwords stored
- **Credential Protection**: Sensitive files are gitignored
- **Local Processing**: All data processing happens locally
- **Read-Only Access**: Gmail API used with read-only scope

## Extending the Pipeline

### Adding New Utility Providers

1. Add email query to `config/config.yaml`
2. Create parser function in `parse/` directory
3. Update `parse/pdf_parser_base.py` to include new utility type
4. Add column mapping in config if needed

### Custom Data Transformations

Add custom transformation functions in `transform/data_preprocess.py`:

```python
def custom_transformation(df):
    # Your transformation logic
    return df
```

## Troubleshooting

### Common Issues

1. **Gmail API Quota Exceeded**
   - Check your API usage in Google Cloud Console
   - Implement rate limiting if needed

2. **PDF Parsing Errors**
   - Check PDF format compatibility
   - Update regex patterns in parse modules

3. **Authentication Issues**
   - Verify OAuth credentials are valid
   - Check environment variables are set correctly

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Acknowledgments

- Built with Python and pandas for data processing
- Uses pdfplumber for PDF text extraction
- Gmail API integration for automated email processing
