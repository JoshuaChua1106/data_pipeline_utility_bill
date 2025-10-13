import re
import os

def parse_water_pdf(full_text, file_path):
    """
    Parses PDF text to extract invoice date, invoice total, and table data.

    Returns:
        table_data: list of dicts (one dict per usage/service block),
                    each including invoice_date, invoice_total, and step-level periods
    """
    # --- Define regex patterns ---
    Invoice_Date_re = r"Issuedate\s*(\d{1,2}[A-Za-z]{3}\d{4})"
    Period_start_end_re = r"From(\d{1,2}[A-Za-z]{3}\d{4})-(\d{1,2}[A-Za-z]{3}\d{4})"
    Invoice_Total_re = r"Totalusagecharges \$([\d.]+)"
    usage_re = r"(?i)STEP(\d+).*?([\d.]+)kL\s*x\s*\$([\d.]+)\s*=\s*\$([\d.]+)"  # step number, usage_kL, price, total
    sub_period_re = r"(\d{2}/\d{2}/\d{4})-(\d{2}/\d{2}/\d{4})"

    # --- Extract single-value fields ---
    invoice_date_match = re.search(Invoice_Date_re, full_text, re.IGNORECASE)
    invoice_date = invoice_date_match.group(1) if invoice_date_match else None
    
    invoice_total_match = re.search(Invoice_Total_re, full_text, re.IGNORECASE)
    invoice_total = invoice_total_match.group(1) if invoice_total_match else None

    # --- Extract periods ---
    invoice_period_matches = re.findall(Period_start_end_re, full_text)
    
    # --- Extract steps ---
    # Split text by sub-periods so each step can be assigned a sub-period
    blocks = re.split(sub_period_re, full_text)
    table_data = []
    file_name = os.path.basename(file_path)
    utility_type = os.path.normpath(file_path).split(os.sep)[-2]

    if len(blocks) == 1:
        # No sub-periods found, treat all steps as belonging to invoice period
        usage_matches = re.findall(usage_re, full_text)
        for usage in usage_matches:
            step_number, usage_kL, price_per_kL, usage_cost = usage
            period_start, period_end = (invoice_period_matches[0] if invoice_period_matches else (None, None))
            table_data.append({
                "invoice_number": os.path.splitext(file_name)[0],
                "utility_type": utility_type,            
                "invoice_date": invoice_date,
                "invoice_total": invoice_total,
                "invoice_period_start": period_start,
                "invoice_period_end": period_end,
                "step_period_start": "NULL",
                "step_period_end": "NULL",
                "step_number": step_number,
                "usage_kL": usage_kL,
                "Price $/kL": price_per_kL,
                "usage_cost": usage_cost,
            })
    else:
        # Sub-periods exist, assign each step to its corresponding sub-period
        # re.split returns ['', start1, end1, block1, start2, end2, block2, ...]
        for i in range(1, len(blocks), 3):
            step_period_start = blocks[i]
            step_period_end = blocks[i+1]
            block_text = blocks[i+2]

            usage_matches = re.findall(usage_re, block_text)
            for usage in usage_matches:
                step_number, usage_kL, price_per_kL, usage_cost = usage
                # If invoice_period exists, use first one
                period_start, period_end = (invoice_period_matches[0] if invoice_period_matches else (None, None))
                table_data.append({
                    "invoice_number": os.path.splitext(file_name)[0],
                    "utility_type": utility_type,            
                    "invoice_date": invoice_date,
                    "invoice_total": invoice_total,
                    "invoice_period_start": period_start,
                    "invoice_period_end": period_end,
                    "step_period_start": step_period_start,
                    "step_period_end": step_period_end,
                    "step_number": step_number,
                    "usage_kL": usage_kL,
                    "Price $/kL": price_per_kL,
                    "usage_cost": usage_cost,
                })

    return table_data


