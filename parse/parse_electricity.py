import re
import os

def parse_electricity_pdf(full_text, file_path):
    """
    Parses PDF text to extract invoice date, invoice total, and table data.
    
    Returns:
        table_data: list of dicts (one dict per usage/service block),
                    each including invoice_date and invoice_total
    """
    # --- Define regex patterns ---
    Invoice_Date_re = r"issuedate\s*([0-9]{1,2}[A-Z]{3}[0-9]{2})"
    Period_start_end_re = r"YourPlanSingleRate\s*From(\d{2}\w+\d{4})to(\d{2}\w+\d{4})"
    Invoice_Total_re = r"ElectricityCharges \$([\d.]+)"
    usage_re = r"Total\s*Anytime\s*(\d+)\s*\$([\d.]+)\s*\$([\d.]+)"  # usage kWh, rate, cost
    service_re = r"Service\s*to\s*Property\s*Charge\s*(\d+)\s*days\s*\$([\d.]+)\s*/day\s*\$([\d.]+)"  # days, rate/day, charge

    # --- Extract single-value fields ---
    invoice_date_match = re.search(Invoice_Date_re, full_text, re.IGNORECASE)
    invoice_date = invoice_date_match.group(1) if invoice_date_match else None

    invoice_total_match = re.search(Invoice_Total_re, full_text, re.IGNORECASE)
    invoice_total = invoice_total_match.group(1) if invoice_total_match else None

    # --- Extract multi-value fields (tables) ---
    usage_matches = re.findall(usage_re, full_text)
    service_matches = re.findall(service_re, full_text)
    period_matches = re.findall(Period_start_end_re, full_text)

    # --- Combine into structured data with invoice info ---
    file_name = os.path.basename(file_path)
    table_data = []
    for i, (usage, service) in enumerate(zip(usage_matches, service_matches)):
        usage_kwh, rate_per_kwh, usage_charge = usage
        service_days, service_rate, service_charge = service

        # Assign period for each table if available
        period_start, period_end = period_matches[i] if i < len(period_matches) else (None, None)

        table_data.append({
            "invoice_number": os.path.splitext(file_name)[0],
            "utility_type": os.path.normpath(file_path).split(os.sep)[-2],            
            "invoice_date": invoice_date,
            "invoice_total": invoice_total,
            "period_start": period_start,
            "period_end": period_end,
            "usage_kwh": usage_kwh,
            "rate_per_kwh": rate_per_kwh,
            "usage_charge": usage_charge,
            "service_days": service_days,
            "service_rate_per_day": service_rate,
            "service_charge": service_charge
        })

    return table_data

