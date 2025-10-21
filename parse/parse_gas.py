import re
import os

def parse_gas_pdf(full_text, file_path):
    """
    Parses gas bill PDF text into structured table data.

    Returns:
        table_data: list of dicts (one row per Step)
    """
    # --- Define regex patterns ---
    Invoice_Date_re = r"IssueDate\s*(\d{1,2}[A-Za-z]{3}\d{2,4})"
    Invoice_Total_re = r"GasCharges \$([\d.]+)"
    Period_start_end_re = r"From(\d{1,2}[A-Za-z]+?\d{4})to(\d{1,2}[A-Za-z]+?\d{4})"
    Season_re = r"(TotalWinter|TotalSummer|TotalSpring|TotalAutumn|TotalFall)"
    step_re = r"Step(\d+)\s+([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)"  # Step number, usage_MJ, rate per MJ, usage_cost
    service_re = r"ServicetoPropertyCharge\s+(\d+)days\s+\$([\d.]+)\/day\s+\$([\d.]+)"  # service_days, rate/day, charge

    # --- Extract invoice info ---
    invoice_date_match = re.search(Invoice_Date_re, full_text, re.IGNORECASE)
    invoice_date = invoice_date_match.group(1) if invoice_date_match else None

    invoice_total_match = re.search(Invoice_Total_re, full_text, re.IGNORECASE)
    invoice_total = invoice_total_match.group(1) if invoice_total_match else None

    # --- Split text by periods ---
    blocks = re.split(Period_start_end_re, full_text)
    table_data = []

    file_name = os.path.basename(file_path)
    utility_type = os.path.normpath(file_path).split(os.sep)[-2]

    # Iterate through blocks containing period info
    for i in range(1, len(blocks), 3):
        period_start = blocks[i]
        period_end = blocks[i+1]
        block_text = blocks[i+2]

        # Extract season
        season_match = re.search(Season_re, block_text, re.IGNORECASE)
        season = season_match.group(1) if season_match else None

        # Extract steps
        steps = re.findall(step_re, block_text, re.IGNORECASE)

        # Extract service info (assume only one service line per period)
        service_match = re.search(service_re, block_text, re.IGNORECASE)
        service_days, service_rate, service_charge = (service_match.groups() if service_match else (None, None, None))

        for step in steps:
            step_number, usage_MJ, rate_per_MJ, usage_cost = step
            table_data.append({
                "invoice_number": os.path.splitext(file_name)[0],
                "utility_type": utility_type,
                "invoice_date": invoice_date,
                "invoice_total": invoice_total,
                "period_start": period_start,
                "period_end": period_end,
                "season": season,
                "step_number": step_number,
                "usage_MJ": usage_MJ,
                "Rate_per_MJ": rate_per_MJ,
                "usage_cost": usage_cost,
                "service_days": service_days,
                "service_rate_per_day": service_rate,
                "service_charge": service_charge
            })

    return table_data