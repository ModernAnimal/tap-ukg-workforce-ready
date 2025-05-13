import httpx
import logging

import singer

from tap_ukg.streams.api import get_saved_report


SAVED_REPORT_ID = 873720


def stream(api_key, company, username, password):
    """Stream data from tap source"""
    data = get_saved_report(SAVED_REPORT_ID, api_key, company, username, password)

    if data:
        # Write the records to the stream
        for record in data:
            singer.write_record(
                "ukg_employee_roster_saved_report",
                {
                    "employee_id": record.get("Employee Id"),
                    "badge": record.get("Badge"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "employee_status": record.get("Employee Status"),
                    "dvm_number_of_weekly_shifts": record.get("DVM NUMBER OF WEEKLY SHIFTS"),
                    "company_code": record.get("Company Code"),
                    "primary_email": record.get("Primary Email"),
                    "holiday_profile": record.get("Holiday Profile"),
                    "pay_period_profile": record.get("Pay Period Profile"),
                    "accrual_profile": record.get("Accrual Profile"),
                    "cost_center": record.get("Cost Center(1)"),
                    "dvm_pto_industry_experience_date": record.get("DVM PTO INDUSTRY EXPERIENCE DATE"),
                    "relief_shift_base_rate": record.get("Relief Shift Base Rate"),
                }
            )
    else:
        logging.error(f"No data retrieved from the report {SAVED_REPORT_ID}.")
