import httpx
import logging

import singer

from tap_ukg.streams.api import get_saved_report


SAVED_REPORT_ID = 896338


def stream(company, token):
    """Stream data from tap source"""
    data = get_saved_report(SAVED_REPORT_ID, company, token)

    if data:
        # Write the records to the stream
        for record in data:
            singer.write_record(
                "ukg_accrual_detail_saved_report",
                {
                    "employee_id": record.get("Employee Id"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "last_first_name": record.get("Last, First Name"),
                    "pay_group": record.get("Pay Group(1)"),
                    "counter_date": record.get("Counter Date"),
                    "counter_hours": record.get("Counter Hours"),
                    "counter_quantity": record.get("Counter\u00a0Quantity"),
                    "counter_name": record.get("Counter Name"),
                    "counter_description": record.get("Counter Description"),
                    "counter_code": record.get("Counter Code"),
                    "cost_center_full_path": record.get(" Cost Center Full Path"),
                    "teams_full_path": record.get(" Teams Full Path"),
                    "job_full_path": record.get(" Job Full Path"),
                    "time_off_name": record.get("Time Off Name"),
                    "primary_email": record.get("Primary Email"),
                    "accrual_profile": record.get("Accrual Profile")
                }
            )
    else:
        logging.error(f"No data retrieved from the report {SAVED_REPORT_ID}.")
