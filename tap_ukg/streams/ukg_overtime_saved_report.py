import httpx
import logging

import singer

from tap_ukg.streams.api import get_saved_report


SAVED_REPORT_ID = 879114


def stream(api_key, company, username, password):
    """Stream data from tap source"""
    data = get_saved_report(SAVED_REPORT_ID, api_key, company, username, password)

    if data:
        # Write the records to the stream
        for record in data:
            singer.write_record(
                "ukg_overtime_saved_report",
                {
                    "counter_date": record.get("Counter Date"),
                    "location": record.get("Location(1)"),
                    "employee_id": record.get("Employee Id"),
                    "last_first_name": record.get("Last, First Name"),
                    "counter_name": record.get("Counter Name"),
                    "counter_hours": record.get("Counter Hours"),
                    "pay_group": record.get("Pay Group(1)"),
                    "overtime_pay": record.get("Overtim Pay ($)"),
                }
            )
    else:
        logging.error(f"No data retrieved from the report {SAVED_REPORT_ID}.")
