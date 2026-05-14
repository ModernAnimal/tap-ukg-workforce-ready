import httpx
import logging

import singer

from tap_ukg.streams.api import get_saved_report


SAVED_REPORT_ID = 1007761450


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
                    "badge": record.get("Badge"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "in_payroll": record.get("In Payroll"),
                    "locked": record.get("Locked"),
                    "employee_status": record.get("Employee Status"),
                    "time_off": record.get("TimeOff"),
                    "transaction_type": record.get("Transaction Type"),
                    "hours_authorized": record.get("Hours Authorized"),
                    "hours_taken": record.get("Hours Taken"),
                    "range_from": record.get("Range From"),
                    "range_to": record.get("Range To"),
                }
            )
    else:
        logging.error(f"No data retrieved from the report {SAVED_REPORT_ID}.")
