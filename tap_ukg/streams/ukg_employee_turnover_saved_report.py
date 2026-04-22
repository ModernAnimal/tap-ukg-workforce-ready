import httpx
import logging

import singer

from tap_ukg.streams.api import get_saved_report


SAVED_REPORT_ID = 1007754058


def stream(company, token):
    """Stream data from tap source"""
    data = get_saved_report(SAVED_REPORT_ID, company, token)

    if data:
        # Write the records to the stream
        for record in data:
            singer.write_record(
                "ukg_employee_turnover_saved_report",
                {
                    "employee_id": record.get("Employee Id"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "employee_status": record.get("Employee Status"),
                    "date_terminated": record.get("Date Terminated"),
                    "date_hired": record.get("Date Hired"),
                    "cost_center": record.get("Cost Center(1)"),
                    "default_cost_center": record.get("Default Cost Center"),
                    "default_job": record.get("Default Job"),
                    "company_system_id": record.get("Company: System Id"),
                }
            )
    else:
        logging.error(f"No data retrieved from the report {SAVED_REPORT_ID}.")
