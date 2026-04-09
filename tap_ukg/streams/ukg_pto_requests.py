import logging
from datetime import datetime

import singer

from tap_ukg.streams.api import post_global_report


GLOBAL_REPORT_ID = "REPORT_PTO_REQUEST_ALL"


def stream(company, token):
    """Stream all PTO requests"""
    request_body = {
        "company": {
            "short_name": company
        },
        "selectors": []
    }

    data = post_global_report(GLOBAL_REPORT_ID, request_body, token)

    if data:
        for record in data:
            date_requested_str = record.get("Date Requested")

            try:
                total_hours = float(record.get("Time"))
            except (ValueError, TypeError):
                total_hours = None

            singer.write_record(
                "ukg_pto_requests",
                {
                    "employee_id": record.get("Employee Id"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "company_code": record.get("Company Code"),
                    "date_requested": date_requested_str,
                    "last_date_requested": record.get("Last Date Requested"),
                    "total_hours": total_hours,
                    "time_off_type": record.get("Time Off"),
                    "request_state": record.get("Request State"),
                    "workflow_status": record.get("Workflow Status"),
                    "date_request_submitted": record.get("Date Request Submitted"),
                }
            )
    else:
        logging.error(f"No data retrieved from the report {GLOBAL_REPORT_ID}.")
