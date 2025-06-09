import httpx
import logging

import singer

from tap_ukg.streams.api import post_global_report


GLOBAL_REPORT_ID = "REPORT_CALCULATED_TIME_TA_COUNTER_DETAILED"


def stream(company, token):
    """Stream data from tap source"""
    request_body = {
        "company": {
            "short_name": company
        },
        "selectors": [
            {
                "name": "TACounterRecordDate",
                "parameters": {
                    "RangeType": "2",
                    "FromDate": "2025-01-01",
                    "ToDate": "2025-03-31"
                }
            }
        ]
    }
    data = post_global_report(
        GLOBAL_REPORT_ID,
        request_body,
        token
    )

    if data:
        # Write the records to the stream
        for record in data:
            singer.write_record(
                "ukg_timesheet_global_report",
                {
                    "employee_id": record.get("Employee Id"),
                    "first_name": record.get("First Name"),
                    "last_name": record.get("Last Name"),
                    "counter_date": record.get("Counter Date"),
                    "counter_hours": record.get("Counter Hours"),
                    "counter_quantity": record.get("Counter\u00a0Quantity"),
                    "counter_name": record.get("Counter Name"),
                    "counter_description": record.get("Counter Description"),
                    "counter_code": record.get("Counter Code"),
                    "cost_center_full_path": record.get(" Cost Center Full Path"),
                    "teams_full_path": record.get(" Teams Full Path"),
                    "job_full_path": record.get(" Job Full Path"),
                    "time_off_name": record.get("Time Off Name")
                }
            )
    else:
        logging.error(f"No data retrieved from the report {GLOBAL_REPORT_ID}.")
