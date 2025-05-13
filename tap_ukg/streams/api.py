import csv
import httpx
import json
from io import BytesIO
import logging
import math
import pandas as pd


V1_URL = "https://secure60.saashr.com/ta/rest/v1/"
BASE_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
}


def get_auth_token(api_key, username, password, company):
    auth = httpx.post(
        url=V1_URL + "login",
        headers=BASE_HEADERS | {"Api-Key": api_key},
        json={
            "credentials": {
                "username": username,
                "password": password,
                "company": company,
            }
        },
    )
    return auth.json()["token"]


def sanitize_null_values(record):
    """Recursively sanitize invalid float values in a record."""
    if isinstance(record, dict):
        return {k: sanitize_null_values(v) for k, v in record.items()}
    elif isinstance(record, list):
        return [sanitize_null_values(v) for v in record]
    elif isinstance(record, float):
        if math.isnan(record) or math.isinf(record):
            return None  # Replace invalid floats with None
    return record


def csv_to_clean_json(content):
    csv_data = BytesIO(content)
    df = pd.read_csv(csv_data, skip_blank_lines=True)

    # Drop 'Unnamed: 0' if it exists
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    # Drop rows that do not contain required fields, but only if those fields exist
    required_columns = ['Counter Date', 'Employee Id']
    existing_required_columns = [col for col in required_columns if col in df.columns]

    if existing_required_columns:
        df = df.dropna(subset=existing_required_columns)

    # Replace NaN with None (for valid JSON output)
    df = df.where(pd.notnull(df), None)

    # Convert DataFrame to list of dictionaries for JSON serialization
    data_dict = df.to_dict(orient='records')

    # Sanitize the JSON data
    json_data = [sanitize_null_values(record) for record in data_dict]

    return json_data


def get_saved_report(saved_report_id, api_key, username, password, company):
    token = get_auth_token(api_key, username, password, company)
    resp = httpx.get(
        url=V1_URL + f"report/saved/{saved_report_id}?company={company}",
        headers=BASE_HEADERS | {"Authorization": "Bearer " + token},
        timeout=60,
    )

    if resp.status_code == 200:
        data = csv_to_clean_json(resp.content)
        return data

    else:
        logging.error(f"Failed to retrieve report. Status code: {resp.status_code}")
        return None


def post_global_report(report_id, request_body, api_key, username, password, company):
    token = get_auth_token(api_key, username, password, company)
    resp = httpx.post(
        url=V1_URL + f"report/global/{report_id}",
        headers=BASE_HEADERS | {"Authorization": "Bearer " + token},
        json=request_body,
        timeout=60,
    )

    if resp.status_code == 200:
        data = csv_to_clean_json(resp.content)
        return data

    else:
        logging.error(f"Failed to retrieve report. Status code: {resp.status_code}")
        return None
