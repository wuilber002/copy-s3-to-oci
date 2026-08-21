import subprocess
import sys

from tools.lookup_aws_rate_codes import csv_price_row, metric_name, parse_output_units, spreadsheet_price


def test_ratecode_lookup_labels_raijin_metrics_from_public_attributes():
    assert metric_name("AmazonS3", {
        "usagetype": "SAE1-Bulk-Retrieval-Bytes", "operation": "DeepArchiveRestoreObject"
    }, {}) == "S3 Glacier Deep Archive BULK data retrieval"
    assert metric_name("AmazonS3", {
        "group": "S3-API-Tier1"
    }, {}) == "S3 Standard PUT/COPY/POST/LIST requests"
    assert metric_name("AWSDataTransfer", {
        "transferType": "AWS Outbound", "toLocation": "External"
    }, {"description": "$0.150 per GB - up to 10 TB / month data transfer out"}) == "Data Transfer OUT to Internet — first tier"


def test_ratecode_lookup_help_documents_csv_price_mode():
    result = subprocess.run(
        [sys.executable, "tools/lookup_aws_rate_codes.py", "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--csv-price" in result.stdout
    assert "parse_intermixed_args" in open("tools/lookup_aws_rate_codes.py", encoding="utf-8").read()


def test_csv_price_shape_is_one_row_in_input_order():
    assert csv_price_row([
        {"rate_code": "third", "price_usd": "0.1500000000"},
        {"rate_code": "first", "price_usd": "0.0080000000"},
        {"rate_code": "missing"},
    ], {"third": "per_1000"}) == ["150,0000000000", "0,0080000000", ""]


def test_spreadsheet_price_scales_only_when_the_ratecode_mapping_requests_it():
    item = {"price_usd": "0.0000070000", "unit": "Requests"}
    assert spreadsheet_price(item) == "0.0000070000"
    assert spreadsheet_price(item, "per_1000") == "0.0070000000"


def test_parse_output_units_is_explicit_and_ratecode_based():
    assert parse_output_units(["abc=per_1000", "def=source"]) == {"abc": "per_1000", "def": "source"}
