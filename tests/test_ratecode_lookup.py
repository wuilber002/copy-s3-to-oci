from tools.lookup_aws_rate_codes import metric_name


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
