#!/usr/bin/env python3
"""Look up AWS public Price List details for one or more RateCodes.

Examples:
  python3 tools/lookup_aws_rate_codes.py --region sa-east-1 \
    QKTPK3975YUWDU3Q.JRTCKXETXF.Q3Z75P77EN \
    ZNKV32W48WWCENCC.JRTCKXETXF.6YS6EN2CT7

  python3 tools/lookup_aws_rate_codes.py --region us-east-1 \
    HQEH3ZWJVT46JHRG.JRTCKXETXF.Q3Z75P77EN

The script does not use AWS credentials and does not access the customer
account. A RateCode is a Price List rate-dimension identifier and is regional,
so pass the AWS Region where the billed S3 bucket resides.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from urllib.request import Request, urlopen


CATALOGS = {
    "AmazonS3": "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/{region}/index.json",
    "AWSDataTransfer": "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/{region}/index.json",
}


def fetch_catalog(url: str) -> dict:
    request = Request(url, headers={"User-Agent": "raijin-ratecode-lookup/1.0"})
    with urlopen(request, timeout=90) as response:
        body = response.read(96 * 1024 * 1024 + 1)
    if len(body) > 96 * 1024 * 1024:
        raise RuntimeError("AWS public Price List catalog exceeded the 96 MiB safety limit")
    return json.loads(body.decode("utf-8"))


def metric_name(service: str, attributes: dict, dimension: dict) -> str:
    """Return the Raijin-facing metric name for a public Price List entry."""
    usage = str(attributes.get("usagetype", ""))
    operation = str(attributes.get("operation", ""))
    fee = str(attributes.get("feeDescription", ""))
    group = str(attributes.get("group", ""))
    description = str(dimension.get("description", ""))

    if service == "AWSDataTransfer" and attributes.get("transferType") == "AWS Outbound" and attributes.get("toLocation") == "External":
        first_tier = "first" in description.lower() or "up to 10 tb" in description.lower()
        return "Data Transfer OUT to Internet — first tier" if first_tier else "Data Transfer OUT to Internet"
    if operation == "DeepArchiveRestoreObject" and "bulk" in usage.lower():
        return "S3 Glacier Deep Archive BULK data retrieval"
    if operation == "DeepArchiveRestoreObject" and "standard" in usage.lower():
        return "S3 Glacier Deep Archive STANDARD data retrieval"
    if "batch operations jobs" in fee.lower():
        return "S3 Batch Operations job charge"
    if "batch operations" in fee.lower() and "object" in fee.lower():
        return "S3 Batch Operations object charge"
    if group == "S3-API-Tier1":
        return "S3 Standard PUT/COPY/POST/LIST requests"
    if group == "S3-API-Tier2":
        return "S3 Standard GET/SELECT and other requests"
    if attributes.get("storageClass") == "General Purpose" and usage.endswith("TimedStorage-ByteHrs"):
        return "S3 Standard storage — first tier" if "first" in description.lower() else "S3 Standard storage"
    return fee or attributes.get("groupDescription") or usage or "AWS public Price List metric"


def find_rate_codes(catalog: dict, wanted: set[str], service: str) -> dict[str, dict]:
    found: dict[str, dict] = {}
    terms = (catalog.get("terms") or {}).get("OnDemand") or {}
    for product in (catalog.get("products") or {}).values():
        sku = product.get("sku")
        if not sku or not any(rate_code.startswith(f"{sku}.") for rate_code in wanted):
            continue
        for term in terms.get(sku, {}).values():
            for dimension in term.get("priceDimensions", {}).values():
                rate_code = dimension.get("rateCode")
                if rate_code not in wanted:
                    continue
                attributes = product.get("attributes") or {}
                found[rate_code] = {
                    "service": service,
                    "metric": metric_name(service, attributes, dimension),
                    "rate_code": rate_code,
                    "price_usd": (dimension.get("pricePerUnit") or {}).get("USD"),
                    "unit": dimension.get("unit"),
                    "description": dimension.get("description"),
                    "usage_type": attributes.get("usagetype"),
                    "operation": attributes.get("operation") or None,
                    "sku": sku,
                }
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--region", required=True, help="AWS Region of the billed resource, for example sa-east-1")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true", help="Emit a JSON array instead of readable text")
    output.add_argument("--csv-price", action="store_true", help="Emit only price_usd as one CSV column, without a header")
    parser.add_argument("rate_codes", nargs="+", metavar="RATE_CODE", help="One or more complete Price List RateCodes")
    args = parser.parse_args()

    wanted = set(args.rate_codes)
    found: dict[str, dict] = {}
    try:
        for service, template in CATALOGS.items():
            found.update(find_rate_codes(fetch_catalog(template.format(region=args.region)), wanted, service))
    except Exception as error:
        print(f"AWS public Price List lookup failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 2

    results = []
    for rate_code in args.rate_codes:
        value = found.get(rate_code)
        if value is None:
            value = {"rate_code": rate_code, "found": False, "region": args.region}
        else:
            value = {"found": True, "region": args.region, **value}
        results.append(value)

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    elif args.csv_price:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        for result in results:
            writer.writerow([result.get("price_usd", "")])
    else:
        for result in results:
            print(f"RateCode: {result['rate_code']}")
            if not result["found"]:
                print(f"  Status: not found in AmazonS3/AWSDataTransfer catalogs for {args.region}")
            else:
                print(f"  Metric: {result['metric']}")
                print(f"  Rate: USD {result['price_usd']} per {result['unit']}")
                print(f"  Service: {result['service']}")
                print(f"  Usage type: {result['usage_type']}")
                print(f"  Operation: {result['operation'] or '—'}")
                print(f"  Description: {result['description']}")
            print()
    return 0 if all(result["found"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
