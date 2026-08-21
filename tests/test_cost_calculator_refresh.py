import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "refresh_cost_calculator_rates", Path("tools/refresh_cost_calculator_rates.py")
)
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)


def test_workbook_rate_mapper_uses_raijin_units_and_supported_dimensions():
    def add(catalog, sku, group, price):
        catalog["products"][sku] = {"sku": sku, "attributes": {"group": group}}
        catalog["terms"]["OnDemand"][sku] = {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": str(price)}}}}}

    catalog = {"products": {}, "terms": {"OnDemand": {}}}
    add(catalog, "deep", "S3-GlacierDeepArchive-Retrieval-Bulk", 0.0025)
    add(catalog, "list", "S3-API-Tier1", 0.005)
    rates = refresh.s3_rates(catalog)
    assert rates["put_list_per_1000"] == 5
    assert rates["deep_bulk_per_gib"] > 0.0025


def test_workbook_rate_mapper_avoids_infrequent_access_and_manifest_generation_prices():
    def add(catalog, sku, attributes, price):
        catalog["products"][sku] = {"sku": sku, "attributes": attributes}
        catalog["terms"]["OnDemand"][sku] = {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": str(price)}}}}}

    catalog = {"products": {}, "terms": {"OnDemand": {}}}
    add(catalog, "infrequent", {"storageClass": "Infrequent Access", "usagetype": "TimedStorage-SIA-ByteHrs"}, 0.0125)
    add(catalog, "standard", {"storageClass": "General Purpose", "usagetype": "TimedStorage-ByteHrs"}, 0.023)
    add(catalog, "manifest", {"feeDescription": "Per object fee to generate Batch Operations Manifest"}, 0.000000015)
    add(catalog, "object-operation", {"feeDescription": "Per object fee for object operations performed by Batch Operations"}, 0.000001)
    rates = refresh.s3_rates(catalog)
    assert rates["temporary_standard_per_gib_month"] > 0.023
    assert rates["batch_object_per_object"] == 0.000001


def test_workbook_transfer_mapper_selects_only_aws_outbound_external():
    catalog = {
        "products": {"external": {"sku": "external", "attributes": {"toLocation": "External", "transferType": "AWS Outbound", "fromLocation": "South America (Sao Paulo)"}}},
        "terms": {"OnDemand": {"external": {"term": {"priceDimensions": {"rate": {"pricePerUnit": {"USD": "0.15"}}}}}}},
    }
    assert refresh.transfer_rate(catalog) > 0.15
