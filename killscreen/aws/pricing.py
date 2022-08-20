import json

from dustgoggles.structures import dig_for_value

from killscreen.aws.utilities import init_client


def get_on_demand_price(instance_type, region=None, client=None, session=None):
    """
    fetch on-dmeand pricing information for a specific instance type.
    """
    client = init_client("pricing", client, session)
    if region is None:
        region = client._client_config.region_name
    product = client.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
            {
                "Type": "TERM_MATCH",
                "Field": "operatingSystem",
                "Value": "Linux",
            },
            {
                "Type": "TERM_MATCH",
                "Field": "usagetype",
                "Value": f"BoxUsage:{instance_type}",
            },
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
        ],
    )['PriceList'][0]
    return float(dig_for_value(json.loads(product), "USD"))


def get_cpu_credit_price(
    instance_type, region=None, client=None, session=None
):
    client = init_client("pricing", client, session)
    if region is None:
        # noinspection PyProtectedMember
        region = client._client_config.region_name
    product = client.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "operation",
                "Value": f"{instance_type.split('.')[0].upper()}CPUCredits",
            },
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
        ],
    )["PriceList"][0]
    return float(dig_for_value(json.loads(product), "USD"))
