import json

from dustgoggles.structures import dig_for_value

from killscreen.aws.utilities import init_client, clarify_region


# TODO: we'll want to cache these, but it's somewhat challenging to do so
#  with PSL objects because we need to be able to pass clients / sessions to
#  them, and we want to get the same answers for instance_type and region
#  even if we pass different clients and sessions. cachetools
#  (https://github.com/tkem/cachetools) is an option, as is something
#  handrolled.

def get_on_demand_price(instance_type, region=None, client=None, session=None):
    """
    fetch on-dmeand pricing information for a specific instance type.
    """
    client = init_client("pricing", client, session)
    region = clarify_region(region, client)
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
    region = clarify_region(region, client)
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

