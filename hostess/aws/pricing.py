import json
import pickle
from pathlib import Path

from dustgoggles.structures import dig_for_value

from hostess.aws.utilities import init_client, clarify_region, autopage
from hostess.config import GENERAL_DEFAULTS
from hostess.utilities import (
    check_cached_results, clear_cached_results, filestamp
)


def get_on_demand_price(instance_type, region=None, client=None, session=None):
    """
    fetch on-demand pricing information for a specific instance type.
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
    )["PriceList"][0]
    return float(dig_for_value(json.loads(product), "USD"))


def get_ec2_product_family_pricelists(
    family, region=None, client=None, session=None
):
    client = init_client("pricing", client, session)
    region = clarify_region(region, client)
    pricelist_strings = autopage(
        client,
        "get_products",
        "PriceList",
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
            {"Type": "TERM_MATCH", "Field": "productFamily", "Value": family},
        ],
    )
    return tuple(map(json.loads, pricelist_strings))


def get_ebs_iops_rates(region=None, client=None, session=None):
    return {
        pricelist["product"]["attributes"]["volumeApiName"].lower(): float(
            dig_for_value(pricelist, "USD")
        )
        for pricelist in get_ec2_product_family_pricelists(
            "System Operation", region, client, session
        )
    }


def get_ebs_throughput_rates(region=None, client=None, session=None):
    return {
        pricelist["product"]["attributes"]["volumeApiName"].lower(): float(
            dig_for_value(pricelist, "USD")
        )
        for pricelist in get_ec2_product_family_pricelists(
            "Provisioned Throughput", region, client, session
        )
    }


def get_ebs_storage_rates(region=None, client=None, session=None):
    return {
        pricelist["product"]["attributes"]["volumeApiName"].lower(): float(
            dig_for_value(pricelist, "USD")
        )
        for pricelist in get_ec2_product_family_pricelists(
            "Storage", region, client, session
        )
    }


def get_ebs_rates(region=None, client=None, session=None):
    client = init_client("pricing", client, session)
    region = clarify_region(region, client)
    iops, throughput, storage = (
        get_ebs_iops_rates(region, client),
        get_ebs_throughput_rates(region, client),
        get_ebs_storage_rates(region, client),
    )
    return [
        {
            "volume_type": k,
            "storage": v,
            "iops": iops.get(k),
            "throughput": throughput.get(k),
        }
        for k, v in storage.items()
    ]


def get_cpu_credit_rates(region=None, client=None, session=None):
    return [
        {
            "instance_family": pricelist["product"]["attributes"][
                "instance"
            ].lower(),
            "usd_per_cpu_credit": float(dig_for_value(pricelist, "USD")),
        }
        for pricelist in get_ec2_product_family_pricelists(
            "CpuCredits", region, client, session
        )
    ]


def unpack_on_demand_pricelist(pricelist):
    return {
        "instance_type": pricelist["product"]["attributes"]["instanceType"],
        "usd_per_hour": float(
            dig_for_value(
                pricelist["terms"]["OnDemand"], "USD"
            )
        ),
    }


def get_on_demand_rates(region=None, client=None, session=None):
    client = init_client("pricing", client, session)
    region = clarify_region(region, client)
    ondemand = autopage(
        client,
        "get_products",
        "PriceList",
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
                "Field": "productFamily",
                "Value": "Compute Instance",
            },
            {
                "Type": "TERM_MATCH",
                "Field": "capacitystatus",
                "Value": "Used",
            },
            {
                "Type": "TERM_MATCH",
                "Field": "tenancy",
                "Value": "Shared",
            },
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
        ],
    )
    return tuple(map(unpack_on_demand_pricelist, map(json.loads, ondemand)))


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


def get_ec2_basic_price_list(
    region=None, client=None, session=None, reset_cache=False
):
    """
    on-demand rates are USD / instance-hour.
    EBS volume is USD / GB-month.
    EBS throughput is USD / Gibps-month.
    EBS IOPS is USD / IOPS-month.
    CPU credits are too complicated to explain in this margin.
    """
    if region is None:
        client = init_client("pricing", client, session)
        region = clarify_region(region, client)
    cache_path = Path(GENERAL_DEFAULTS["cache_path"])
    prefix = f"ec2_basic_price_list_{region}"
    if reset_cache is False:
        cached_results = check_cached_results(cache_path, prefix, max_age=7)
        if cached_results is not None:
            return pickle.load(cached_results.open("rb"))
    if client is None:
        client = init_client("pricing", client, session)
    prices = {
        "ondemand": get_on_demand_rates(region, client),
        "credits": get_cpu_credit_rates(region, client),
        "ebs": get_ebs_storage_rates(region, client),
    }
    clear_cached_results(cache_path, prefix)
    with Path(cache_path, f"{prefix }_{filestamp()}.pkl").open("wb") as stream:
        pickle.dump(prices, stream)
    return prices
