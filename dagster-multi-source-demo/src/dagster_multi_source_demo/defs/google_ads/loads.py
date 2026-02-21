import os
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


@dlt.source(name="google_ads", max_table_nesting=2)
def demo_google_ads_source() -> list:
    """Demo source that produces sample Google Ads data."""

    @dlt.resource(write_disposition="replace", name="customers")
    def customers() -> Iterator[TDataItem]:
        yield from [
            {"id": "123-456-7890", "descriptive_name": "Acme Corp Main Account", "currency_code": "USD", "time_zone": "America/New_York"},
            {"id": "234-567-8901", "descriptive_name": "Acme Corp EU Account", "currency_code": "EUR", "time_zone": "Europe/Berlin"},
        ]

    @dlt.resource(write_disposition="replace", name="campaigns")
    def campaigns() -> Iterator[TDataItem]:
        campaign_data = [
            {"id": "10001", "name": "Brand Awareness - US", "status": "ENABLED", "advertising_channel_type": "SEARCH", "budget_amount_micros": 50000000},
            {"id": "10002", "name": "Product Launch - Q1", "status": "ENABLED", "advertising_channel_type": "SEARCH", "budget_amount_micros": 100000000},
            {"id": "10003", "name": "Retargeting - Website Visitors", "status": "ENABLED", "advertising_channel_type": "DISPLAY", "budget_amount_micros": 30000000},
            {"id": "10004", "name": "Competitor Keywords", "status": "PAUSED", "advertising_channel_type": "SEARCH", "budget_amount_micros": 75000000},
            {"id": "10005", "name": "App Install - Android", "status": "ENABLED", "advertising_channel_type": "MULTI_CHANNEL", "budget_amount_micros": 25000000},
        ]
        yield from campaign_data

    @dlt.resource(write_disposition="replace", name="change_events")
    def change_events() -> Iterator[TDataItem]:
        import random

        base_date = date.today() - timedelta(days=14)
        change_types = ["BUDGET", "BID", "STATUS", "AD_TEXT", "TARGETING"]
        for i in range(20):
            current_date = base_date + timedelta(days=random.randint(0, 13))
            yield {
                "change_date_time": current_date.isoformat(),
                "change_resource_type": random.choice(change_types),
                "resource_name": f"customers/123456/campaigns/1000{random.randint(1,5)}",
                "user_email": "ads-manager@acme.com",
            }

    @dlt.resource(write_disposition="replace", name="customer_clients")
    def customer_clients() -> Iterator[TDataItem]:
        yield from [
            {"client_customer": "123-456-7890", "status": "ENABLED", "level": 0},
            {"client_customer": "234-567-8901", "status": "ENABLED", "level": 1},
        ]

    return [customers, campaigns, change_events, customer_clients]


if DEMO_MODE:
    my_load_source = demo_google_ads_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_ads",
        destination="duckdb",
        dataset_name="google_ads_raw",
    )
else:
    from .google_ads import google_ads

    my_load_source = google_ads()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_ads",
        destination="databricks",
        dataset_name="google_ads_raw",
    )
