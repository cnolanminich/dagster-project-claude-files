"""Apple App Store Connect data ingestion using dlt REST API source.

Uses the App Store Connect API v1 to load app analytics data.
API docs: https://developer.apple.com/documentation/appstoreconnectapi

In demo mode, generates realistic sample data locally.
In production, uses JWT-based auth against the App Store Connect API.
"""

import os
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.rest_api import rest_api_source

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


def create_app_store_connect_source():
    """Create a dlt REST API source for App Store Connect API."""
    return rest_api_source(
        {
            "client": {
                "base_url": "https://api.appstoreconnect.apple.com/v1/",
                "auth": {
                    "type": "bearer",
                    "token": dlt.secrets.value,
                },
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "links.next",
                },
            },
            "resources": [
                {
                    "name": "apps",
                    "endpoint": {
                        "path": "apps",
                        "params": {
                            "fields[apps]": "bundleId,name,primaryLocale,sku",
                        },
                    },
                    "primary_key": "id",
                },
                {
                    "name": "app_store_versions",
                    "endpoint": {
                        "path": "apps/{app_id}/appStoreVersions",
                        "params": {
                            "fields[appStoreVersions]": "versionString,appStoreState,createdDate,platform",
                            "app_id": {
                                "type": "resolve",
                                "resource": "apps",
                                "field": "id",
                            },
                        },
                    },
                    "primary_key": "id",
                },
                {
                    "name": "customer_reviews",
                    "endpoint": {
                        "path": "apps/{app_id}/customerReviews",
                        "params": {
                            "fields[customerReviews]": "rating,title,body,reviewerNickname,createdDate,territory",
                            "sort": "-createdDate",
                            "app_id": {
                                "type": "resolve",
                                "resource": "apps",
                                "field": "id",
                            },
                        },
                    },
                    "primary_key": "id",
                },
                {
                    "name": "sales_reports",
                    "endpoint": {
                        "path": "salesReports",
                        "params": {
                            "filter[reportType]": "SALES",
                            "filter[reportSubType]": "SUMMARY",
                            "filter[frequency]": "DAILY",
                            "filter[vendorNumber]": dlt.config.value,
                        },
                    },
                },
            ],
        },
        name="apple_app_store",
    )


@dlt.source(name="apple_app_store")
def demo_apple_app_store_source() -> list:
    """Demo source with realistic Apple App Store data."""

    @dlt.resource(write_disposition="replace", name="apps")
    def apps() -> Iterator[TDataItem]:
        yield from [
            {"id": "app_001", "bundleId": "com.acme.mainapp", "name": "Acme Pro", "primaryLocale": "en-US", "sku": "ACME_PRO_001"},
            {"id": "app_002", "bundleId": "com.acme.lite", "name": "Acme Lite", "primaryLocale": "en-US", "sku": "ACME_LITE_001"},
        ]

    @dlt.resource(write_disposition="replace", name="app_store_versions")
    def app_store_versions() -> Iterator[TDataItem]:
        yield from [
            {"id": "ver_001", "app_id": "app_001", "versionString": "3.2.1", "appStoreState": "READY_FOR_SALE", "createdDate": "2025-01-15", "platform": "IOS"},
            {"id": "ver_002", "app_id": "app_001", "versionString": "3.2.0", "appStoreState": "REPLACED", "createdDate": "2024-12-01", "platform": "IOS"},
            {"id": "ver_003", "app_id": "app_002", "versionString": "2.0.0", "appStoreState": "READY_FOR_SALE", "createdDate": "2025-02-01", "platform": "IOS"},
        ]

    @dlt.resource(write_disposition="append", name="customer_reviews")
    def customer_reviews() -> Iterator[TDataItem]:
        import random

        titles = ["Great app!", "Love it!", "Needs work", "Perfect", "Could be better", "Amazing!", "Solid app", "Best in class"]
        bodies = [
            "This app has completely transformed my workflow. Highly recommended!",
            "Great concept but crashes occasionally on my iPad.",
            "The latest update is a huge improvement. Keep it up!",
            "Simple, intuitive, and powerful. Everything I needed.",
            "Good app but I wish it had better offline support.",
        ]
        territories = ["USA", "GBR", "DEU", "FRA", "JPN", "CAN", "AUS"]
        for i in range(75):
            review_date = date.today() - timedelta(days=random.randint(0, 90))
            yield {
                "id": f"review_{i:05d}",
                "app_id": random.choice(["app_001", "app_002"]),
                "rating": random.choices([1, 2, 3, 4, 5], weights=[3, 4, 8, 25, 60])[0],
                "title": random.choice(titles),
                "body": random.choice(bodies),
                "reviewerNickname": f"user_{random.randint(1000, 9999)}",
                "createdDate": review_date.isoformat(),
                "territory": random.choice(territories),
            }

    @dlt.resource(write_disposition="append", name="sales_reports")
    def sales_reports() -> Iterator[TDataItem]:
        import random

        base_date = date.today() - timedelta(days=90)
        product_types = ["1F", "1T", "IA1", "FI1"]  # Free, Paid, IAP, Free IAP
        type_labels = {"1F": "Free Download", "1T": "Paid Download", "IA1": "In-App Purchase", "FI1": "Free In-App"}
        territories = ["US", "GB", "DE", "FR", "JP", "CA", "AU"]
        for day_offset in range(90):
            current_date = base_date + timedelta(days=day_offset)
            for app_id, app_name in [("com.acme.mainapp", "Acme Pro"), ("com.acme.lite", "Acme Lite")]:
                for territory in random.sample(territories, k=random.randint(3, 6)):
                    product_type = random.choice(product_types)
                    units = random.randint(5, 500)
                    price = 0.0 if product_type in ("1F", "FI1") else round(random.uniform(0.99, 19.99), 2)
                    yield {
                        "date": current_date.isoformat(),
                        "app_apple_id": app_id,
                        "app_name": app_name,
                        "product_type_identifier": product_type,
                        "product_type": type_labels[product_type],
                        "units": units,
                        "developer_proceeds": round(units * price * 0.7, 2),
                        "customer_currency": "USD",
                        "country_code": territory,
                        "device": random.choice(["iPhone", "iPad", "Mac"]),
                    }

    return [apps, app_store_versions, customer_reviews, sales_reports]


if DEMO_MODE:
    my_load_source = demo_apple_app_store_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="apple_app_store",
        destination="duckdb",
        dataset_name="apple_app_store_raw",
    )
else:
    my_load_source = create_app_store_connect_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="apple_app_store",
        destination="databricks",
        dataset_name="apple_app_store_raw",
    )
