"""Google Play Store data ingestion using dlt REST API source.

Uses the Google Play Developer API v3 to load app analytics data.
API docs: https://developers.google.com/android-publisher

In demo mode, generates realistic sample data locally.
In production, uses OAuth2/service account auth against Google Play Developer API.
"""

import os
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.rest_api import rest_api_source

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


def create_google_play_source():
    """Create a dlt REST API source for Google Play Developer API."""
    return rest_api_source(
        {
            "client": {
                "base_url": "https://androidpublisher.googleapis.com/androidpublisher/v3/applications/",
                "auth": {
                    "type": "bearer",
                    "token": dlt.secrets.value,
                },
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "tokenPagination.nextPageToken",
                },
            },
            "resources": [
                {
                    "name": "reviews",
                    "endpoint": {
                        "path": "{package_name}/reviews",
                        "params": {
                            "package_name": dlt.config.value,
                            "maxResults": 100,
                            "translationLanguage": "en",
                        },
                    },
                    "primary_key": "reviewId",
                    "write_disposition": "append",
                },
                {
                    "name": "app_details",
                    "endpoint": {
                        "path": "{package_name}/edits/{edit_id}/details",
                        "params": {
                            "package_name": dlt.config.value,
                            "edit_id": dlt.config.value,
                        },
                    },
                    "write_disposition": "replace",
                },
            ],
        },
        name="google_play_store",
    )


@dlt.source(name="google_play_store")
def demo_google_play_source() -> list:
    """Demo source with realistic Google Play Store data."""

    @dlt.resource(write_disposition="replace", name="app_details")
    def app_details() -> Iterator[TDataItem]:
        yield from [
            {
                "package_name": "com.acme.mainapp",
                "title": "Acme Pro",
                "short_description": "Professional productivity suite",
                "full_description": "Acme Pro is the leading productivity app for professionals.",
                "default_language": "en-US",
                "category": "PRODUCTIVITY",
                "content_rating": "Everyone",
                "current_version": "4.1.2",
                "min_sdk_version": 26,
                "target_sdk_version": 34,
            },
            {
                "package_name": "com.acme.lite",
                "title": "Acme Lite",
                "short_description": "Free productivity tools",
                "full_description": "Get started with Acme's free productivity tools.",
                "default_language": "en-US",
                "category": "PRODUCTIVITY",
                "content_rating": "Everyone",
                "current_version": "2.3.0",
                "min_sdk_version": 24,
                "target_sdk_version": 34,
            },
        ]

    @dlt.resource(write_disposition="append", name="reviews")
    def reviews() -> Iterator[TDataItem]:
        import random

        comments = [
            "Works great on my Pixel! Smooth and fast.",
            "The app keeps crashing after the latest update.",
            "Love the new dark mode feature!",
            "Battery drain is noticeable when running in background.",
            "Best productivity app on Android. Period.",
            "UI is clean and intuitive. Great job!",
            "Needs better widget support for home screen.",
            "Syncs perfectly across all my devices.",
        ]
        languages = ["en", "de", "fr", "ja", "es", "pt"]
        devices = [
            "Pixel 8 Pro", "Samsung Galaxy S24", "OnePlus 12",
            "Samsung Galaxy A54", "Pixel 7a", "Xiaomi 14",
        ]
        for i in range(100):
            review_date = date.today() - timedelta(days=random.randint(0, 90))
            star_rating = random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0]
            yield {
                "reviewId": f"gp_review_{i:05d}",
                "package_name": random.choice(["com.acme.mainapp", "com.acme.lite"]),
                "authorName": f"Android User {random.randint(100, 999)}",
                "comments": [{
                    "userComment": {
                        "text": random.choice(comments),
                        "lastModified": {"seconds": int(review_date.strftime("%s")) if hasattr(review_date, "strftime") else 0},
                        "starRating": star_rating,
                        "device": random.choice(devices),
                        "androidOsVersion": random.choice([12, 13, 14, 15]),
                        "appVersionCode": random.choice([410, 411, 412]),
                        "originalText": random.choice(comments),
                        "reviewerLanguage": random.choice(languages),
                    }
                }],
            }

    @dlt.resource(write_disposition="append", name="store_performance")
    def store_performance() -> Iterator[TDataItem]:
        """Store listing performance metrics - simulating Play Console data."""
        import random

        base_date = date.today() - timedelta(days=90)
        acquisition_sources = ["Play Store organic", "Google Ads", "Third-party referrals", "Explore"]
        for day_offset in range(90):
            current_date = base_date + timedelta(days=day_offset)
            for package in ["com.acme.mainapp", "com.acme.lite"]:
                visitors = random.randint(500, 10000) if "mainapp" in package else random.randint(200, 5000)
                installers = int(visitors * random.uniform(0.15, 0.35))
                yield {
                    "date": current_date.isoformat(),
                    "package_name": package,
                    "store_listing_visitors": visitors,
                    "installers": installers,
                    "conversion_rate": round(installers / visitors, 4),
                    "uninstalls": int(installers * random.uniform(0.05, 0.2)),
                    "active_device_installs": random.randint(50000, 200000),
                    "update_users": int(installers * random.uniform(0.3, 0.8)),
                    "acquisition_source": random.choice(acquisition_sources),
                    "country": random.choice(["US", "IN", "BR", "GB", "DE", "JP", "FR", "KR"]),
                }

    @dlt.resource(write_disposition="append", name="revenue")
    def revenue() -> Iterator[TDataItem]:
        """Revenue data simulating Play Console financial reports."""
        import random

        base_date = date.today() - timedelta(days=90)
        transaction_types = ["Charge", "Charge refund", "Google fee", "Tax"]
        for day_offset in range(90):
            current_date = base_date + timedelta(days=day_offset)
            for package in ["com.acme.mainapp", "com.acme.lite"]:
                num_transactions = random.randint(10, 100)
                for _ in range(random.randint(1, 4)):
                    txn_type = random.choice(transaction_types)
                    amount = round(random.uniform(0.99, 49.99), 2) if txn_type == "Charge" else round(random.uniform(-15, -0.30), 2)
                    yield {
                        "date": current_date.isoformat(),
                        "package_name": package,
                        "product_id": f"{'sub' if random.random() > 0.5 else 'iap'}_{random.randint(1,5):03d}",
                        "transaction_type": txn_type,
                        "amount_merchant_currency": amount,
                        "merchant_currency": "USD",
                        "buyer_country": random.choice(["US", "IN", "BR", "GB", "DE"]),
                        "quantity": random.randint(1, num_transactions),
                    }

    return [app_details, reviews, store_performance, revenue]


if DEMO_MODE:
    my_load_source = demo_google_play_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_play_store",
        destination="duckdb",
        dataset_name="google_play_store_raw",
    )
else:
    my_load_source = create_google_play_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_play_store",
        destination="databricks",
        dataset_name="google_play_store_raw",
    )
