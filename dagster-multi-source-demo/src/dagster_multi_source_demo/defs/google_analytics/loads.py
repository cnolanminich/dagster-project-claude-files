import os
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


@dlt.source(name="google_analytics", max_table_nesting=2)
def demo_google_analytics_source() -> list:
    """Demo source that produces sample Google Analytics data."""

    @dlt.resource(write_disposition="replace", name="metrics")
    def metrics() -> Iterator[TDataItem]:
        sample_metrics = [
            {"api_name": "sessions", "ui_name": "Sessions", "description": "Total sessions", "category": "Traffic"},
            {"api_name": "totalUsers", "ui_name": "Total Users", "description": "Total unique users", "category": "User"},
            {"api_name": "newUsers", "ui_name": "New Users", "description": "First-time users", "category": "User"},
            {"api_name": "screenPageViews", "ui_name": "Views", "description": "Page/screen views", "category": "Traffic"},
            {"api_name": "bounceRate", "ui_name": "Bounce Rate", "description": "Bounce rate percentage", "category": "Traffic"},
            {"api_name": "averageSessionDuration", "ui_name": "Avg Session Duration", "description": "Average session length", "category": "Traffic"},
        ]
        yield from sample_metrics

    @dlt.resource(write_disposition="replace", name="dimensions")
    def dimensions() -> Iterator[TDataItem]:
        sample_dimensions = [
            {"api_name": "date", "ui_name": "Date", "description": "Date of the session", "category": "Time"},
            {"api_name": "country", "ui_name": "Country", "description": "User country", "category": "Geo"},
            {"api_name": "city", "ui_name": "City", "description": "User city", "category": "Geo"},
            {"api_name": "deviceCategory", "ui_name": "Device Category", "description": "Device type", "category": "Platform"},
            {"api_name": "sessionSource", "ui_name": "Session Source", "description": "Traffic source", "category": "Traffic Source"},
        ]
        yield from sample_dimensions

    @dlt.resource(write_disposition="append", name="website_overview")
    def website_overview() -> Iterator[TDataItem]:
        import random

        base_date = date.today() - timedelta(days=30)
        sources = ["google", "direct", "facebook", "email", "referral"]
        countries = ["United States", "United Kingdom", "Germany", "France", "Canada", "Australia"]
        devices = ["desktop", "mobile", "tablet"]

        for day_offset in range(30):
            current_date = base_date + timedelta(days=day_offset)
            for source in random.sample(sources, k=3):
                sessions = random.randint(100, 5000)
                yield {
                    "date": current_date.isoformat(),
                    "sessionSource": source,
                    "country": random.choice(countries),
                    "deviceCategory": random.choice(devices),
                    "sessions": sessions,
                    "totalUsers": int(sessions * random.uniform(0.7, 0.95)),
                    "newUsers": int(sessions * random.uniform(0.2, 0.5)),
                    "screenPageViews": int(sessions * random.uniform(1.5, 4.0)),
                    "bounceRate": round(random.uniform(0.25, 0.75), 4),
                    "averageSessionDuration": round(random.uniform(30, 300), 2),
                }

    return [metrics, dimensions, website_overview]


if DEMO_MODE:
    my_load_source = demo_google_analytics_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_analytics",
        destination="duckdb",
        dataset_name="google_analytics_raw",
    )
else:
    from .google_analytics import google_analytics

    my_load_source = google_analytics()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="google_analytics",
        destination="databricks",
        dataset_name="google_analytics_raw",
    )
