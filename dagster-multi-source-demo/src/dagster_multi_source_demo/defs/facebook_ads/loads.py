import os
from datetime import date, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


@dlt.source(name="facebook_ads")
def demo_facebook_ads_source() -> list:
    """Demo source that produces sample Facebook Ads data."""

    @dlt.resource(primary_key="id", write_disposition="replace", name="campaigns")
    def campaigns() -> Iterator[TDataItem]:
        yield from [
            {"id": "23851234567890", "name": "Spring Sale 2025", "status": "ACTIVE", "objective": "CONVERSIONS", "daily_budget": 5000, "lifetime_budget": 0},
            {"id": "23851234567891", "name": "Brand Awareness - Social", "status": "ACTIVE", "objective": "BRAND_AWARENESS", "daily_budget": 2000, "lifetime_budget": 0},
            {"id": "23851234567892", "name": "App Install Campaign", "status": "ACTIVE", "objective": "APP_INSTALLS", "daily_budget": 3000, "lifetime_budget": 0},
            {"id": "23851234567893", "name": "Lead Gen - Webinar", "status": "PAUSED", "objective": "LEAD_GENERATION", "daily_budget": 1500, "lifetime_budget": 0},
        ]

    @dlt.resource(primary_key="id", write_disposition="replace", name="ads")
    def ads() -> Iterator[TDataItem]:
        yield from [
            {"id": "23861234567890", "name": "Spring Sale - Image Ad", "campaign_id": "23851234567890", "adset_id": "23871234567890", "status": "ACTIVE", "creative_id": "23881234567890"},
            {"id": "23861234567891", "name": "Spring Sale - Video Ad", "campaign_id": "23851234567890", "adset_id": "23871234567890", "status": "ACTIVE", "creative_id": "23881234567891"},
            {"id": "23861234567892", "name": "Brand - Carousel Ad", "campaign_id": "23851234567891", "adset_id": "23871234567891", "status": "ACTIVE", "creative_id": "23881234567892"},
            {"id": "23861234567893", "name": "App Install - Static", "campaign_id": "23851234567892", "adset_id": "23871234567892", "status": "ACTIVE", "creative_id": "23881234567893"},
        ]

    @dlt.resource(primary_key="id", write_disposition="replace", name="ad_sets")
    def ad_sets() -> Iterator[TDataItem]:
        yield from [
            {"id": "23871234567890", "name": "US 25-44 Interest", "campaign_id": "23851234567890", "status": "ACTIVE", "targeting_age_min": 25, "targeting_age_max": 44, "daily_budget": 2500},
            {"id": "23871234567891", "name": "Lookalike Audience", "campaign_id": "23851234567891", "status": "ACTIVE", "targeting_age_min": 18, "targeting_age_max": 65, "daily_budget": 2000},
            {"id": "23871234567892", "name": "Mobile Users 18-34", "campaign_id": "23851234567892", "status": "ACTIVE", "targeting_age_min": 18, "targeting_age_max": 34, "daily_budget": 3000},
        ]

    @dlt.resource(primary_key="id", write_disposition="replace", name="ad_creatives")
    def ad_creatives() -> Iterator[TDataItem]:
        yield from [
            {"id": "23881234567890", "name": "Spring Sale Image", "title": "50% Off Spring Collection", "body": "Shop now and save big!", "link_url": "https://acme.com/spring-sale"},
            {"id": "23881234567891", "name": "Spring Sale Video", "title": "Spring Collection Preview", "body": "Watch our new arrivals", "link_url": "https://acme.com/spring-video"},
            {"id": "23881234567892", "name": "Brand Carousel", "title": "Discover Acme", "body": "Explore our product range", "link_url": "https://acme.com/discover"},
            {"id": "23881234567893", "name": "App Install Creative", "title": "Get the Acme App", "body": "Download now for exclusive deals", "link_url": "https://acme.com/app"},
        ]

    @dlt.resource(primary_key=["date_start", "ad_id"], write_disposition="merge", name="facebook_insights")
    def facebook_insights() -> Iterator[TDataItem]:
        import random

        base_date = date.today() - timedelta(days=30)
        ad_ids = ["23861234567890", "23861234567891", "23861234567892", "23861234567893"]
        for day_offset in range(30):
            current_date = base_date + timedelta(days=day_offset)
            for ad_id in ad_ids:
                impressions = random.randint(1000, 50000)
                clicks = int(impressions * random.uniform(0.01, 0.05))
                yield {
                    "date_start": current_date.isoformat(),
                    "date_stop": current_date.isoformat(),
                    "ad_id": ad_id,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": round(random.uniform(10, 500), 2),
                    "ctr": round(clicks / impressions, 4) if impressions else 0,
                    "cpc": round(random.uniform(0.5, 3.0), 2),
                    "cpm": round(random.uniform(5, 25), 2),
                    "reach": int(impressions * random.uniform(0.6, 0.9)),
                    "conversions": random.randint(0, int(clicks * 0.1) + 1),
                }

    return [campaigns, ads, ad_sets, ad_creatives, facebook_insights]


if DEMO_MODE:
    my_load_source = demo_facebook_ads_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="duckdb",
        dataset_name="facebook_ads_raw",
    )
else:
    from .facebook_ads import facebook_ads_source

    my_load_source = facebook_ads_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="databricks",
        dataset_name="facebook_ads_raw",
    )
