"""SharePoint data ingestion using dlt REST API source.

Uses the Microsoft Graph API to load data from SharePoint Online.
API docs: https://learn.microsoft.com/en-us/graph/api/resources/sharepoint

In demo mode, generates realistic sample data locally.
In production, uses OAuth2 client credentials against Microsoft Graph API.
"""

import os
from datetime import datetime, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.rest_api import rest_api_source

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


def create_sharepoint_source():
    """Create a dlt REST API source for Microsoft Graph API (SharePoint).

    Requires Azure AD app registration with the following permissions:
    - Sites.Read.All
    - Files.Read.All
    """
    return rest_api_source(
        {
            "client": {
                "base_url": "https://graph.microsoft.com/v1.0/",
                "auth": {
                    "type": "bearer",
                    "token": dlt.secrets.value,
                },
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "@odata.nextLink",
                },
            },
            "resources": [
                {
                    "name": "sites",
                    "endpoint": {
                        "path": "sites",
                        "params": {
                            "search": dlt.config.value,
                            "$select": "id,name,displayName,webUrl,createdDateTime,lastModifiedDateTime",
                        },
                        "data_selector": "value",
                    },
                    "primary_key": "id",
                    "write_disposition": "replace",
                },
                {
                    "name": "site_lists",
                    "endpoint": {
                        "path": "sites/{site_id}/lists",
                        "params": {
                            "site_id": {
                                "type": "resolve",
                                "resource": "sites",
                                "field": "id",
                            },
                            "$select": "id,displayName,description,createdDateTime,lastModifiedDateTime,list",
                        },
                        "data_selector": "value",
                    },
                    "primary_key": "id",
                    "write_disposition": "replace",
                },
                {
                    "name": "drive_items",
                    "endpoint": {
                        "path": "sites/{site_id}/drive/root/children",
                        "params": {
                            "site_id": {
                                "type": "resolve",
                                "resource": "sites",
                                "field": "id",
                            },
                            "$select": "id,name,size,createdDateTime,lastModifiedDateTime,webUrl,file,folder",
                        },
                        "data_selector": "value",
                    },
                    "primary_key": "id",
                    "write_disposition": "replace",
                },
                {
                    "name": "list_items",
                    "endpoint": {
                        "path": "sites/{site_id}/lists/{list_id}/items",
                        "params": {
                            "site_id": {
                                "type": "resolve",
                                "resource": "sites",
                                "field": "id",
                            },
                            "list_id": {
                                "type": "resolve",
                                "resource": "site_lists",
                                "field": "id",
                            },
                            "$expand": "fields",
                        },
                        "data_selector": "value",
                    },
                    "primary_key": "id",
                    "write_disposition": "merge",
                },
            ],
        },
        name="sharepoint",
    )


@dlt.source(name="sharepoint")
def demo_sharepoint_source() -> list:
    """Demo source with realistic SharePoint data."""

    @dlt.resource(write_disposition="replace", name="sites")
    def sites() -> Iterator[TDataItem]:
        now = datetime.now()
        yield from [
            {
                "id": "acme.sharepoint.com,site-001",
                "name": "Marketing",
                "displayName": "Marketing Team Site",
                "webUrl": "https://acme.sharepoint.com/sites/marketing",
                "createdDateTime": (now - timedelta(days=365)).isoformat(),
                "lastModifiedDateTime": (now - timedelta(hours=2)).isoformat(),
            },
            {
                "id": "acme.sharepoint.com,site-002",
                "name": "Sales",
                "displayName": "Sales Team Site",
                "webUrl": "https://acme.sharepoint.com/sites/sales",
                "createdDateTime": (now - timedelta(days=400)).isoformat(),
                "lastModifiedDateTime": (now - timedelta(hours=5)).isoformat(),
            },
            {
                "id": "acme.sharepoint.com,site-003",
                "name": "Engineering",
                "displayName": "Engineering Hub",
                "webUrl": "https://acme.sharepoint.com/sites/engineering",
                "createdDateTime": (now - timedelta(days=500)).isoformat(),
                "lastModifiedDateTime": (now - timedelta(hours=1)).isoformat(),
            },
        ]

    @dlt.resource(write_disposition="replace", name="site_lists")
    def site_lists() -> Iterator[TDataItem]:
        now = datetime.now()
        lists = [
            {"id": "list_001", "site_id": "acme.sharepoint.com,site-001", "displayName": "Campaign Tracker", "description": "Track marketing campaigns and budgets", "template": "genericList"},
            {"id": "list_002", "site_id": "acme.sharepoint.com,site-001", "displayName": "Content Calendar", "description": "Editorial and content planning", "template": "genericList"},
            {"id": "list_003", "site_id": "acme.sharepoint.com,site-002", "displayName": "Deal Pipeline", "description": "Active sales opportunities", "template": "genericList"},
            {"id": "list_004", "site_id": "acme.sharepoint.com,site-002", "displayName": "Client Contacts", "description": "Key client contacts and relationships", "template": "contacts"},
            {"id": "list_005", "site_id": "acme.sharepoint.com,site-003", "displayName": "Sprint Board", "description": "Current sprint tasks and backlog", "template": "genericList"},
            {"id": "list_006", "site_id": "acme.sharepoint.com,site-003", "displayName": "Architecture Decisions", "description": "ADR tracking list", "template": "genericList"},
        ]
        for lst in lists:
            lst["createdDateTime"] = (now - timedelta(days=180)).isoformat()
            lst["lastModifiedDateTime"] = (now - timedelta(hours=3)).isoformat()
            yield lst

    @dlt.resource(write_disposition="replace", name="drive_items")
    def drive_items() -> Iterator[TDataItem]:
        import random

        now = datetime.now()
        files = [
            {"name": "Q1_Marketing_Report.pptx", "site_id": "acme.sharepoint.com,site-001", "mime_type": "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
            {"name": "Brand_Guidelines_2025.pdf", "site_id": "acme.sharepoint.com,site-001", "mime_type": "application/pdf"},
            {"name": "Campaign_Assets.zip", "site_id": "acme.sharepoint.com,site-001", "mime_type": "application/zip"},
            {"name": "Sales_Forecast_Q2.xlsx", "site_id": "acme.sharepoint.com,site-002", "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            {"name": "Proposal_Template.docx", "site_id": "acme.sharepoint.com,site-002", "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
            {"name": "Client_Presentations/", "site_id": "acme.sharepoint.com,site-002", "mime_type": None},
            {"name": "Architecture_Diagram.png", "site_id": "acme.sharepoint.com,site-003", "mime_type": "image/png"},
            {"name": "API_Spec_v3.yaml", "site_id": "acme.sharepoint.com,site-003", "mime_type": "text/yaml"},
            {"name": "Runbooks/", "site_id": "acme.sharepoint.com,site-003", "mime_type": None},
        ]
        for i, f in enumerate(files):
            is_folder = f["mime_type"] is None
            item = {
                "id": f"item_{i:04d}",
                "name": f["name"],
                "site_id": f["site_id"],
                "size": 0 if is_folder else random.randint(10000, 50000000),
                "createdDateTime": (now - timedelta(days=random.randint(10, 300))).isoformat(),
                "lastModifiedDateTime": (now - timedelta(days=random.randint(0, 30))).isoformat(),
                "webUrl": f"https://acme.sharepoint.com/sites/team/Shared%20Documents/{f['name']}",
            }
            if is_folder:
                item["folder"] = {"childCount": random.randint(2, 15)}
            else:
                item["file"] = {"mimeType": f["mime_type"]}
            yield item

    @dlt.resource(write_disposition="merge", name="list_items")
    def list_items() -> Iterator[TDataItem]:
        import random

        now = datetime.now()
        # Campaign Tracker items
        campaign_statuses = ["Active", "Planned", "Completed", "On Hold"]
        channels = ["Email", "Social Media", "PPC", "Content", "Events"]
        for i in range(15):
            yield {
                "id": f"campaign_{i:04d}",
                "list_id": "list_001",
                "site_id": "acme.sharepoint.com,site-001",
                "fields": {
                    "Title": f"Campaign {i+1}: {random.choice(['Spring', 'Summer', 'Fall', 'Winter'])} {random.choice(['Launch', 'Promo', 'Sale', 'Event'])}",
                    "Status": random.choice(campaign_statuses),
                    "Channel": random.choice(channels),
                    "Budget": random.randint(5000, 100000),
                    "StartDate": (now - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
                    "Owner": random.choice(["Jane Smith", "John Doe", "Alice Johnson"]),
                },
                "createdDateTime": (now - timedelta(days=random.randint(30, 180))).isoformat(),
                "lastModifiedDateTime": (now - timedelta(days=random.randint(0, 14))).isoformat(),
            }

        # Deal Pipeline items
        deal_stages = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
        for i in range(20):
            yield {
                "id": f"deal_{i:04d}",
                "list_id": "list_003",
                "site_id": "acme.sharepoint.com,site-002",
                "fields": {
                    "Title": f"Deal: {random.choice(['TechCorp', 'RetailCo', 'HealthFirst', 'EduLearn', 'FinServe'])} - {random.choice(['Enterprise', 'Pro', 'Standard'])}",
                    "Stage": random.choice(deal_stages),
                    "Amount": random.randint(10000, 500000),
                    "CloseDate": (now + timedelta(days=random.randint(-30, 90))).strftime("%Y-%m-%d"),
                    "SalesRep": random.choice(["John Doe", "Sarah Chen", "Michael Brown"]),
                    "Probability": random.choice([10, 25, 50, 75, 90, 100]),
                },
                "createdDateTime": (now - timedelta(days=random.randint(15, 120))).isoformat(),
                "lastModifiedDateTime": (now - timedelta(days=random.randint(0, 7))).isoformat(),
            }

    return [sites, site_lists, drive_items, list_items]


if DEMO_MODE:
    my_load_source = demo_sharepoint_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="sharepoint",
        destination="duckdb",
        dataset_name="sharepoint_raw",
    )
else:
    my_load_source = create_sharepoint_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="sharepoint",
        destination="databricks",
        dataset_name="sharepoint_raw",
    )
