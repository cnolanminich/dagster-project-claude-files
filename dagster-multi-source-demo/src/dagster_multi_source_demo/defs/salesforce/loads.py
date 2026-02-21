import os
from datetime import datetime, timedelta
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"


@dlt.source(name="salesforce")
def demo_salesforce_source() -> list:
    """Demo source that produces sample Salesforce CRM data."""

    @dlt.resource(write_disposition="replace", name="sf_user")
    def sf_user() -> Iterator[TDataItem]:
        yield from [
            {"Id": "005xx000001Sv1a", "Name": "Jane Smith", "Email": "jane.smith@acme.com", "Username": "jsmith@acme.com", "IsActive": True, "ProfileId": "00exx000001HKm0", "UserRoleId": "00Exx000001YSp2"},
            {"Id": "005xx000001Sv1b", "Name": "John Doe", "Email": "john.doe@acme.com", "Username": "jdoe@acme.com", "IsActive": True, "ProfileId": "00exx000001HKm0", "UserRoleId": "00Exx000001YSp3"},
            {"Id": "005xx000001Sv1c", "Name": "Alice Johnson", "Email": "alice.j@acme.com", "Username": "ajohnson@acme.com", "IsActive": True, "ProfileId": "00exx000001HKm1", "UserRoleId": "00Exx000001YSp4"},
        ]

    @dlt.resource(write_disposition="replace", name="user_role")
    def user_role() -> Iterator[TDataItem]:
        yield from [
            {"Id": "00Exx000001YSp2", "Name": "VP Sales", "DeveloperName": "VP_Sales"},
            {"Id": "00Exx000001YSp3", "Name": "Account Executive", "DeveloperName": "Account_Executive"},
            {"Id": "00Exx000001YSp4", "Name": "SDR", "DeveloperName": "SDR"},
        ]

    @dlt.resource(write_disposition="merge", name="account")
    def account() -> Iterator[TDataItem]:
        yield from [
            {"Id": "001xx000003DGb1", "Name": "TechCorp Inc", "Industry": "Technology", "AnnualRevenue": 5000000, "NumberOfEmployees": 250, "BillingCity": "San Francisco", "BillingState": "CA", "Type": "Customer", "OwnerId": "005xx000001Sv1a", "LastModifiedDate": datetime.now().isoformat()},
            {"Id": "001xx000003DGb2", "Name": "Global Retail Co", "Industry": "Retail", "AnnualRevenue": 25000000, "NumberOfEmployees": 1500, "BillingCity": "New York", "BillingState": "NY", "Type": "Customer", "OwnerId": "005xx000001Sv1b", "LastModifiedDate": datetime.now().isoformat()},
            {"Id": "001xx000003DGb3", "Name": "HealthFirst Ltd", "Industry": "Healthcare", "AnnualRevenue": 10000000, "NumberOfEmployees": 500, "BillingCity": "Boston", "BillingState": "MA", "Type": "Prospect", "OwnerId": "005xx000001Sv1a", "LastModifiedDate": datetime.now().isoformat()},
            {"Id": "001xx000003DGb4", "Name": "EduLearn Systems", "Industry": "Education", "AnnualRevenue": 3000000, "NumberOfEmployees": 100, "BillingCity": "Austin", "BillingState": "TX", "Type": "Prospect", "OwnerId": "005xx000001Sv1c", "LastModifiedDate": datetime.now().isoformat()},
        ]

    @dlt.resource(write_disposition="replace", name="contact")
    def contact() -> Iterator[TDataItem]:
        yield from [
            {"Id": "003xx000004TMa1", "FirstName": "Sarah", "LastName": "Chen", "Email": "sarah.chen@techcorp.com", "Phone": "+1-415-555-0101", "AccountId": "001xx000003DGb1", "Title": "CTO"},
            {"Id": "003xx000004TMa2", "FirstName": "Michael", "LastName": "Brown", "Email": "m.brown@globalretail.com", "Phone": "+1-212-555-0202", "AccountId": "001xx000003DGb2", "Title": "VP Engineering"},
            {"Id": "003xx000004TMa3", "FirstName": "Emily", "LastName": "Davis", "Email": "e.davis@healthfirst.com", "Phone": "+1-617-555-0303", "AccountId": "001xx000003DGb3", "Title": "Director of IT"},
        ]

    @dlt.resource(write_disposition="replace", name="lead")
    def lead() -> Iterator[TDataItem]:
        yield from [
            {"Id": "00Qxx000001LMn1", "FirstName": "Robert", "LastName": "Wilson", "Company": "FinServe Analytics", "Email": "r.wilson@finserve.com", "Status": "Open", "LeadSource": "Web", "Industry": "Financial Services", "OwnerId": "005xx000001Sv1c"},
            {"Id": "00Qxx000001LMn2", "FirstName": "Lisa", "LastName": "Park", "Company": "CloudNine Media", "Email": "l.park@cloudnine.com", "Status": "Working", "LeadSource": "Conference", "Industry": "Media", "OwnerId": "005xx000001Sv1c"},
            {"Id": "00Qxx000001LMn3", "FirstName": "David", "LastName": "Kim", "Company": "LogiTech Solutions", "Email": "d.kim@logitech-sol.com", "Status": "Qualified", "LeadSource": "Partner Referral", "Industry": "Technology", "OwnerId": "005xx000001Sv1b"},
        ]

    @dlt.resource(write_disposition="merge", name="opportunity")
    def opportunity() -> Iterator[TDataItem]:
        now = datetime.now()
        yield from [
            {"Id": "006xx000004Abc1", "Name": "TechCorp - Enterprise License", "AccountId": "001xx000003DGb1", "Amount": 150000, "StageName": "Negotiation", "CloseDate": (now + timedelta(days=30)).strftime("%Y-%m-%d"), "Probability": 75, "OwnerId": "005xx000001Sv1a", "SystemModstamp": now.isoformat()},
            {"Id": "006xx000004Abc2", "Name": "Global Retail - Platform Expansion", "AccountId": "001xx000003DGb2", "Amount": 500000, "StageName": "Proposal", "CloseDate": (now + timedelta(days=45)).strftime("%Y-%m-%d"), "Probability": 50, "OwnerId": "005xx000001Sv1b", "SystemModstamp": now.isoformat()},
            {"Id": "006xx000004Abc3", "Name": "HealthFirst - Pilot Program", "AccountId": "001xx000003DGb3", "Amount": 75000, "StageName": "Discovery", "CloseDate": (now + timedelta(days=60)).strftime("%Y-%m-%d"), "Probability": 25, "OwnerId": "005xx000001Sv1a", "SystemModstamp": now.isoformat()},
            {"Id": "006xx000004Abc4", "Name": "EduLearn - Annual Subscription", "AccountId": "001xx000003DGb4", "Amount": 35000, "StageName": "Closed Won", "CloseDate": (now - timedelta(days=10)).strftime("%Y-%m-%d"), "Probability": 100, "OwnerId": "005xx000001Sv1c", "SystemModstamp": now.isoformat()},
        ]

    @dlt.resource(write_disposition="replace", name="campaign")
    def campaign() -> Iterator[TDataItem]:
        yield from [
            {"Id": "701xx000001Abc1", "Name": "Q1 Product Launch", "Type": "Email", "Status": "Completed", "StartDate": "2025-01-15", "EndDate": "2025-03-15", "BudgetedCost": 25000, "ActualCost": 22000},
            {"Id": "701xx000001Abc2", "Name": "Annual Conference 2025", "Type": "Conference", "Status": "Planned", "StartDate": "2025-06-01", "EndDate": "2025-06-03", "BudgetedCost": 100000, "ActualCost": 0},
        ]

    return [sf_user, user_role, account, contact, lead, opportunity, campaign]


if DEMO_MODE:
    my_load_source = demo_salesforce_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="salesforce",
        destination="duckdb",
        dataset_name="salesforce_raw",
    )
else:
    from .salesforce import salesforce_source

    my_load_source = salesforce_source()
    my_load_pipeline = dlt.pipeline(
        pipeline_name="salesforce",
        destination="databricks",
        dataset_name="salesforce_raw",
    )
