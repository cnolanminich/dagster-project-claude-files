-- Staging model for customer data from multiple sources
-- Combines Salesforce accounts with NetSuite customers

with salesforce_accounts as (
    select
        id as customer_id,
        name as customer_name,
        industry,
        annual_revenue,
        created_date,
        'salesforce' as source_system
    from {{ source('fivetran_salesforce', 'accounts') }}
),

netsuite_customers as (
    select
        customer_id,
        company_name as customer_name,
        industry,
        credit_limit as annual_revenue,
        date_created as created_date,
        'netsuite' as source_system
    from {{ source('fivetran_netsuite', 'customers') }}
),

combined as (
    select * from salesforce_accounts
    union all
    select * from netsuite_customers
)

select
    md5(customer_id || '-' || source_system) as customer_key,
    customer_id,
    customer_name,
    industry,
    annual_revenue,
    created_date,
    source_system,
    current_timestamp() as _loaded_at
from combined
