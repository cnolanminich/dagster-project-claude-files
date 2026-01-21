-- Staging model for sales opportunities from Salesforce

with source as (
    select
        id as opportunity_id,
        account_id as customer_id,
        name as opportunity_name,
        stage_name as stage,
        amount,
        probability,
        close_date,
        created_date,
        is_won,
        is_closed
    from {{ source('fivetran_salesforce', 'opportunities') }}
)

select
    opportunity_id,
    customer_id,
    opportunity_name,
    stage,
    amount,
    probability,
    close_date,
    created_date,
    is_won,
    is_closed,
    current_timestamp() as _loaded_at
from source
