-- Staging model for financial transactions from NetSuite

with source as (
    select
        transaction_id,
        customer_id,
        transaction_type,
        amount,
        transaction_date,
        status,
        currency_code
    from {{ source('fivetran_netsuite', 'transactions') }}
)

select
    transaction_id,
    customer_id,
    transaction_type,
    amount,
    transaction_date,
    status,
    currency_code,
    current_timestamp() as _loaded_at
from source
