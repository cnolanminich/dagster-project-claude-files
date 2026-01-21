-- Customer 360 view combining all customer-related data

with customers as (
    select * from {{ ref('stg_customers') }}
),

opportunities as (
    select
        customer_id,
        count(*) as total_opportunities,
        sum(case when is_won then 1 else 0 end) as won_opportunities,
        sum(case when is_closed and not is_won then 1 else 0 end) as lost_opportunities,
        sum(case when not is_closed then 1 else 0 end) as open_opportunities,
        sum(case when is_won then amount else 0 end) as total_won_revenue,
        sum(case when not is_closed then amount else 0 end) as pipeline_value
    from {{ ref('stg_opportunities') }}
    group by customer_id
),

transactions as (
    select
        customer_id,
        count(*) as total_transactions,
        sum(amount) as total_transaction_value,
        max(transaction_date) as last_transaction_date
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.industry,
    c.annual_revenue,
    c.source_system,
    c.created_date as customer_since,

    -- Opportunity metrics
    coalesce(o.total_opportunities, 0) as total_opportunities,
    coalesce(o.won_opportunities, 0) as won_opportunities,
    coalesce(o.lost_opportunities, 0) as lost_opportunities,
    coalesce(o.open_opportunities, 0) as open_opportunities,
    coalesce(o.total_won_revenue, 0) as total_won_revenue,
    coalesce(o.pipeline_value, 0) as pipeline_value,

    -- Transaction metrics
    coalesce(t.total_transactions, 0) as total_transactions,
    coalesce(t.total_transaction_value, 0) as total_transaction_value,
    t.last_transaction_date,

    -- Calculated fields
    case
        when o.total_opportunities > 0
        then round(o.won_opportunities::float / o.total_opportunities * 100, 2)
        else 0
    end as win_rate_pct,

    current_timestamp() as _updated_at

from customers c
left join opportunities o on c.customer_id = o.customer_id
left join transactions t on c.customer_id = t.customer_id
