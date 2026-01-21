-- Revenue analytics aggregated by time period and dimensions

with opportunities as (
    select * from {{ ref('stg_opportunities') }}
    where is_closed = true
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Monthly opportunity revenue
monthly_opps as (
    select
        date_trunc('month', close_date) as month,
        c.industry,
        c.source_system,
        sum(case when o.is_won then o.amount else 0 end) as won_revenue,
        sum(case when not o.is_won then o.amount else 0 end) as lost_revenue,
        count(case when o.is_won then 1 end) as won_deals,
        count(case when not o.is_won then 1 end) as lost_deals
    from opportunities o
    left join customers c on o.customer_id = c.customer_id
    group by 1, 2, 3
),

-- Monthly transaction revenue
monthly_txns as (
    select
        date_trunc('month', t.transaction_date) as month,
        c.industry,
        c.source_system,
        sum(t.amount) as transaction_revenue,
        count(*) as transaction_count
    from transactions t
    left join customers c on t.customer_id = c.customer_id
    group by 1, 2, 3
)

select
    coalesce(o.month, t.month) as month,
    coalesce(o.industry, t.industry) as industry,
    coalesce(o.source_system, t.source_system) as source_system,

    -- Opportunity metrics
    coalesce(o.won_revenue, 0) as won_revenue,
    coalesce(o.lost_revenue, 0) as lost_revenue,
    coalesce(o.won_deals, 0) as won_deals,
    coalesce(o.lost_deals, 0) as lost_deals,

    -- Transaction metrics
    coalesce(t.transaction_revenue, 0) as transaction_revenue,
    coalesce(t.transaction_count, 0) as transaction_count,

    -- Total revenue
    coalesce(o.won_revenue, 0) + coalesce(t.transaction_revenue, 0) as total_revenue,

    current_timestamp() as _updated_at

from monthly_opps o
full outer join monthly_txns t
    on o.month = t.month
    and o.industry = t.industry
    and o.source_system = t.source_system
