with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,
        count(order_id) as total_orders,
        sum(case when order_status = 'completed' then order_amount else 0 end) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date

    from orders
    group by customer_id

),

final as (

    select
        customers.customer_id,
        customers.customer_name,
        customers.customer_email,
        coalesce(customer_orders.total_orders, 0) as total_orders,
        coalesce(customer_orders.total_revenue, 0) as total_revenue,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date

    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id

)

select * from final
