with source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        id as order_id,
        customer_id,
        order_date,
        status as order_status,
        amount as order_amount

    from source

)

select * from renamed
