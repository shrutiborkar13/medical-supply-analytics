select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestamp) as order_date,
    cast(order_delivered_customer_date as timestamp) as delivered_date,
    cast(order_estimated_delivery_date as timestamp) as estimated_date
from raw_orders
where order_id is not null
