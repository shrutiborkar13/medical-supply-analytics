select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_date,
    o.delivered_date,
    o.estimated_date,
    p.payment_amount,
    p.payment_type,
    case
        when o.delivered_date <= o.estimated_date then 'on_time'
        when o.delivered_date > o.estimated_date then 'late'
        else 'not_delivered'
    end as delivery_status,
    case
        when o.delivered_date > o.estimated_date
        then cast(o.delivered_date as date) - cast(o.estimated_date as date)
        else 0
    end as days_late
from {{ ref('stg_orders') }} o
left join {{ ref('stg_payments') }} p
    on o.order_id = p.order_id
    