select
    order_id,
    customer_id,
    order_date,
    delivered_date,
    estimated_date,
    days_late,
    delivery_status,
    case
        when days_late >= 7 then 'critical'
        when days_late >= 3 then 'moderate'
        when days_late >= 1 then 'minor'
        else 'none'
    end as delay_category
from {{ ref('mart_order_fulfillment') }}
where delivery_status = 'late'
order by days_late desc
