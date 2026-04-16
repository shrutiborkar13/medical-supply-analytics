select
    order_id,
    payment_type,
    payment_installments,
    cast(payment_value as float) as payment_amount
from raw_payments
where order_id is not null
