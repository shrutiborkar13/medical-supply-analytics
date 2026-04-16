select
    product_code,
    card_type,
    count(*) as total_transactions,
    sum(case when is_fraud then 1 else 0 end) as fraud_count,
    round(
        100.0 * sum(case when is_fraud then 1 else 0 end) / count(*),
        2
    ) as fraud_rate_pct,
    round(cast(avg(transaction_amount) as numeric), 2) as avg_transaction_amount
from {{ ref('stg_fraud') }}
group by product_code, card_type
order by fraud_rate_pct desc