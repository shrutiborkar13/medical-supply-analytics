select
    "TransactionID" as transaction_id,
    "TransactionAmt" as transaction_amount,
    "ProductCD" as product_code,
    "card4" as card_type,
    "P_emaildomain" as email_domain,
    case when "isFraud" = 1 then true else false end as is_fraud
from raw_fraud
where "TransactionID" is not null