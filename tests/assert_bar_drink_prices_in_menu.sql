-- Bar transaction amounts must match valid drink prices from the venue menu.
-- Valid prices: 6.00, 7.00, 8.00, 8.50, 9.00, 10.50, 12.00, 13.50, 14.00, 16.00, 17.00

select
    transaction_id,
    event_id,
    amount,
    payment_method
from {{ ref('srv_bar_transactions') }}
where round(amount::numeric, 2) not in (6.00, 7.00, 8.00, 8.50, 9.00, 10.50, 12.00, 13.50, 14.00, 16.00, 17.00)
