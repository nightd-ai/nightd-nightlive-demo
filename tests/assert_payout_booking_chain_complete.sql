-- Every payout must link to a valid booking, and the payout amount
-- must match the booking's agreed fee. Broken chains indicate NULL
-- booking_ids or NULL amounts.

select
    payout_id,
    booking_id,
    amount,
    agreed_fee,
    event_id
from {{ ref('srv_payouts') }}
where booking_id is null
   or agreed_fee is null
   or amount is null
   or abs(amount - agreed_fee) > 0.01
