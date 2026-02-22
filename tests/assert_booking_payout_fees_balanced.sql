-- Per event, total booking agreed fees must equal total payout amounts.
-- Any imbalance indicates missing payouts, NULL amounts, or broken
-- booking-payout linkage.

select
    event_id,
    event_name,
    booking_fee_total,
    payout_total,
    booking_payout_gap
from {{ ref('srv_event_financials') }}
where abs(booking_payout_gap) > 0.01
