-- Per event, total lineup slot fees must equal total booking agreed fees.
-- Any imbalance indicates missing or incorrect fee data, NULL fees, or
-- records assigned to the wrong event.

select
    event_id,
    event_name,
    lineup_fee_total,
    booking_fee_total,
    lineup_booking_gap
from {{ ref('srv_event_financials') }}
where abs(lineup_booking_gap) > 0.01
