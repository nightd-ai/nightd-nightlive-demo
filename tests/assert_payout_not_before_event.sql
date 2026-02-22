-- Payouts should not be processed before the event has taken place.
-- The payout date must be on or after the event date.

select
    payout_id,
    booking_id,
    amount,
    payout_date,
    event_id,
    event_date,
    event_name
from {{ ref('srv_payouts') }}
where payout_date < event_date
