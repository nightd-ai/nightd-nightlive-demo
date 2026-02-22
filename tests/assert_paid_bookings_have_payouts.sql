-- Bookings with payment_status = 'paid' must have a corresponding
-- payout record. A "paid" booking without a payout is a state machine violation.

select
    booking_id,
    event_id,
    artist_id,
    agreed_fee,
    payment_status
from {{ ref('srv_bookings') }}
where payment_status = 'paid'
  and payout_id is null
