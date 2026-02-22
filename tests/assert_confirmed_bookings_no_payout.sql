-- Bookings with payment_status = 'confirmed' should not have payout records.
-- A payout should only exist for bookings with payment_status = 'paid'.

select
    booking_id,
    event_id,
    artist_id,
    payment_status,
    payout_id,
    payout_amount
from {{ ref('srv_bookings') }}
where payment_status = 'confirmed'
  and payout_id is not null
