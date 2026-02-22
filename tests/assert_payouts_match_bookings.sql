-- Payout amounts must match the agreed booking fee.
-- Any mismatch indicates a financial reconciliation issue.

select
    booking_id,
    artist_id,
    event_id,
    agreed_fee,
    payout_amount,
    payout_amount - agreed_fee as difference
from {{ ref('srv_bookings') }}
where payout_amount is not null
  and abs(payout_amount - agreed_fee) > 0.01
