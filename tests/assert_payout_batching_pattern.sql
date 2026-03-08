-- All payouts for bookings at the same event must share the same payout_date.
-- This validates the batch processing pattern where all artists for an event
-- are paid in the same batch.

select
    p1.payout_id as payout_a,
    p2.payout_id as payout_b,
    p1.event_id,
    p1.payout_date as date_a,
    p2.payout_date as date_b
from {{ ref('srv_payouts') }} p1
inner join {{ ref('srv_payouts') }} p2
    on p1.event_id = p2.event_id
    and p1.payout_id < p2.payout_id
where p1.payout_date != p2.payout_date
