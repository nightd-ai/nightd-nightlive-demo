-- Per event, the total payout amount must equal the total paid booking fees.
-- This is an alternative reconciliation check using the payouts table directly.

with event_payouts as (
    select
        event_id,
        sum(amount) as total_payout
    from {{ ref('srv_payouts') }}
    where event_id is not null
    group by event_id
),

event_bookings as (
    select
        event_id,
        sum(agreed_fee) as total_booking_fees
    from {{ ref('srv_bookings') }}
    where payment_status = 'paid'
    group by event_id
)

select
    coalesce(ep.event_id, eb.event_id) as event_id,
    ep.total_payout,
    eb.total_booking_fees,
    abs(coalesce(ep.total_payout, 0) - coalesce(eb.total_booking_fees, 0)) as difference
from event_payouts ep
full outer join event_bookings eb on ep.event_id = eb.event_id
where abs(coalesce(ep.total_payout, 0) - coalesce(eb.total_booking_fees, 0)) > 0.01
