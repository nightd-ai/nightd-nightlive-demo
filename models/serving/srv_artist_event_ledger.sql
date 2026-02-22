-- Per artist+event record linking lineup slots, bookings, and payouts.
-- Uses FULL OUTER JOIN to detect orphaned records on either side.

with lineup as (
    select
        event_id,
        artist_id,
        fee as lineup_fee,
        slot_id
    from {{ ref('stg_lineup_slots') }}
),

bookings as (
    select
        event_id,
        artist_id,
        agreed_fee as booking_fee,
        booking_id,
        payment_status
    from {{ ref('stg_bookings') }}
),

payouts as (
    select
        booking_id,
        amount as payout_amount
    from {{ ref('stg_payouts') }}
)

select
    coalesce(l.event_id, b.event_id) as event_id,
    coalesce(l.artist_id, b.artist_id) as artist_id,
    l.slot_id,
    l.lineup_fee,
    b.booking_id,
    b.booking_fee,
    b.payment_status,
    p.payout_amount,
    l.slot_id is not null as has_lineup,
    b.booking_id is not null as has_booking,
    p.payout_amount is not null as has_payout,
    case
        when l.lineup_fee is not null and b.booking_fee is not null
        then abs(l.lineup_fee - b.booking_fee) < 0.01
        else null
    end as lineup_booking_fee_match,
    case
        when b.booking_fee is not null and p.payout_amount is not null
        then abs(b.booking_fee - p.payout_amount) < 0.01
        else null
    end as booking_payout_match
from lineup l
full outer join bookings b
    on l.event_id = b.event_id
    and l.artist_id = b.artist_id
left join payouts p on b.booking_id = p.booking_id
