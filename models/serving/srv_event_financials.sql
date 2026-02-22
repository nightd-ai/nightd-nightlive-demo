-- Per-event financial reconciliation: compares lineup fees, booking fees,
-- and payout totals to detect imbalances caused by missing or incorrect data.

with lineup_fees as (
    select
        event_id,
        sum(fee) as lineup_fee_total,
        count(*) as lineup_slots
    from {{ ref('stg_lineup_slots') }}
    group by event_id
),

booking_fees as (
    select
        event_id,
        sum(agreed_fee) as booking_fee_total,
        count(*) as bookings_count
    from {{ ref('stg_bookings') }}
    group by event_id
),

paid_booking_fees as (
    select
        event_id,
        sum(agreed_fee) as paid_booking_fee_total,
        count(*) as paid_bookings_count
    from {{ ref('stg_bookings') }}
    where payment_status = 'paid'
    group by event_id
),

payout_totals as (
    select
        b.event_id,
        sum(p.amount) as payout_total,
        count(p.payout_id) as payouts_count
    from {{ ref('stg_bookings') }} b
    inner join {{ ref('stg_payouts') }} p on b.booking_id = p.booking_id
    group by b.event_id
)

select
    e.event_id,
    e.event_name,
    e.event_status,
    coalesce(lf.lineup_fee_total, 0) as lineup_fee_total,
    coalesce(lf.lineup_slots, 0) as lineup_slots,
    coalesce(bf.booking_fee_total, 0) as booking_fee_total,
    coalesce(bf.bookings_count, 0) as bookings_count,
    coalesce(pbf.paid_booking_fee_total, 0) as paid_booking_fee_total,
    coalesce(pbf.paid_bookings_count, 0) as paid_bookings_count,
    coalesce(pt.payout_total, 0) as payout_total,
    coalesce(pt.payouts_count, 0) as payouts_count,
    coalesce(lf.lineup_fee_total, 0) - coalesce(bf.booking_fee_total, 0) as lineup_booking_gap,
    coalesce(pbf.paid_booking_fee_total, 0) - coalesce(pt.payout_total, 0) as booking_payout_gap
from {{ ref('stg_events') }} e
left join lineup_fees lf on e.event_id = lf.event_id
left join booking_fees bf on e.event_id = bf.event_id
left join paid_booking_fees pbf on e.event_id = pbf.event_id
left join payout_totals pt on e.event_id = pt.event_id
where e.event_status in ('completed', 'live')
