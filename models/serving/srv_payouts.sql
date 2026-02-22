with payouts as (
    select * from {{ ref('stg_payouts') }}
),

bookings as (
    select * from {{ ref('stg_bookings') }}
),

events as (
    select * from {{ ref('stg_events') }}
)

select
    p.payout_id,
    p.booking_id,
    p.amount,
    p.payout_date,
    p.invoice_status,
    b.event_id,
    b.artist_id,
    b.agreed_fee,
    b.payment_status,
    e.event_name,
    e.event_date

from payouts p
left join bookings b on p.booking_id = b.booking_id
left join events e on b.event_id = e.event_id
