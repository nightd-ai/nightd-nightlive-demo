with bookings as (
    select * from {{ ref('stg_bookings') }}
),

payouts as (
    select * from {{ ref('stg_payouts') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
)

select
    b.booking_id,
    b.event_id,
    b.artist_id,
    a.artist_name,
    e.event_name,
    e.event_date,
    b.agreed_fee,
    b.payment_status,
    b.rider_requirements,
    b.cancellation_terms,
    p.payout_id,
    p.amount as payout_amount,
    p.payout_date,
    p.invoice_status,
    coalesce(p.amount, 0) - b.agreed_fee as payout_difference,
    case
        when b.payment_status = 'paid' and p.payout_id is null then true
        else false
    end as missing_payout,
    case
        when p.amount is not null and p.amount != b.agreed_fee then true
        else false
    end as amount_mismatch

from bookings b
left join payouts p on b.booking_id = p.booking_id
left join events e on b.event_id = e.event_id
left join artists a on b.artist_id = a.artist_id
