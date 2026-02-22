-- Booking agreed_fee should match the total of lineup slot fees for the
-- same event/artist combination. A mismatch indicates a contract discrepancy.

with lineup_totals as (
    select
        event_id,
        artist_id,
        sum(fee) as total_lineup_fee
    from {{ ref('srv_lineup') }}
    group by event_id, artist_id
)

select
    b.booking_id,
    b.event_id,
    b.artist_id,
    b.agreed_fee,
    lt.total_lineup_fee,
    abs(coalesce(b.agreed_fee, 0) - coalesce(lt.total_lineup_fee, 0)) as difference
from {{ ref('srv_bookings') }} b
left join lineup_totals lt
    on b.event_id = lt.event_id
    and b.artist_id = lt.artist_id
where abs(b.agreed_fee - lt.total_lineup_fee) > 0.01
   or lt.total_lineup_fee is null
   or b.agreed_fee is null
