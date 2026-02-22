-- Every lineup slot for a completed or live event must have a corresponding
-- booking record (matching event_id + artist_id).

select
    l.slot_id,
    l.event_id,
    l.artist_id,
    l.room,
    l.fee
from {{ ref('srv_lineup') }} l
left join {{ ref('srv_bookings') }} b
    on l.event_id = b.event_id
    and l.artist_id = b.artist_id
where l.event_status in ('completed', 'live')
  and b.booking_id is null
