-- Artists with booking_tier = 'opener' should not be placed in headliner
-- slots. This is a booking policy violation.

select
    slot_id,
    event_id,
    artist_id,
    artist_name,
    booking_tier,
    is_headliner,
    fee
from {{ ref('srv_lineup') }}
where booking_tier = 'opener'
  and is_headliner = true
