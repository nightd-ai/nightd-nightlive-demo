-- Lineup set_start must be at or after event_start.
-- An artist's set cannot begin before the event opens.

select
    slot_id,
    event_id,
    artist_id,
    set_start,
    event_start
from {{ ref('srv_lineup') }}
where event_status in ('completed', 'live')
  and set_start < event_start
