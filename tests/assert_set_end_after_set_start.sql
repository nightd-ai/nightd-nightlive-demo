-- Lineup slot set_end must be after set_start. An inverted time range
-- indicates a data entry error.

select
    slot_id,
    event_id,
    artist_id,
    room,
    set_start,
    set_end
from {{ ref('srv_lineup') }}
where set_end <= set_start
