-- Every lineup slot for a completed/live event must have a valid room assignment
-- and valid fee. NULL room or fee values indicate incomplete lineup data.

select
    slot_id,
    event_id,
    artist_id,
    room,
    fee,
    event_status
from {{ ref('srv_lineup') }}
where event_status in ('completed', 'live')
  and (room is null or fee is null)
