-- All door entries must have a non-null entry_status (admitted or rejected).
-- A NULL status means the entry outcome was not recorded.

select
    entry_id,
    event_id,
    venue_id,
    entry_timestamp,
    entry_status
from {{ ref('srv_door_entries') }}
where entry_status is null
