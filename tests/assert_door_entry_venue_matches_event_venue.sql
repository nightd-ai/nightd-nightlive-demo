-- Door entry venue_id must match the venue of the event it references.

select
    entry_id,
    event_id,
    venue_id as entry_venue_id,
    event_venue_id
from {{ ref('srv_door_entries') }}
where venue_id != event_venue_id
   or venue_id is null
   or event_venue_id is null
