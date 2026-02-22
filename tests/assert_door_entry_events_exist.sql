-- Every door entry must reference a valid event, and the entry's venue must
-- match the event's venue. NULL event_ids or venue_ids indicate broken
-- referential integrity.

select
    de.entry_id,
    de.event_id,
    de.venue_id,
    e.venue_id as event_venue_id
from {{ ref('srv_door_entries') }} de
left join {{ ref('srv_events') }} e on de.event_id = e.event_id
where de.event_id is null
   or e.event_id is null
   or de.venue_id is null
   or e.venue_id is null
