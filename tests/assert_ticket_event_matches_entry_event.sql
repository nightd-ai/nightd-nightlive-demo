-- When a door entry references a ticket, the ticket's event_id must match
-- the door entry's event_id. A mismatch means a ticket is being used at
-- the wrong event.

select
    entry_id,
    event_id as entry_event_id,
    ticket_id,
    ticket_event_id
from {{ ref('srv_door_entries') }}
where ticket_id is not null
  and event_id != ticket_event_id
