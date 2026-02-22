-- Door entries should only occur during the event's operating window
-- (between event_start and event_end).

select
    entry_id,
    event_id,
    entry_timestamp,
    event_start,
    event_end
from {{ ref('srv_door_entries') }}
where entry_timestamp < event_start
   or entry_timestamp > event_end
   or event_start is null
   or event_end is null
