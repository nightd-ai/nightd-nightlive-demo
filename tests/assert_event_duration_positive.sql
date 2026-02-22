-- Events must have a positive duration: event_end must be after event_start.
-- Both timestamps must be present for completed/live events.

select
    event_id,
    event_name,
    event_start,
    event_end,
    event_status
from {{ ref('srv_events') }}
where event_status in ('completed', 'live')
  and (event_end <= event_start
       or event_start is null
       or event_end is null)
