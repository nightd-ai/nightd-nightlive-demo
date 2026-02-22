-- Completed and live events must have all date fields populated:
-- event_date, event_start, and event_end.

select
    event_id,
    event_name,
    event_date,
    event_start,
    event_end,
    event_status
from {{ ref('srv_events') }}
where event_status in ('completed', 'live')
  and (event_date is null
       or event_start is null
       or event_end is null)
