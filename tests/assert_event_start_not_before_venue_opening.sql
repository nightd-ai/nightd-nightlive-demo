-- For completed events, the event start time-of-day must be at or after
-- the venue's published opening time. An event cannot start before the
-- venue is licensed to open.
--
-- Only applies to events starting in the evening (hour >= 12), to avoid
-- false positives for events that start after midnight (where the time-of-day
-- comparison is not meaningful).

select
    e.event_id,
    e.event_name,
    e.event_start,
    v.opening_hours_start,
    strftime(e.event_start, '%H:%M') as actual_start_time
from {{ ref('srv_events') }} e
inner join {{ ref('srv_venues') }} v on e.venue_id = v.venue_id
where e.event_status in ('completed', 'live')
  and e.event_start is not null
  and extract(hour from e.event_start) >= 12
  and strftime(e.event_start, '%H:%M') < v.opening_hours_start
