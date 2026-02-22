-- The event end time-of-day must not exceed the venue's published closing
-- time. For cross-midnight events that end the next day (or later), the
-- time-of-day of event_end should be at or before the venue's
-- opening_hours_end.
--
-- Excludes 24-hour venues where opening_hours_start = opening_hours_end
-- (e.g., Sisyphos with 22:00-22:00).

select
    e.event_id,
    e.event_name,
    e.event_end,
    v.opening_hours_end,
    strftime(e.event_end, '%H:%M') as end_time_of_day
from {{ ref('srv_events') }} e
inner join {{ ref('srv_venues') }} v on e.venue_id = v.venue_id
where e.event_status in ('completed', 'live')
  and e.event_end is not null
  and v.opening_hours_start != v.opening_hours_end
  and strftime(e.event_end, '%H:%M') > v.opening_hours_end
