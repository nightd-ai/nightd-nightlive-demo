-- For completed events with door entries, attendance count should be at least 2
-- and should not exceed venue capacity.

select
    e.event_id,
    e.event_name,
    e.total_attendance,
    v.total_capacity
from {{ ref('srv_events') }} e
inner join {{ ref('srv_venues') }} v on e.venue_id = v.venue_id
where e.event_status = 'completed'
  and e.total_attendance > 0
  and (e.total_attendance < 2
       or e.total_attendance > v.total_capacity)
