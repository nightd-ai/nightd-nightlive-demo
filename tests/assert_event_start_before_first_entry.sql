-- The event start time must be at or before the earliest non-reentry
-- door entry for that event. If event_start is after the first scan,
-- the timing data is inconsistent.

with first_entries as (
    select
        event_id,
        min(entry_timestamp) as earliest_entry
    from {{ ref('stg_door_entries') }}
    where is_reentry = false
    group by event_id
)

select
    e.event_id,
    e.event_name,
    e.event_start,
    fe.earliest_entry
from {{ ref('srv_events') }} e
inner join first_entries fe on e.event_id = fe.event_id
where e.event_status in ('completed', 'live')
  and e.event_start is not null
  and e.event_start > fe.earliest_entry
