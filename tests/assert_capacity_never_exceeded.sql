-- Running occupancy (admitted entries minus no exit tracking, so cumulative admits)
-- should never exceed the venue's total capacity at any point during an event.

select
    event_id,
    venue_id,
    entry_timestamp,
    running_occupancy as current_occupancy,
    total_capacity
from {{ ref('srv_door_entries') }}
where running_occupancy > total_capacity
