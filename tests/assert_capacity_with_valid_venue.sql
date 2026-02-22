-- Admitted door entries must have valid venue data to ensure occupancy
-- tracking is accurate. Missing venue_id or total_capacity means
-- running_occupancy cannot be validated against limits.

select
    entry_id,
    event_id,
    venue_id,
    total_capacity,
    running_occupancy
from {{ ref('srv_door_entries') }}
where entry_status = 'admitted'
  and (total_capacity is null
       or venue_id is null)
