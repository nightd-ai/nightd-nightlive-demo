-- Sound checks must be performed before the event starts. A sound check
-- timestamp after the event start indicates improper preparation.

select
    soundcheck_id,
    event_id,
    venue_id,
    room,
    check_timestamp,
    event_start
from {{ ref('srv_sound_checks') }}
where check_timestamp >= event_start
   or event_start is null
