-- Sound checks must occur within 8 hours before event_start.
-- A sound check too far in advance suggests wrong event association.

select
    soundcheck_id,
    event_id,
    check_timestamp,
    event_start,
    extract(epoch from (event_start - check_timestamp)) / 3600.0 as hours_before_event
from {{ ref('srv_sound_checks') }}
where event_start is not null
  and check_timestamp is not null
  and (check_timestamp >= event_start
       or extract(epoch from (event_start - check_timestamp)) / 3600.0 > 8.0)
