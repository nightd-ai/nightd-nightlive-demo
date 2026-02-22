-- Every room that has lineup slots for a completed or live event must
-- have a corresponding sound check record.

with active_rooms as (
    select distinct
        event_id,
        venue_id,
        room
    from {{ ref('srv_lineup') }}
    where event_status in ('completed', 'live')
),

sound_checks as (
    select distinct
        event_id,
        venue_id,
        room
    from {{ ref('srv_sound_checks') }}
)

select
    ar.event_id,
    ar.venue_id,
    ar.room
from active_rooms ar
left join sound_checks sc
    on ar.event_id = sc.event_id
    and ar.venue_id = sc.venue_id
    and ar.room = sc.room
where sc.event_id is null
