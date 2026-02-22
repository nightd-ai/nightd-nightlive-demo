with sound_checks as (
    select * from {{ ref('stg_sound_checks') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
)

select
    sc.soundcheck_id,
    sc.event_id,
    sc.venue_id,
    sc.room,
    sc.check_timestamp,
    sc.spl_reading_db,
    sc.equipment_list,
    sc.engineer_signoff,
    e.event_name,
    e.event_start,
    v.venue_name,
    v.spl_limit_db

from sound_checks sc
left join events e on sc.event_id = e.event_id
left join venues v on sc.venue_id = v.venue_id
