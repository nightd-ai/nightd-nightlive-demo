select
    cast(soundcheck_id as varchar) as soundcheck_id,
    cast(event_id as varchar) as event_id,
    cast(venue_id as varchar) as venue_id,
    room,
    cast(check_timestamp as timestamp) as check_timestamp,
    cast(spl_reading_db as integer) as spl_reading_db,
    equipment_list,
    cast(engineer_signoff as boolean) as engineer_signoff

from {{ source('nightlive', 'sound_checks') }}
