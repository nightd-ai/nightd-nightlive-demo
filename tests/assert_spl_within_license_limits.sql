-- Sound check SPL readings must not exceed the venue's licensed SPL limit.

select
    soundcheck_id,
    event_id,
    venue_id,
    room,
    spl_reading_db,
    spl_limit_db,
    spl_reading_db - spl_limit_db as excess_db
from {{ ref('srv_sound_checks') }}
where spl_reading_db > spl_limit_db
   or spl_limit_db is null
   or spl_reading_db is null
