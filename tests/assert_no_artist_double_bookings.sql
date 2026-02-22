-- Artists must not be playing at two different venues at overlapping times.
-- This checks for cross-venue schedule conflicts (same artist, different events,
-- overlapping set times).

select
    a.artist_id,
    a.slot_id as slot_a,
    b.slot_id as slot_b,
    a.event_id as event_a,
    b.event_id as event_b,
    a.venue_id as venue_a,
    b.venue_id as venue_b,
    a.set_start as a_start,
    a.set_end as a_end,
    b.set_start as b_start,
    b.set_end as b_end
from {{ ref('srv_lineup') }} a
inner join {{ ref('srv_lineup') }} b
    on a.artist_id = b.artist_id
    and a.event_id != b.event_id
    and a.set_start < b.set_end
    and a.set_end > b.set_start
where a.slot_id < b.slot_id
