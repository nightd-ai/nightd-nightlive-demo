-- No two artists should be scheduled in the same room at the same event
-- during overlapping time periods.

select
    a.event_id,
    a.room,
    a.slot_id as slot_a,
    b.slot_id as slot_b,
    a.artist_id as artist_a,
    b.artist_id as artist_b,
    a.set_start as a_start,
    a.set_end as a_end,
    b.set_start as b_start,
    b.set_end as b_end
from {{ ref('srv_lineup') }} a
inner join {{ ref('srv_lineup') }} b
    on a.event_id = b.event_id
    and a.room = b.room
    and a.set_start < b.set_end
    and a.set_end > b.set_start
    and a.slot_id < b.slot_id
