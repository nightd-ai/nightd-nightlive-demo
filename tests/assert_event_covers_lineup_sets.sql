-- The event must run at least until the last lineup set ends.
-- event_end must be >= the latest set_end for any lineup slot at that event.
-- An event that ends before its last DJ finishes playing indicates
-- incorrect timing data.

with last_sets as (
    select
        event_id,
        max(set_end) as latest_set_end
    from {{ ref('stg_lineup_slots') }}
    group by event_id
)

select
    e.event_id,
    e.event_name,
    e.event_end,
    ls.latest_set_end
from {{ ref('srv_events') }} e
inner join last_sets ls on e.event_id = ls.event_id
where e.event_status in ('completed', 'live')
  and e.event_end is not null
  and e.event_end < ls.latest_set_end
