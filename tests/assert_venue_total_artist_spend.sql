-- Per venue across all completed events, total artist spend should be within
-- realistic bounds: between 5000 and 100000.

with venue_spend as (
    select
        e.venue_id,
        v.venue_name,
        sum(e.total_artist_costs) as total_spend
    from {{ ref('srv_events') }} e
    inner join {{ ref('srv_venues') }} v on e.venue_id = v.venue_id
    where e.event_status = 'completed'
      and e.total_artist_costs > 0
    group by e.venue_id, v.venue_name
)

select
    venue_id,
    venue_name,
    total_spend
from venue_spend
where total_spend < 5000
   or total_spend > 100000
