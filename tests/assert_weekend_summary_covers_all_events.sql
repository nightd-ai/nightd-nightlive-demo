-- Every completed/live event must appear in the weekend summary aggregation.
-- Events with NULL event_date or NULL venue_id cannot be mapped to a weekend
-- and will be missing from the summary.

with event_weekends as (
    select
        event_id,
        venue_id,
        event_date,
        case
            when extract(dow from event_date) = 5 then event_date
            when extract(dow from event_date) = 6 then event_date - interval '1 day'
            when extract(dow from event_date) = 0 then event_date - interval '2 days'
            when extract(dow from event_date) = 1 then event_date - interval '3 days'
            else event_date
        end as weekend_start
    from {{ ref('srv_events') }}
    where event_status in ('completed', 'live')
),

matched as (
    select
        ew.event_id
    from event_weekends ew
    inner join {{ ref('srv_weekend_summary') }} ws
        on ew.venue_id = ws.venue_id
        and cast(ew.weekend_start as date) = ws.weekend_start
)

select
    e.event_id,
    e.venue_id,
    e.event_date
from {{ ref('srv_events') }} e
left join matched m on e.event_id = m.event_id
where e.event_status in ('completed', 'live')
  and m.event_id is null
