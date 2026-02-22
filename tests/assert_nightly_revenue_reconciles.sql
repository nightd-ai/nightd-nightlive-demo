-- The weekend summary report's total revenue per venue per weekend should
-- match the sum of individual nightly revenues for those events.

with nightly as (
    select
        nr.event_id,
        nr.venue_id,
        nr.total_revenue,
        nr.event_date,
        -- Map to weekend start (Friday)
        case
            when extract(dow from nr.event_date) = 5 then nr.event_date
            when extract(dow from nr.event_date) = 6 then nr.event_date - interval '1 day'
            when extract(dow from nr.event_date) = 0 then nr.event_date - interval '2 days'
            when extract(dow from nr.event_date) = 1 then nr.event_date - interval '3 days'
            else nr.event_date
        end as weekend_start
    from {{ ref('srv_nightly_revenue') }} nr
),

nightly_agg as (
    select
        venue_id,
        cast(weekend_start as date) as weekend_start,
        sum(total_revenue) as nightly_total
    from nightly
    group by venue_id, cast(weekend_start as date)
),

weekend as (
    select
        venue_id,
        weekend_start,
        total_revenue as weekend_total
    from {{ ref('srv_weekend_summary') }}
)

select
    coalesce(n.venue_id, w.venue_id) as venue_id,
    coalesce(n.weekend_start, w.weekend_start) as weekend_start,
    n.nightly_total,
    w.weekend_total,
    abs(coalesce(n.nightly_total, 0) - coalesce(w.weekend_total, 0)) as difference
from nightly_agg n
full outer join weekend w
    on n.venue_id = w.venue_id
    and n.weekend_start = w.weekend_start
where abs(coalesce(n.nightly_total, 0) - coalesce(w.weekend_total, 0)) > 0.01
