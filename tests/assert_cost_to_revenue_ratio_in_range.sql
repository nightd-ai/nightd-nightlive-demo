-- Average artist_costs / revenue ratio across all completed events should be
-- between 1.5 and 2.5. This constrains the overall financial balance of the
-- pipeline — artist costs are roughly 2x total revenue from tickets + bar.

with event_ratios as (
    select
        event_id,
        event_name,
        total_artist_costs,
        total_revenue,
        cost_to_revenue_ratio
    from {{ ref('srv_events') }}
    where event_status = 'completed'
      and total_revenue > 0
),

avg_ratio as (
    select avg(cost_to_revenue_ratio) as avg_cost_revenue_ratio
    from event_ratios
)

select
    avg_cost_revenue_ratio
from avg_ratio
where avg_cost_revenue_ratio < 1.5
   or avg_cost_revenue_ratio > 2.5
