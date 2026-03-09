-- Bar revenue should be between 15% and 50% of total revenue for each event
-- with both ticket and bar revenue.

select
    event_id,
    event_name,
    bar_revenue,
    total_revenue,
    bar_revenue * 100.0 / nullif(total_revenue, 0) as bar_pct
from {{ ref('srv_events') }}
where event_status = 'completed'
  and total_revenue > 0
  and ticket_revenue > 0
  and bar_revenue > 0
  and (bar_revenue * 100.0 / total_revenue < 15.0
       or bar_revenue * 100.0 / total_revenue > 50.0)
