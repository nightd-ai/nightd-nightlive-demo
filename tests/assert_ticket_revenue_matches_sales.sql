-- For each completed event, the ticket revenue calculated from individual
-- ticket prices (used tickets only) should match the event's recorded
-- ticket revenue in the nightly revenue report.

with ticket_detail as (
    select
        event_id,
        sum(case when ticket_status = 'used' then price else 0 end) as calculated_revenue
    from {{ ref('srv_tickets') }}
    group by event_id
),

reported_revenue as (
    select
        event_id,
        ticket_revenue as reported_revenue
    from {{ ref('srv_nightly_revenue') }}
)

select
    coalesce(td.event_id, rr.event_id) as event_id,
    td.calculated_revenue,
    rr.reported_revenue,
    abs(coalesce(td.calculated_revenue, 0) - coalesce(rr.reported_revenue, 0)) as difference
from ticket_detail td
full outer join reported_revenue rr on td.event_id = rr.event_id
where abs(coalesce(td.calculated_revenue, 0) - coalesce(rr.reported_revenue, 0)) > 0.01
