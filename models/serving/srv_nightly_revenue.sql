with ticket_rev as (
    select
        event_id,
        sum(case when ticket_status = 'used' then price else 0 end) as ticket_revenue
    from {{ ref('stg_tickets') }}
    group by event_id
),

bar_rev as (
    select
        event_id,
        sum(amount) as bar_revenue
    from {{ ref('stg_bar_transactions') }}
    group by event_id
),

events as (
    select * from {{ ref('stg_events') }}
)

select
    e.event_id,
    e.venue_id,
    e.event_name,
    e.event_date,
    coalesce(tr.ticket_revenue, 0) as ticket_revenue,
    coalesce(br.bar_revenue, 0) as bar_revenue,
    coalesce(tr.ticket_revenue, 0) + coalesce(br.bar_revenue, 0) as total_revenue

from events e
left join ticket_rev tr on e.event_id = tr.event_id
left join bar_rev br on e.event_id = br.event_id
