with events as (
    select * from {{ ref('srv_events') }}
),

weekends as (
    select
        *,
        case
            when extract(dow from event_date) = 5 then event_date
            when extract(dow from event_date) = 6 then event_date - interval '1 day'
            when extract(dow from event_date) = 0 then event_date - interval '2 days'
            when extract(dow from event_date) = 1 then event_date - interval '3 days'
            else event_date
        end as weekend_start
    from events
)

select
    venue_id,
    venue_name,
    cast(weekend_start as date) as weekend_start,
    count(*) as events_count,
    sum(total_attendance) as total_attendance,
    sum(rejections) as total_rejections,
    sum(ticket_revenue) as total_ticket_revenue,
    sum(bar_revenue) as total_bar_revenue,
    sum(total_revenue) as total_revenue,
    sum(total_artist_costs) as total_artist_costs,
    sum(net_profit) as total_net_profit,
    sum(tickets_sold) as total_tickets_sold

from weekends
group by venue_id, venue_name, weekend_start
