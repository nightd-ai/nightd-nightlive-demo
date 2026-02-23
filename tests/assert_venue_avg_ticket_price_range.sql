-- Per-venue average ticket price (for used tickets) must fall within realistic bands.
-- Small venues: 12-22, medium: 15-28, large: 18-35.

with venue_ticket_avgs as (
    select
        t.venue_id,
        v.total_capacity,
        avg(t.price) as avg_ticket_price
    from {{ ref('srv_tickets') }} t
    inner join {{ ref('srv_venues') }} v on t.venue_id = v.venue_id
    where t.ticket_status = 'used'
    group by t.venue_id, v.total_capacity
)

select
    venue_id,
    total_capacity,
    avg_ticket_price
from venue_ticket_avgs
where (total_capacity <= 500 and (avg_ticket_price < 12.00 or avg_ticket_price > 22.00))
   or (total_capacity > 500 and total_capacity <= 800 and (avg_ticket_price < 15.00 or avg_ticket_price > 28.00))
   or (total_capacity > 800 and (avg_ticket_price < 18.00 or avg_ticket_price > 35.00))
