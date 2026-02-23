-- Within each event, early_bird price < regular price < at_door price.
-- This ensures ticket tier pricing is correctly ordered.

with event_type_prices as (
    select
        event_id,
        ticket_type,
        min(price) as min_price,
        max(price) as max_price
    from {{ ref('srv_tickets') }}
    where ticket_status in ('used', 'cancelled')
    group by event_id, ticket_type
),

pivoted as (
    select
        event_id,
        max(case when ticket_type = 'early_bird' then max_price end) as early_bird_max,
        min(case when ticket_type = 'regular' then min_price end) as regular_min,
        max(case when ticket_type = 'regular' then max_price end) as regular_max,
        min(case when ticket_type = 'at_door' then min_price end) as at_door_min
    from event_type_prices
    group by event_id
)

select
    event_id,
    early_bird_max,
    regular_min,
    regular_max,
    at_door_min
from pivoted
where (early_bird_max is not null and regular_min is not null and early_bird_max >= regular_min)
   or (regular_max is not null and at_door_min is not null and regular_max >= at_door_min)
