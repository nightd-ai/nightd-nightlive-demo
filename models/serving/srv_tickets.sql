with tickets as (
    select * from {{ ref('stg_tickets') }}
),

events as (
    select * from {{ ref('stg_events') }}
)

select
    t.ticket_id,
    t.event_id,
    t.ticket_type,
    t.price,
    t.purchase_timestamp,
    t.ticket_status,
    e.event_name,
    e.venue_id

from tickets t
left join events e on t.event_id = e.event_id
