with bar as (
    select * from {{ ref('stg_bar_transactions') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

door_entries as (
    select * from {{ ref('stg_door_entries') }}
)

select
    b.transaction_id,
    b.event_id,
    b.venue_id,
    b.entry_id,
    b.transaction_timestamp,
    b.item_category,
    b.amount,
    b.payment_method,
    b.tab_ref,
    e.event_name,
    e.event_start,
    e.event_end,
    e.venue_id as event_venue_id,
    de.entry_status

from bar b
left join events e on b.event_id = e.event_id
left join door_entries de on b.entry_id = de.entry_id
