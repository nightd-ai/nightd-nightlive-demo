with entries as (
    select * from {{ ref('stg_door_entries') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
),

tickets as (
    select * from {{ ref('stg_tickets') }}
),

admitted_entries as (
    select
        entry_id,
        event_id,
        venue_id,
        ticket_id,
        entry_timestamp,
        entry_status,
        is_reentry,
        case when entry_status = 'admitted' then 1 else 0 end as occupancy_delta
    from entries
)

select
    ae.entry_id,
    ae.event_id,
    ae.venue_id,
    ae.ticket_id,
    ae.entry_timestamp,
    ae.entry_status,
    ae.is_reentry,
    e.event_name,
    e.event_start,
    e.event_end,
    e.venue_id as event_venue_id,
    v.venue_name,
    v.total_capacity,
    t.event_id as ticket_event_id,
    t.ticket_status,
    sum(ae.occupancy_delta) over (
        partition by ae.event_id, ae.venue_id
        order by ae.entry_timestamp, ae.entry_id
        rows between unbounded preceding and current row
    ) as running_occupancy

from admitted_entries ae
left join events e on ae.event_id = e.event_id
left join venues v on ae.venue_id = v.venue_id
left join tickets t on ae.ticket_id = t.ticket_id
