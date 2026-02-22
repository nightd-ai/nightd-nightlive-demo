-- For each event, all related operational records (door entries, bar transactions,
-- sound checks) must reference the same venue as the event. Any NULL venue_id
-- or event_id indicates broken referential integrity.

with event_venues as (
    select event_id, venue_id as expected_venue
    from {{ ref('srv_events') }}
    where event_status in ('completed', 'live')
),

all_venue_refs as (
    select event_id, venue_id, 'door_entry' as source, entry_id as record_id
    from {{ ref('stg_door_entries') }}
    union all
    select event_id, venue_id, 'bar_transaction', transaction_id
    from {{ ref('stg_bar_transactions') }}
    union all
    select event_id, venue_id, 'sound_check', soundcheck_id
    from {{ ref('stg_sound_checks') }}
)

select
    r.record_id,
    r.source,
    r.event_id,
    r.venue_id,
    ev.expected_venue
from all_venue_refs r
left join event_venues ev on r.event_id = ev.event_id
where r.venue_id is null
   or ev.expected_venue is null
   or r.venue_id != ev.expected_venue
   or r.event_id is null
