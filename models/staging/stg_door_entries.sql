select
    cast(entry_id as varchar) as entry_id,
    cast(event_id as varchar) as event_id,
    cast(venue_id as varchar) as venue_id,
    cast(ticket_id as varchar) as ticket_id,
    cast(entry_timestamp as timestamp) as entry_timestamp,
    entry_status,
    cast(is_reentry as boolean) as is_reentry

from {{ source('nightlive', 'door_entries') }}
