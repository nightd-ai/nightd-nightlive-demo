select
    cast(event_id as varchar) as event_id,
    cast(venue_id as varchar) as venue_id,
    event_name,
    cast(event_date as date) as event_date,
    cast(event_start as timestamp) as event_start,
    cast(event_end as timestamp) as event_end,
    door_policy,
    event_status

from {{ source('nightlive', 'events') }}
