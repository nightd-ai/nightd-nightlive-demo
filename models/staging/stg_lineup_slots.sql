select
    cast(slot_id as varchar) as slot_id,
    cast(event_id as varchar) as event_id,
    cast(artist_id as varchar) as artist_id,
    room,
    cast(set_start as timestamp) as set_start,
    cast(set_end as timestamp) as set_end,
    cast(is_headliner as boolean) as is_headliner,
    cast(fee as numeric) as fee

from {{ source('nightlive', 'lineup_slots') }}
