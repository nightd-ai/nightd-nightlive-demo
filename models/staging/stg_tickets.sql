select
    cast(ticket_id as varchar) as ticket_id,
    cast(event_id as varchar) as event_id,
    ticket_type,
    cast(price as numeric) as price,
    cast(purchase_timestamp as timestamp) as purchase_timestamp,
    ticket_status

from {{ source('nightlive', 'tickets') }}
