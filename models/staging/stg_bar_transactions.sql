select
    cast(transaction_id as varchar) as transaction_id,
    cast(event_id as varchar) as event_id,
    cast(venue_id as varchar) as venue_id,
    cast(entry_id as varchar) as entry_id,
    cast(timestamp as timestamp) as transaction_timestamp,
    item_category,
    cast(amount as numeric) as amount,
    payment_method,
    tab_ref

from {{ source('nightlive', 'bar_transactions') }}
