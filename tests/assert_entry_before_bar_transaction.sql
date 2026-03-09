-- Bar transactions must occur after the patron's door entry timestamp.
-- You can't buy a drink before you've entered the venue.

select
    bt.transaction_id,
    bt.entry_id,
    bt.transaction_timestamp,
    de.entry_timestamp
from {{ ref('srv_bar_transactions') }} bt
inner join {{ ref('srv_door_entries') }} de on bt.entry_id = de.entry_id
where bt.transaction_timestamp <= de.entry_timestamp
