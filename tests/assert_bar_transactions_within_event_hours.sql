-- Bar transactions should only occur during the event's operating window.

select
    transaction_id,
    event_id,
    transaction_timestamp,
    event_start,
    event_end
from {{ ref('srv_bar_transactions') }}
where transaction_timestamp < event_start
   or transaction_timestamp > event_end
   or event_start is null
   or event_end is null
