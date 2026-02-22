-- Bar transaction venue_id must match the venue of the event it references.

select
    transaction_id,
    event_id,
    venue_id as bar_venue_id,
    event_venue_id
from {{ ref('srv_bar_transactions') }}
where venue_id != event_venue_id
   or venue_id is null
   or event_venue_id is null
