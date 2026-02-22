-- Every used ticket must reference a valid event with a known venue.
-- NULL event_id means the ticket cannot be associated with any event.

select
    ticket_id,
    event_id,
    venue_id,
    ticket_type,
    ticket_status
from {{ ref('srv_tickets') }}
where ticket_status = 'used'
  and (event_id is null
       or venue_id is null)
