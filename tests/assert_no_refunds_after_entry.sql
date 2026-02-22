-- Tickets that have been used for door entry (admitted) should not
-- have a refunded status. Refunding after entry is a business rule violation.

select
    entry_id,
    ticket_id,
    entry_status,
    entry_timestamp,
    ticket_status
from {{ ref('srv_door_entries') }}
where entry_status = 'admitted'
  and ticket_status = 'refunded'
