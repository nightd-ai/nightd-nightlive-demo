-- The same ticket should not be scanned more than once at the same event
-- as a non-reentry. Duplicate non-reentry scans indicate a scanning error.

select
    ticket_id,
    event_id,
    count(*) as scan_count
from {{ ref('srv_door_entries') }}
where is_reentry = false
  and ticket_id is not null
group by ticket_id, event_id
having count(*) > 1
