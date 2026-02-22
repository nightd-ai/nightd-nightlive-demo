-- A re-entry (is_reentry = true) must have a prior non-reentry admission
-- for the same ticket at the same event. A re-entry without an original
-- admission is an operational anomaly.

with first_admissions as (
    select distinct
        ticket_id,
        event_id
    from {{ ref('srv_door_entries') }}
    where is_reentry = false
      and entry_status = 'admitted'
      and ticket_id is not null
)

select
    de.entry_id,
    de.event_id,
    de.ticket_id,
    de.entry_timestamp
from {{ ref('srv_door_entries') }} de
left join first_admissions fa
    on de.ticket_id = fa.ticket_id
    and de.event_id = fa.event_id
where de.is_reentry = true
  and de.ticket_id is not null
  and fa.ticket_id is null
