-- For completed/live events, every artist-event pair in the ledger must have
-- both a lineup slot and a booking. Missing either side indicates NULL
-- artist_ids or event_ids that break the FULL OUTER JOIN matching.

select
    ael.event_id,
    ael.artist_id,
    ael.slot_id,
    ael.booking_id,
    ael.has_lineup,
    ael.has_booking
from {{ ref('srv_artist_event_ledger') }} ael
inner join {{ ref('srv_events') }} e on ael.event_id = e.event_id
where e.event_status in ('completed', 'live')
  and (not ael.has_lineup or not ael.has_booking)
