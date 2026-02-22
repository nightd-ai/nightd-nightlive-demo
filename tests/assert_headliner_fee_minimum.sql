-- Headliner slots must have fees at or above the venue's headliner minimum fee.

select
    slot_id,
    event_id,
    artist_id,
    fee,
    headliner_minimum_fee,
    venue_name
from {{ ref('srv_lineup') }}
where is_headliner = true
  and (fee < headliner_minimum_fee
       or fee is null
       or headliner_minimum_fee is null)
