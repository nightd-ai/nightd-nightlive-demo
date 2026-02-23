-- Within each event, the headliner fee must be at least 3x the minimum opener fee.
-- This constrains fee ratios to prevent unrealistic artist pricing.

with event_fees as (
    select
        event_id,
        max(case when is_headliner then fee end) as max_headliner_fee,
        min(case when not is_headliner and booking_tier = 'opener' then fee end) as min_opener_fee
    from {{ ref('srv_lineup') }}
    where event_status in ('completed', 'live')
    group by event_id
)

select
    event_id,
    max_headliner_fee,
    min_opener_fee,
    max_headliner_fee * 1.0 / nullif(min_opener_fee, 0) as ratio
from event_fees
where max_headliner_fee is not null
  and min_opener_fee is not null
  and max_headliner_fee < 3.0 * min_opener_fee
