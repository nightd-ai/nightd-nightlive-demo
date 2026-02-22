with lineup as (
    select * from {{ ref('stg_lineup_slots') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
),

events as (
    select * from {{ ref('stg_events') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
)

select
    l.slot_id,
    l.event_id,
    l.artist_id,
    a.artist_name,
    a.genre,
    a.booking_tier,
    e.event_name,
    e.venue_id,
    v.venue_name,
    e.event_start,
    e.event_end,
    e.event_status,
    l.room,
    l.set_start,
    l.set_end,
    extract(epoch from (l.set_end - l.set_start)) / 3600.0 as set_duration_hours,
    l.is_headliner,
    l.fee,
    v.headliner_minimum_fee,
    rank() over (partition by l.event_id order by l.fee desc) as fee_rank_in_event,
    sum(l.fee) over (partition by l.event_id) as total_event_lineup_fees,
    count(*) over (partition by l.event_id) as slots_in_event,
    exists (
        select 1
        from {{ ref('stg_lineup_slots') }} l2
        where l2.artist_id = l.artist_id
          and l2.slot_id != l.slot_id
          and l2.set_start < l.set_end
          and l2.set_end > l.set_start
    ) as has_schedule_conflict

from lineup l
left join artists a on l.artist_id = a.artist_id
left join events e on l.event_id = e.event_id
left join venues v on e.venue_id = v.venue_id
