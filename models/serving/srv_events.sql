with events as (
    select * from {{ ref('stg_events') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
),

lineup_stats as (
    select
        event_id,
        count(*) as total_lineup_slots,
        count(case when is_headliner then 1 end) as headliner_slots,
        sum(fee) as total_lineup_fees
    from {{ ref('stg_lineup_slots') }}
    group by event_id
),

door_stats as (
    select
        event_id,
        count(case when entry_status = 'admitted' and not is_reentry then 1 end) as total_attendance,
        count(case when entry_status = 'rejected' then 1 end) as rejections,
        count(case when is_reentry then 1 end) as reentries,
        min(entry_timestamp) as first_entry,
        max(entry_timestamp) as last_entry
    from {{ ref('stg_door_entries') }}
    group by event_id
),

ticket_revenue as (
    select
        event_id,
        sum(case when ticket_status = 'used' then price else 0 end) as ticket_revenue,
        count(*) as tickets_sold
    from {{ ref('stg_tickets') }}
    group by event_id
),

bar_stats as (
    select
        event_id,
        count(*) as bar_transaction_count,
        sum(amount) as bar_revenue
    from {{ ref('stg_bar_transactions') }}
    group by event_id
),

booking_costs as (
    select
        b.event_id,
        sum(b.agreed_fee) as total_artist_costs,
        count(*) as total_bookings,
        count(case when b.payment_status = 'paid' and p.payout_id is null then 1 end) as unpaid_bookings
    from {{ ref('stg_bookings') }} b
    left join {{ ref('stg_payouts') }} p on b.booking_id = p.booking_id
    group by b.event_id
),

sound_stats as (
    select
        event_id,
        count(*) as sound_checks_done,
        max(spl_reading_db) as max_spl_reading,
        bool_and(engineer_signoff) as all_engineers_signed_off
    from {{ ref('stg_sound_checks') }}
    group by event_id
)

select
    e.event_id,
    e.venue_id,
    v.venue_name,
    e.event_name,
    e.event_date,
    e.event_start,
    e.event_end,
    e.door_policy,
    e.event_status,
    coalesce(ls.total_lineup_slots, 0) as total_lineup_slots,
    coalesce(ls.headliner_slots, 0) as headliner_slots,
    coalesce(ds.total_attendance, 0) as total_attendance,
    coalesce(ds.rejections, 0) as rejections,
    coalesce(ds.reentries, 0) as reentries,
    coalesce(tr.ticket_revenue, 0) as ticket_revenue,
    coalesce(bs.bar_revenue, 0) as bar_revenue,
    coalesce(tr.ticket_revenue, 0) + coalesce(bs.bar_revenue, 0) as total_revenue,
    coalesce(bc.total_artist_costs, 0) as total_artist_costs,
    coalesce(tr.ticket_revenue, 0) + coalesce(bs.bar_revenue, 0) - coalesce(bc.total_artist_costs, 0) as net_profit,
    coalesce(ss.sound_checks_done, 0) as sound_checks_done,
    ss.max_spl_reading,
    coalesce(ss.all_engineers_signed_off, false) as all_engineers_signed_off,
    coalesce(tr.tickets_sold, 0) as tickets_sold,
    coalesce(bs.bar_revenue, 0) / nullif(coalesce(ds.total_attendance, 0), 0) as bar_revenue_per_attendee,
    coalesce(tr.ticket_revenue, 0) / nullif(coalesce(tr.tickets_sold, 0), 0) as avg_ticket_price,
    coalesce(bc.total_artist_costs, 0) / nullif(coalesce(tr.ticket_revenue, 0) + coalesce(bs.bar_revenue, 0), 0) as cost_to_revenue_ratio

from events e
left join venues v on e.venue_id = v.venue_id
left join lineup_stats ls on e.event_id = ls.event_id
left join door_stats ds on e.event_id = ds.event_id
left join ticket_revenue tr on e.event_id = tr.event_id
left join bar_stats bs on e.event_id = bs.event_id
left join booking_costs bc on e.event_id = bc.event_id
left join sound_stats ss on e.event_id = ss.event_id
