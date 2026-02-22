select
    venue_id,
    venue_name,
    address,
    total_capacity,
    main_floor_capacity,
    bar_area_capacity,
    garden_capacity,
    sound_system,
    spl_limit_db,
    license_type,
    opening_hours_start,
    opening_hours_end,
    headliner_minimum_fee

from {{ ref('stg_venues') }}
