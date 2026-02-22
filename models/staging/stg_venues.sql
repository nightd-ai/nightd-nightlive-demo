select
    cast(venue_id as varchar) as venue_id,
    venue_name,
    address,
    cast(total_capacity as integer) as total_capacity,
    cast(main_floor_capacity as integer) as main_floor_capacity,
    cast(bar_area_capacity as integer) as bar_area_capacity,
    cast(garden_capacity as integer) as garden_capacity,
    sound_system,
    cast(spl_limit_db as integer) as spl_limit_db,
    license_type,
    opening_hours_start,
    opening_hours_end,
    cast(headliner_minimum_fee as numeric) as headliner_minimum_fee

from {{ source('nightlive', 'venues') }}
