select
    artist_id,
    artist_name,
    genre,
    booking_tier,
    is_resident,
    home_city

from {{ ref('stg_artists') }}
