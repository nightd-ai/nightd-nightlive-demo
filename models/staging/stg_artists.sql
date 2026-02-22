select
    cast(artist_id as varchar) as artist_id,
    artist_name,
    genre,
    booking_tier,
    cast(is_resident as boolean) as is_resident,
    home_city

from {{ source('nightlive', 'artists') }}
