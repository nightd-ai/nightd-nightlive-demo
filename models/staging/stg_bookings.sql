select
    cast(booking_id as varchar) as booking_id,
    cast(event_id as varchar) as event_id,
    cast(artist_id as varchar) as artist_id,
    cast(agreed_fee as numeric) as agreed_fee,
    payment_status,
    rider_requirements,
    cancellation_terms

from {{ source('nightlive', 'bookings') }}
