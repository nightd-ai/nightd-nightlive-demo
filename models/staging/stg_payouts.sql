select
    cast(payout_id as varchar) as payout_id,
    cast(booking_id as varchar) as booking_id,
    cast(amount as numeric) as amount,
    cast(payout_date as date) as payout_date,
    invoice_status

from {{ source('nightlive', 'payouts') }}
