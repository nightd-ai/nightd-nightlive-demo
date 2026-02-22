-- Guests who were rejected at the door or never entered should not
-- have any bar transactions. This checks that bar_transactions.entry_id
-- references only admitted door entries.

select
    transaction_id,
    entry_id,
    entry_status
from {{ ref('srv_bar_transactions') }}
where entry_status = 'rejected'
