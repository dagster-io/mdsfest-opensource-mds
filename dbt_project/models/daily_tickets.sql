select

date_trunc('day', left(created_at, 10)::date) as created_at,
channel,
count(1) as num_tickets

from {{ source('main', 'tickets') }}
group by all
