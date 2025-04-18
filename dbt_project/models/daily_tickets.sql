select

created_at,
channel,
count(1) as num_tickets

from {{ source('main', 'tickets') }}
group by all
