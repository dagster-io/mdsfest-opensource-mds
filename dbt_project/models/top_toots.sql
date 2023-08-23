select *

from {{ source('main', 'bird_toots') }}
where count > 1
